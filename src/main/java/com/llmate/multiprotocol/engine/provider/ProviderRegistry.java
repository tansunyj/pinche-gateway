package com.llmate.multiprotocol.engine.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.CacheConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.ProviderCapability;
import com.llmate.multiprotocol.entity.ProxyChannelModelsEntity;
import com.llmate.multiprotocol.entity.ProxyChannelsEntity;
import com.llmate.multiprotocol.entity.ProxyChannelTokensEntity;
import com.llmate.multiprotocol.repository.ProxyChannelModelsRepository;
import com.llmate.multiprotocol.repository.ProxyChannelTokensRepository;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.repository.ProxyChannelsRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.log4j.Log4j2;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provider 注册中心
 *
 * 模型级绑定模式：adapter 不再按"一个渠道一个"，而是按 proxy_channel_models 的模型绑定行
 * （provider_capability JSON 里的 provider_alias）注册。注册表为嵌套结构：
 * - {@code providers}      ：channelCode → (providerAlias → ProviderAdapter)，存 chat/image 域
 * - {@code videoProviders}  ：channelCode → (providerAlias → ProviderAdapter)，存 video 域
 * - 同一渠道可同时注册多个 adapter（vapeur：gpt→openai_bearer、claude→anthropic、
 *   gpt-image→openai_image、wanx→dashscope_video 共用一个渠道记录，共享 base_url + token 池）
 *
 * 全响应式加载（无 .block()，避免在 reactor-tcp-nio 事件循环线程上阻塞）：
 * - 启动 {@link #init()} 在 main 线程 block() 等待全量加载完成：逐渠道读启用绑定行 →
 *   解析 provider_alias → 去重 → 按域入表
 * - 绑定行 provider_capability 是模型协议<b>唯一真相源</b>（绑定界面设置 openai/claude/gemini 系列）；
 *   为空/无法解析的行跳过并告警，<b>绝不</b>按渠道 type 推导兜底——proxy_channels.type 是单值字段、
 *   无法表达多协议渠道（vp 同时含 gpt/claude/gemini 系列），不参与任何路由/注册
 * - 运行时查询（getProvider / getVideoProvider）为纯内存查找，不触碰数据库
 * - 轮询/重载走响应式：{@link #getVideoProviderByModel}、{@link #reloadProvider}
 *
 * 说明：运行时不再"首次访问自动加载新渠道"（旧实现该路径在 Reactor 线程上 block() 本就有隐患）。
 * 新增/改渠道后调用 {@link #reloadProvider} 或重启网关生效。
 */
@Component
@Log4j2
public class ProviderRegistry {

    private final ProxyChannelsRepository proxyChannelsRepository;
    private final ProxyChannelModelsRepository proxyChannelModelsRepository;
    private final ProxyChannelTokensRepository proxyChannelTokensRepository;
    private final ProviderFactory providerFactory;
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    // 嵌套注册表：channelCode -> (providerAlias -> ProviderAdapter)
    // providers 存 chat/image 域；videoProviders 存 video 域
    private final Map<String, Map<String, ProviderAdapter>> providers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, ProviderAdapter>> videoProviders = new ConcurrentHashMap<>();

    public ProviderRegistry(ProxyChannelsRepository proxyChannelsRepository,
                            ProxyChannelModelsRepository proxyChannelModelsRepository,
                            ProxyChannelTokensRepository proxyChannelTokensRepository,
                            ProviderFactory providerFactory,
                            RedisTemplate<String, String> redisTemplate,
                            ObjectMapper objectMapper) {
        this.proxyChannelsRepository = proxyChannelsRepository;
        this.proxyChannelModelsRepository = proxyChannelModelsRepository;
        this.proxyChannelTokensRepository = proxyChannelTokensRepository;
        this.providerFactory = providerFactory;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Spring 启动后自动注册所有启用渠道的 Provider（按模型绑定）
     * 全响应式链在 main 线程 block() 等待完成（main 非 Reactor 线程，block 合法）
     */
    @PostConstruct
    public void init() {
        try {
            loadProvidersFromDatabase()
                .subscribeOn(Schedulers.boundedElastic())
                .block();
        } catch (Exception e) {
            log.error("[ProviderRegistry] 启动加载 Provider 失败", e);
        }
    }

    /**
     * 从数据库加载所有启用渠道的 adapter（按绑定行 provider_alias 注册，按 domain 分域入表）
     */
    private Mono<Void> loadProvidersFromDatabase() {
        return proxyChannelsRepository.findAll()
            .filter(channel -> channel.getStatus() != null && channel.getStatus() == SystemConstants.STATUS_ENABLED)
            .flatMap(this::createProviderConfig)
            .concatMap(this::loadChannelAdapters)
            .then()
            .doOnSuccess(v -> log.info("[ProviderRegistry] Provider 注册完成: providers={}, videoProviders={}",
                    providers.size(), videoProviders.size()))
            .onErrorResume(e -> {
                log.error("[ProviderRegistry] 注册 Provider 失败", e);
                return Mono.empty();
            });
    }

    /**
     * 加载单个渠道的全部 adapter（响应式，不阻塞）：读启用绑定行 → 解析 provider_alias →
     * 去重 → 按 domain 分域创建。同渠道多个模型绑同一 alias 时只建一个实例。
     * 绑定行 provider_capability 为空/无法解析时跳过该行并告警（协议真相必须显式声明，不按 type 兜底）。
     */
    private Mono<Void> loadChannelAdapters(ProviderProperties.ProviderConfig config) {
        Long channelId = config.getChannelId();
        String channelCode = config.getAlias();
        if (channelId == null || channelCode == null) {
            return Mono.empty();
        }

        return proxyChannelModelsRepository
            .findByChannelIdAndIsEnabled(channelId, SystemConstants.STATUS_ENABLED)
            .collectList()
            .flatMap(bindings -> {
                if (bindings == null || bindings.isEmpty()) {
                    log.debug("[ProviderRegistry] 渠道 {} 无启用模型绑定，跳过 adapter 注册", channelCode);
                    return Mono.<Void>empty();
                }

                Map<String, ProviderAdapter> chatAdapters = new ConcurrentHashMap<>();
                Map<String, ProviderAdapter> videoAdapters = new ConcurrentHashMap<>();
                Set<String> loaded = new HashSet<>();

                for (ProxyChannelModelsEntity binding : bindings) {
                    String capabilityJson = binding.getProviderCapability();
                    if (capabilityJson == null || capabilityJson.isBlank()) {
                        // 协议真相只能由绑定界面显式设置（openai/claude/gemini 系列）。provider_capability
                        // 为空的行跳过并告警，绝不按渠道 type 自动推导——type 是单值字段、无法表达多协议
                        // 渠道，按它回填会把模型写成错误协议（生产 asi/vp 曾 type=openai 却绑 anthropic，
                        // 兜底选中即 502 的根源）
                        log.warn("[ProviderRegistry] 模型 {} 绑定行 provider_capability 为空，跳过注册：channel={}（需在绑定界面为该模型显式设置 openai/claude/gemini 系列协议）",
                                binding.getModelId(), channelCode);
                        continue;
                    }

                    String alias = extractProviderAlias(capabilityJson);
                    if (alias == null) {
                        log.warn("[ProviderRegistry] 绑定行无法解析 provider_alias，跳过: channel={}, model={}",
                                channelCode, binding.getModelId());
                        continue;
                    }
                    if (ProviderCapabilityCatalog.findByAlias(alias) == null) {
                        log.warn("[ProviderRegistry] 绑定行 provider_alias 不在能力清单，跳过: channel={}, model={}, alias={}",
                                channelCode, binding.getModelId(), alias);
                        continue;
                    }

                    // 同渠道同 alias 只建一个实例
                    if (!loaded.add(alias)) {
                        continue;
                    }

                    try {
                        ProviderAdapter adapter = providerFactory.createByAlias(alias, config);
                        if (ProviderCapabilityCatalog.isVideo(alias)) {
                            videoAdapters.put(alias, adapter);
                        } else {
                            chatAdapters.put(alias, adapter);
                        }
                        log.info("[ProviderRegistry] 已注册 Provider: channel={}, providerAlias={} (domain={}), name={}",
                                channelCode, alias, ProviderCapabilityCatalog.findDomain(alias), adapter.getProviderName());
                    } catch (Exception e) {
                        log.error("[ProviderRegistry] 创建 Provider 失败: channel={}, providerAlias={}, type={}",
                                channelCode, alias, config.getType(), e);
                    }
                }

                if (!chatAdapters.isEmpty()) {
                    providers.put(channelCode, chatAdapters);
                }
                if (!videoAdapters.isEmpty()) {
                    videoProviders.put(channelCode, videoAdapters);
                }
                return Mono.<Void>empty();
            })
            .onErrorResume(e -> {
                log.error("[ProviderRegistry] 加载渠道 {} 失败", channelCode, e);
                return Mono.<Void>empty();
            });
    }

    // ==================== 运行时查询（纯内存，不触碰数据库） ====================

    /**
     * 根据渠道代码 + providerAlias 获取 Provider（chat/image 域）
     */
    public ProviderAdapter getProvider(String channelCode, String providerAlias) {
        Map<String, ProviderAdapter> aliasMap = providers.get(channelCode);
        return aliasMap != null ? aliasMap.get(providerAlias) : null;
    }

    /**
     * 根据渠道代码 + providerAlias 获取生视频 Provider（video 域）
     */
    public ProviderAdapter getVideoProvider(String channelCode, String providerAlias) {
        Map<String, ProviderAdapter> aliasMap = videoProviders.get(channelCode);
        return aliasMap != null ? aliasMap.get(providerAlias) : null;
    }

    /**
     * 按渠道ID + 模型ID 解析视频 Provider（响应式；异步生视频功能已下线，保留供未来复用）。
     * 查 proxy_channel_models 绑定行拿 provider_alias → videoProviders[channelCode][alias]；
     * 绑定行缺失/无 alias 时返回空（不按渠道 type 兜底——协议真相只能来自绑定行）。
     */
    public Mono<ProviderAdapter> getVideoProviderByModel(Long channelId, String modelId) {
        if (channelId == null) {
            return Mono.empty();
        }
        return proxyChannelsRepository.findById(channelId)
            .flatMap(channel -> {
                String channelCode = channel.getChannelCode();
                Mono<ProviderAdapter> bound = Mono.empty();
                if (modelId != null) {
                    bound = proxyChannelModelsRepository
                        .findByChannelIdAndModelIdAndIsEnabled(channelId, modelId, SystemConstants.STATUS_ENABLED)
                        .mapNotNull(binding -> extractProviderAlias(binding.getProviderCapability()))
                        // 与 getOrLoadChatProvider 同策略：绑定 alias 未在内存注册时走 DB 懒加载自愈
                        .flatMap(alias -> getOrLoadVideoProvider(channelCode, alias));
                }
                return bound;
            })
            .onErrorResume(e -> {
                log.error("[ProviderRegistry] getVideoProviderByModel 失败: channelId={}, modelId={}", channelId, modelId, e);
                return Mono.empty();
            });
    }

    /**
     * 获取所有已注册的 Provider（flatten providers + videoProviders，供状态查看）
     */
    public List<ProviderAdapter> getProviders() {
        List<ProviderAdapter> all = new ArrayList<>();
        for (Map<String, ProviderAdapter> aliasMap : providers.values()) {
            all.addAll(aliasMap.values());
        }
        for (Map<String, ProviderAdapter> aliasMap : videoProviders.values()) {
            all.addAll(aliasMap.values());
        }
        return List.copyOf(all);
    }

    /**
     * 重新加载指定渠道（用于渠道配置更新后；响应式异步加载）
     */
    public void reloadProvider(String channelCode) {
        log.info("[ProviderRegistry] 重新加载渠道: {}", channelCode);
        providers.remove(channelCode);
        videoProviders.remove(channelCode);
        clearCache(channelCode);
        loadChannelConfigAsync(channelCode)
            .flatMap(this::loadChannelAdapters)
            .subscribe(
                v -> log.info("[ProviderRegistry] 重新加载完成: {}", channelCode),
                e -> log.error("[ProviderRegistry] 重新加载失败: {}", channelCode, e));
    }

    /**
     * 移除指定渠道（用于渠道被禁用或删除后）
     */
    public void removeProvider(String channelCode) {
        providers.remove(channelCode);
        videoProviders.remove(channelCode);
        log.info("[ProviderRegistry] 移除渠道 Provider: {}", channelCode);
        clearCache(channelCode);
    }

    /**
     * 清除 Redis 缓存（保留兼容；当前渠道配置加载直接走数据库）
     */
    public void clearCache(String channelCode) {
        try {
            String cacheKey = CacheConstants.providerConfigKey(channelCode);
            redisTemplate.delete(cacheKey);
            log.debug("[ProviderRegistry] 清除 Redis 缓存: {}", channelCode);
        } catch (Exception e) {
            log.warn("[ProviderRegistry] 清除 Redis 缓存失败: {}", channelCode, e);
        }
    }

    /**
     * 检查渠道是否已加载（chat/image 或 video 任一注册表命中）
     */
    public boolean isProviderLoaded(String channelCode) {
        return providers.containsKey(channelCode) || videoProviders.containsKey(channelCode);
    }

    // ==================== 懒加载：运行时首次访问自动从数据库加载渠道（响应式，不阻塞） ====================

    /**
     * 获取 chat/image 域 Provider（内存优先，未命中时从数据库懒加载）。
     *
     * 解决"管理后台新增渠道后不重启不生效"的问题：首次请求路由到新渠道时，
     * 由本方法触发响应式 DB 查询 → 构建 ProviderConfig → 注册 adapter → 返回。
     * 后续请求命中内存缓存，零 DB 开销。
     *
     * @param channelCode   渠道代码（如 aliyun、deepseek、asi）
     * @param providerAlias provider 别名（如 openai_bearer、anthropic、vertex）
     * @return 懒加载完成后返回 ProviderAdapter，渠道不存在/禁用返回 CHANNEL_NOT_FOUND 错误
     */
    public Mono<ProviderAdapter> getOrLoadChatProvider(String channelCode, String providerAlias) {
        // 1. 内存快路径：已注册的精确匹配
        ProviderAdapter cached = getProvider(channelCode, providerAlias);
        if (cached != null) {
            return Mono.just(cached);
        }

        // 2. 数据库懒加载：alias 未命中时【不】回退任何渠道 type 推导的默认适配器（type 无意义，
        // 且可能与绑定行 provider_capability 声明的协议不一致）。统一走 lazyLoadChannel：
        // 按 DB 最新绑定注册全部 adapter 后精确匹配 alias，注册表过期（运行中改绑定未 reload）
        // 也能自愈；"真不存在的 alias"直接报错（模型未绑定该协议），绝不按 type 猜。
        log.info("[ProviderRegistry] 渠道 {} 未命中 alias={}，触发懒加载 (chat domain)", channelCode, providerAlias);
        return lazyLoadChannel(channelCode, providerAlias, false);
    }

    /**
     * 获取 video 域 Provider（内存优先，未命中时从数据库懒加载）。
     * 与 {@link #getOrLoadChatProvider} 同机制，查询 videoProviders 注册表。
     */
    public Mono<ProviderAdapter> getOrLoadVideoProvider(String channelCode, String providerAlias) {
        // 1. 内存快路径
        ProviderAdapter cached = getVideoProvider(channelCode, providerAlias);
        if (cached != null) {
            return Mono.just(cached);
        }

        // 2. 与 chat 域同策略：alias 未命中【不】回退任何 type 默认适配器，统一走 DB 懒加载
        //    按最新绑定注册后精确匹配
        log.info("[ProviderRegistry] 渠道 {} 未命中 alias={}，触发懒加载 (video domain)", channelCode, providerAlias);
        return lazyLoadChannel(channelCode, providerAlias, true);
    }

    /**
     * 获取所有已加载的渠道代码列表（供管理后台查看加载状态）
     */
    public Set<String> getLoadedChannelCodes() {
        Set<String> codes = new HashSet<>();
        codes.addAll(providers.keySet());
        codes.addAll(videoProviders.keySet());
        return codes;
    }

    /**
     * 重新加载全部渠道（管理后台手动触发，或部署后批量刷新）。
     * 异步执行：清除所有内存注册表 → 全量重新加载；调用方不等待完成（fire-and-forget），
     * 通过日志确认结果。
     */
    public void reloadAllProviders() {
        int prevChat = providers.size();
        int prevVideo = videoProviders.size();
        log.info("[ProviderRegistry] 重新加载全部渠道（当前 chat={}, video={}）", prevChat, prevVideo);

        providers.clear();
        videoProviders.clear();

        loadProvidersFromDatabase()
            .subscribeOn(Schedulers.boundedElastic())
            .subscribe(
                v -> log.info("[ProviderRegistry] 全量重新加载完成: 原chat={}/video={} → 新chat={}/video={}",
                    prevChat, prevVideo, providers.size(), videoProviders.size()),
                e -> log.error("[ProviderRegistry] 全量重新加载失败", e));
    }

    /**
     * 从数据库懒加载单个渠道：查 proxy_channels → 构建 ProviderConfig（含 Token 池）→
     * 调 loadChannelAdapters 创建全部 adapter 并注册 → 从内存取回指定 alias 的 adapter。
     *
     * 加载的是整个渠道（该渠道下所有 model binding 的 adapter），不止请求的那一个 alias：
     * 同渠道的其他模型后续请求也能命中内存缓存，避免重复 DB 查询。
     *
     * @param isVideo true=查 videoProviders，false=查 providers
     */
    private Mono<ProviderAdapter> lazyLoadChannel(String channelCode, String providerAlias, boolean isVideo) {
        return loadChannelConfigAsync(channelCode)
            .flatMap(config -> loadChannelAdapters(config)
                .then(Mono.defer(() -> {
                    // 加载完成后再次从内存获取（loadChannelAdapters 已注册 adapter 到对应 map）
                    ProviderAdapter adapter = isVideo
                        ? getVideoProvider(channelCode, providerAlias)
                        : getProvider(channelCode, providerAlias);
                    if (adapter != null) {
                        log.info("[ProviderRegistry] 懒加载成功: channel={}, providerAlias={}", channelCode, providerAlias);
                        return Mono.just(adapter);
                    }

                    // 请求的 alias 在绑定行中不存在 → 该模型未绑定此协议。绝不按渠道 type 猜默认
                    // adapter（type 无意义且可能错配，生产 asi/vp 曾因此 502），直接报错暴露配置缺口
                    log.error("[ProviderRegistry] 懒加载失败: channel={}, providerAlias={}, 渠道已加载但无匹配adapter（请检查该模型绑定行的 provider_capability）",
                        channelCode, providerAlias);
                    return Mono.error(new LlmGatewayException(
                        LlmErrorCode.CHANNEL_NOT_FOUND,
                        "channel=" + channelCode + ", alias=" + providerAlias + " (loaded but no matching adapter)"));
                })))
            .switchIfEmpty(Mono.defer(() -> {
                // 渠道不存在或已禁用，或无可用 Token
                log.warn("[ProviderRegistry] 懒加载失败: channel={} 不存在/已禁用/无可用Token", channelCode);
                return Mono.error(new LlmGatewayException(
                    LlmErrorCode.CHANNEL_NOT_FOUND,
                    "channel=" + channelCode + " (not found or disabled)"));
            }));
    }

    // ==================== 配置加载（响应式） ====================

    /**
     * 响应式加载渠道配置（含渠道ID、Token 池）；渠道不存在或无可启用 Token 时返回空 Mono
     */
    private Mono<ProviderProperties.ProviderConfig> loadChannelConfigAsync(String channelCode) {
        return proxyChannelsRepository.findByChannelCodeAndStatus(channelCode, SystemConstants.STATUS_ENABLED)
            .flatMap(this::createProviderConfig);
    }

    /**
     * 创建 ProviderConfig
     * 方案 C：加载渠道下所有启用的 Token（apiKeys + tokenIds 列表），Provider 内部动态选择
     */
    private Mono<ProviderProperties.ProviderConfig> createProviderConfig(ProxyChannelsEntity channel) {
        return proxyChannelTokensRepository.findByChannelIdAndStatus(channel.getId(), SystemConstants.STATUS_ENABLED)
            .collectList()
            .flatMap(tokens -> {
                if (tokens.isEmpty()) {
                    log.warn("[ProviderRegistry] 渠道 {} 没有可用的 Token，跳过注册", channel.getName());
                    return Mono.empty();
                }

                // 收集所有 Token 的 apiKey 和 id
                List<String> apiKeys = new ArrayList<>();
                List<Long> tokenIds = new ArrayList<>();
                for (ProxyChannelTokensEntity token : tokens) {
                    apiKeys.add(token.getApiKeyEncrypted());
                    tokenIds.add(token.getId());
                }

                ProviderProperties.ProviderConfig config = new ProviderProperties.ProviderConfig();
                config.setName(channel.getName());
                config.setAlias(channel.getChannelCode());
                config.setChannelId(channel.getId());
                config.setType(channel.getType());
                config.setBaseUrl(channel.getBaseUrl());
                // 方案 C：多 Token 列表
                config.setApiKeys(apiKeys);
                config.setTokenIds(tokenIds);
                // 单 Token 兼容：取第一个
                config.setApiKey(apiKeys.get(0));

                log.info("[ProviderRegistry] 渠道 {} 加载了 {} 个 Token", channel.getName(), apiKeys.size());
                return Mono.just(config);
            });
    }

    // ==================== provider_capability 解析 ====================

    /**
     * 解析 provider_capability JSON，取 provider_alias；空/解析失败返回 null
     */
    private String extractProviderAlias(String capabilityJson) {
        if (capabilityJson == null || capabilityJson.isBlank()) {
            return null;
        }
        try {
            ProviderCapability cap = objectMapper.readValue(capabilityJson, ProviderCapability.class);
            String alias = cap.getProviderAlias();
            return alias != null && !alias.isBlank() ? alias : null;
        } catch (Exception e) {
            log.warn("[ProviderRegistry] 解析 provider_capability 失败: json={}", capabilityJson, e);
            return null;
        }
    }
}
