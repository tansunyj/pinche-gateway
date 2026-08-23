package com.llmate.multiprotocol.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.CacheConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.ProviderCapability;
import com.llmate.multiprotocol.dto.RoutingResult;
import com.llmate.multiprotocol.entity.ModelLibraryEntity;
import com.llmate.multiprotocol.entity.ProxyChannelModelsEntity;
import com.llmate.multiprotocol.entity.ProxyChannelsEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.repository.ModelLibraryRepository;
import com.llmate.multiprotocol.repository.ProxyChannelModelsRepository;
import com.llmate.multiprotocol.repository.ProxyChannelsRepository;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Random;

/**
 * 模型路由器
 * 支持三种模型ID格式：
 * 1. 纯模型ID（无斜杠）- 需要兜底：查询所有支持该模型的渠道，随机选择一个
 * 2. 带渠道前缀（aliyun/gpt-4o）- 直接路由到指定渠道
 * 3. 模型ID本身含斜杠 - 先查 model_library 表，存在则兜底；不存在则按"渠道/模型"解析
 */
@Component
@Log4j2
public class ModelRouter {

    private final ModelLibraryRepository modelLibraryRepository;
    private final ProxyChannelsRepository proxyChannelsRepository;
    private final ProxyChannelModelsRepository proxyChannelModelsRepository;
    private final ChannelResolver channelResolver;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();

    public ModelRouter(ModelLibraryRepository modelLibraryRepository,
                       ProxyChannelsRepository proxyChannelsRepository,
                       ProxyChannelModelsRepository proxyChannelModelsRepository,
                       ChannelResolver channelResolver,
                       @Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate,
                       ObjectMapper objectMapper) {
        this.modelLibraryRepository = modelLibraryRepository;
        this.proxyChannelsRepository = proxyChannelsRepository;
        this.proxyChannelModelsRepository = proxyChannelModelsRepository;
        this.channelResolver = channelResolver;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * 模型ID路由 - 支持三种情况
     *
     * @param modelId 用户传入的模型ID
     * @return RoutingResult 包含：原始模型ID、渠道代码、上游真实模型名
     */
    public Mono<RoutingResult> resolve(String modelId) {
        if (modelId == null || modelId.isEmpty()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.MODEL_NOT_FOUND, "empty"));
        }

        if (modelId.contains("/")) {
            // 情况2或3：可能是 "渠道/模型" 或 "模型ID本身含斜杠"
            return resolveWithSlash(modelId);
        } else {
            // 情况1：纯模型ID，需要兜底
            return resolvePureModelId(modelId);
        }
    }

    /**
     * 纯模型ID兜底：查询所有支持该模型的渠道，随机选择一个
     */
    private Mono<RoutingResult> resolvePureModelId(String modelId) {
        // 查询所有支持该模型的渠道关联
        return proxyChannelModelsRepository.findByModelId(modelId)
            .filter(entity -> entity.getIsEnabled() != null && entity.getIsEnabled() == SystemConstants.STATUS_ENABLED)
            .collectList()
            .flatMap(channelModels -> {
                if (channelModels.isEmpty()) {
                    return Mono.error(new LlmGatewayException(
                        LlmErrorCode.CHANNEL_NO_DEFAULT, modelId));
                }

                // 随机选择一个渠道
                ProxyChannelModelsEntity selected = selectRandomChannel(channelModels);

                return findChannelById(selected.getChannelId())
                    .map(channel -> RoutingResult.builder()
                        .modelId(modelId)
                        .pureModelId(modelId)
                        .channelCode(channel.getChannelCode())
                        .channelId(channel.getId())
                        .upstreamModel(modelId)  // 直接使用原始模型名作为上游模型名
                        .providerAlias(extractProviderAlias(selected, channel))
                        .hasChannelPrefix(false)
                        .build());
            });
    }

    /**
     * 随机选择一个渠道
     */
    private ProxyChannelModelsEntity selectRandomChannel(List<ProxyChannelModelsEntity> channelModels) {
        int index = random.nextInt(channelModels.size());
        return channelModels.get(index);
    }

    /**
     * 带斜杠的模型ID：先查 model_library，存在则兜底，不存在则按"渠道/模型"解析
     */
    private Mono<RoutingResult> resolveWithSlash(String modelId) {
        // 1. 先尝试查 model_library（处理情况3：模型ID本身含斜杠）
        return getModelFromLibrary(modelId)
            .flatMap(model -> {
                // 情况3：确实是模型库中的模型ID（含斜杠）
                return resolvePureModelId(modelId);
            })
            .switchIfEmpty(
                // 2. 不是模型库中的模型ID，按"渠道/模型"解析（情况2）
                resolveChannelPrefix(modelId)
            );
    }

    /**
     * 按"渠道/模型"格式解析
     * 支持模型ID本身含斜杠的情况，如 aliyun/kimi/kimi-k3
     * 解析逻辑：
     * 1. 先按第一个斜杠分割：渠道 = aliyun，模型 = kimi/kimi-k3
     * 2. 验证渠道是否支持该模型（查询 proxy_channel_models）
     * 3. 如果不支持，尝试去掉模型ID中的层级前缀（如 kimi/kimi-k3 → kimi-k3）再查
     */
    private Mono<RoutingResult> resolveChannelPrefix(String modelId) {
        int slashIndex = modelId.indexOf('/');
        if (slashIndex <= 0 || slashIndex == modelId.length() - 1) {
            return Mono.error(new LlmGatewayException(
                LlmErrorCode.MODEL_NOT_FOUND, modelId));
        }

        String channelCode = modelId.substring(0, slashIndex);
        String pureModelId = modelId.substring(slashIndex + 1);

        // 1. 查询渠道是否存在
        return findChannelByCode(channelCode)
            .flatMap(channel ->
                // 2. 检查渠道是否支持该模型（先按完整模型ID查）
                validateAndBuildRouting(channel, modelId, pureModelId, channelCode)
                    .switchIfEmpty(
                        // 3. 如果完整模型ID不支持，尝试去掉层级前缀再查
                        // 例如 kimi/kimi-k3 → 尝试 kimi-k3
                        tryFallbackModelId(channel, modelId, pureModelId, channelCode)
                    )
            );
    }

    /**
     * 验证渠道是否支持模型，并构建路由结果
     */
    private Mono<RoutingResult> validateAndBuildRouting(ProxyChannelsEntity channel, String originalModelId,
                                                        String pureModelId, String channelCode) {
        return channelResolver.validateModelSupport(channel.getId(), pureModelId)
            .flatMap(binding -> {
                if (binding == null) {
                    return Mono.empty(); // 无绑定行，返回空，让上层尝试 fallback
                }

                // 构建路由结果（绑定行带出 provider_alias）
                return Mono.just(RoutingResult.builder()
                    .modelId(originalModelId)
                    .pureModelId(pureModelId)
                    .channelCode(channelCode)
                    .channelId(channel.getId())
                    .upstreamModel(pureModelId)  // 直接使用纯模型ID作为上游模型名
                    .providerAlias(extractProviderAlias(binding, channel))
                    .hasChannelPrefix(true)
                    .build());
            });
    }

    /**
     * 尝试去掉模型ID中的层级前缀，再验证渠道支持
     * 例如：kimi/kimi-k3 → 尝试 kimi-k3
     */
    private Mono<RoutingResult> tryFallbackModelId(ProxyChannelsEntity channel, String originalModelId,
                                                   String pureModelId, String channelCode) {
        // 如果 pureModelId 还包含斜杠，尝试取最后一部分
        if (!pureModelId.contains("/")) {
            // 没有更多层级可以去掉，返回错误
            return Mono.error(new LlmGatewayException(
                LlmErrorCode.MODEL_NOT_SUPPORTED, pureModelId, channelCode));
        }

        // 取最后一部分作为 fallback 模型ID
        String fallbackModelId = pureModelId.substring(pureModelId.lastIndexOf('/') + 1);
        log.debug("[ModelRouter] 尝试 fallback 模型ID: original={}, fallback={}", pureModelId, fallbackModelId);

        return channelResolver.validateModelSupport(channel.getId(), fallbackModelId)
            .flatMap(binding -> {
                if (binding == null) {
                    // 仍然不支持，返回错误
                    return Mono.error(new LlmGatewayException(
                        LlmErrorCode.MODEL_NOT_SUPPORTED, pureModelId, channelCode));
                }

                // 使用 fallback 模型ID构建路由结果
                // 注意：upstreamModel 使用原始的 pureModelId（如 kimi/kimi-k3），
                // 而不是 fallbackModelId（如 kimi-k3），因为上游渠道需要完整的模型ID
                log.info("[ModelRouter] 使用 fallback 模型ID路由: original={}, pureModelId={}, fallback={}, channel={}",
                    originalModelId, pureModelId, fallbackModelId, channelCode);
                return Mono.just(RoutingResult.builder()
                    .modelId(originalModelId)
                    .pureModelId(pureModelId)  // 保留原始的 pureModelId（如 kimi/kimi-k3）
                    .channelCode(channelCode)
                    .channelId(channel.getId())
                    .upstreamModel(pureModelId)  // 上游使用完整的模型ID（如 kimi/kimi-k3）
                    .providerAlias(extractProviderAlias(binding, channel))
                    .hasChannelPrefix(true)
                    .build());
            });
    }

    /**
     * 从模型库获取模型信息（带缓存）
     */
    private Mono<ModelLibraryEntity> getModelFromLibrary(String modelId) {
        String cacheKey = CacheConstants.modelLibKey(modelId);

        return redisTemplate.opsForValue().get(cacheKey)
            .flatMap(this::parseModelFromCache)
            .switchIfEmpty(
                modelLibraryRepository.findByModelIdAndStatus(modelId, SystemConstants.STATUS_ENABLED)
                    .doOnNext(model -> cacheModel(cacheKey, model).subscribe())
            );
    }

    /**
     * 根据渠道ID查询渠道
     */
    private Mono<ProxyChannelsEntity> findChannelById(Long channelId) {
        return proxyChannelsRepository.findById(channelId)
            .filter(channel -> channel.getStatus() != null && channel.getStatus() == SystemConstants.STATUS_ENABLED)
            .switchIfEmpty(Mono.error(new LlmGatewayException(
                LlmErrorCode.CHANNEL_NOT_FOUND, "channelId:" + channelId)));
    }

    /**
     * 根据渠道代码查询渠道
     */
    private Mono<ProxyChannelsEntity> findChannelByCode(String channelCode) {
        return proxyChannelsRepository.findByChannelCodeAndStatus(channelCode, SystemConstants.STATUS_ENABLED)
            .switchIfEmpty(Mono.error(new LlmGatewayException(
                LlmErrorCode.CHANNEL_NOT_FOUND, channelCode)));
    }

    /**
     * 从缓存解析模型
     */
    private Mono<ModelLibraryEntity> parseModelFromCache(String cached) {
        try {
            Long modelId = Long.parseLong(cached);
            return modelLibraryRepository.findById(modelId);
        } catch (Exception e) {
            return Mono.empty();
        }
    }

    /**
     * 缓存模型信息
     */
    private Mono<Boolean> cacheModel(String cacheKey, ModelLibraryEntity model) {
        return redisTemplate.opsForValue()
            .set(cacheKey, String.valueOf(model.getId()), CacheConstants.TTL_MODEL_LIB);
    }

    /**
     * 清除模型库缓存
     */
    public Mono<Void> clearCache(String modelId) {
        String cacheKey = CacheConstants.modelLibKey(modelId);
        return redisTemplate.delete(cacheKey).then();
    }

    /**
     * 从绑定行 provider_capability JSON 解析 provider_alias——协议唯一真相源。
     * 绑定行缺失/无 capability/解析失败时直接抛错，绝不按渠道 type 兜底（type 无意义、
     * 单值字段无法表达多协议渠道；按它猜会写错协议，生产 asi/vp 曾因此 502）。
     */
    private String extractProviderAlias(ProxyChannelModelsEntity binding, ProxyChannelsEntity channel) {
        String json = binding != null ? binding.getProviderCapability() : null;
        if (json != null && !json.isBlank()) {
            try {
                ProviderCapability cap = objectMapper.readValue(json, ProviderCapability.class);
                String alias = cap.getProviderAlias();
                if (alias != null && !alias.isBlank()) {
                    return alias;
                }
            } catch (Exception e) {
                log.error("[ModelRouter] 解析绑定行 provider_capability 失败: channel={}, model={}, json={}",
                    channel.getChannelCode(), binding.getModelId(), json, e);
            }
        }
        String modelId = binding != null ? binding.getModelId() : "unknown";
        throw new LlmGatewayException(LlmErrorCode.MODEL_NOT_FOUND,
            "模型 " + modelId + " 在渠道 " + channel.getChannelCode()
                + " 的绑定行缺少 provider_capability（请在后台绑定界面为该模型设置 openai/claude/gemini 系列协议）");
    }
}
