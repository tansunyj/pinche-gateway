package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.entity.EndpointEntity;
import com.llmate.multiprotocol.entity.ProxyChannelModelsEntity;
import com.llmate.multiprotocol.entity.ProxyChannelsEntity;
import com.llmate.multiprotocol.repository.EndpointRepository;
import com.llmate.multiprotocol.repository.ProxyChannelModelsRepository;
import com.llmate.multiprotocol.repository.ProxyChannelsRepository;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 模型端点解析器
 *
 * 新库（pt_carpool）端点拼接规则：proxy_channel_models.use_endpoint_id → endpoint.path，
 * 未绑定端点（use_endpoint_id 为 NULL）时按渠道协议类型取默认路径。
 * base_url 始终来自 proxy_channels。
 */
@Log4j2
@Service
public class ModelEndpointResolver {

    private final ProxyChannelModelsRepository proxyChannelModelsRepository;
    private final EndpointRepository endpointRepository;
    private final ProxyChannelsRepository proxyChannelsRepository;

    public ModelEndpointResolver(ProxyChannelModelsRepository proxyChannelModelsRepository,
                                 EndpointRepository endpointRepository,
                                 ProxyChannelsRepository proxyChannelsRepository) {
        this.proxyChannelModelsRepository = proxyChannelModelsRepository;
        this.endpointRepository = endpointRepository;
        this.proxyChannelsRepository = proxyChannelsRepository;
    }

    /**
     * 解析模型端点配置
     *
     * URL 构建规则：proxy_channels.base_url + endpoint.path
     *
     * @param modelId   模型ID（上游真实模型名）
     * @param channelId 渠道ID
     * @return 端点配置，包含完整的 baseUrl 和 endpointPath
     */
    public Mono<ModelEndpointConfig> resolve(String modelId, Long channelId) {
        log.info("[ModelEndpointResolver] 解析端点配置: modelId={}, channelId={}", modelId, channelId);

        return proxyChannelModelsRepository.findByChannelIdAndModelIdAndIsEnabled(
                        channelId, modelId, SystemConstants.STATUS_ENABLED)
                .flatMap(mapping -> buildEndpointConfig(channelId, mapping.getUseEndpointId()))
                .switchIfEmpty(Mono.defer(() -> {
                    log.warn("[ModelEndpointResolver] 未找到模型渠道配置: modelId={}, channelId={}", modelId, channelId);
                    // 降级：只使用渠道的 baseUrl，使用默认 endpoint
                    return buildDefaultConfig(channelId);
                }));
    }

    /**
     * 按模型ID解析首个启用渠道及端点（TTS/Embedding/ASR 等非聊天服务用）
     *
     * 从 proxy_channel_models（is_enabled=1，按优先级降序）取渠道ID，
     * use_endpoint_id 有值时返回 endpoint.path，否则 endpointPath 为 null（由调用方用协议默认路径兜底）。
     *
     * @param modelId 模型ID（上游真实模型名）
     * @return 渠道ID + 可选端点路径
     */
    public Mono<ModelChannelEndpoint> resolveByModelId(String modelId) {
        return proxyChannelModelsRepository.findFirstByModelIdAndIsEnabledOrderByPriorityDesc(
                        modelId, SystemConstants.STATUS_ENABLED)
                .flatMap(mapping -> {
                    Long channelId = mapping.getChannelId();
                    Long useEndpointId = mapping.getUseEndpointId();
                    if (useEndpointId == null) {
                        return Mono.just(new ModelChannelEndpoint(channelId, null));
                    }
                    return endpointRepository.findById(useEndpointId)
                            .map(endpoint -> new ModelChannelEndpoint(channelId, endpoint.getPath()))
                            .defaultIfEmpty(new ModelChannelEndpoint(channelId, null));
                });
    }

    /**
     * 按渠道ID + 端点ID构建端点配置
     *
     * @param channelId    渠道ID
     * @param useEndpointId 绑定的端点ID（可为 null）
     */
    private Mono<ModelEndpointConfig> buildEndpointConfig(Long channelId, Long useEndpointId) {
        return proxyChannelsRepository.findById(channelId)
                .switchIfEmpty(Mono.error(new IllegalStateException("渠道不存在: channelId=" + channelId)))
                .flatMap(channel -> {
                    String baseUrl = channel.getBaseUrl();
                    Mono<String> endpointPathMono;
                    if (useEndpointId != null) {
                        endpointPathMono = endpointRepository.findById(useEndpointId)
                                .map(EndpointEntity::getPath)
                                .switchIfEmpty(Mono.defer(() -> {
                                    log.warn("[ModelEndpointResolver] 端点不存在或已禁用: endpointId={}", useEndpointId);
                                    return Mono.just(defaultEndpointPath(channel.getType()));
                                }));
                    } else {
                        endpointPathMono = Mono.just(defaultEndpointPath(channel.getType()));
                    }

                    return endpointPathMono.map(endpointPath -> {
                        log.info("[ModelEndpointResolver] 构建端点配置: baseUrl={}, endpointPath={}", baseUrl, endpointPath);
                        return ModelEndpointConfig.builder()
                                .baseUrl(baseUrl)
                                .endpointPath(endpointPath)
                                .httpMethod("POST")
                                .build();
                    });
                });
    }

    /**
     * 构建默认配置（当找不到 proxy_channel_models 记录时）
     */
    private Mono<ModelEndpointConfig> buildDefaultConfig(Long channelId) {
        return proxyChannelsRepository.findById(channelId)
                .map(channel -> {
                    log.warn("[ModelEndpointResolver] 使用默认配置: baseUrl={}, endpointPath={}",
                            channel.getBaseUrl(), defaultEndpointPath(channel.getType()));
                    return ModelEndpointConfig.builder()
                            .baseUrl(channel.getBaseUrl())
                            .endpointPath(defaultEndpointPath(channel.getType()))
                            .httpMethod("POST")
                            .build();
                })
                .switchIfEmpty(Mono.error(new IllegalStateException("渠道不存在: channelId=" + channelId)));
    }

    /**
     * 根据渠道协议类型返回默认端点路径
     * - anthropic / claude → v1/messages
     * - vertex / gemini → models/{model}:generateContent（{model} 由 Provider 层替换）
     * - 其余（OpenAI 兼容）→ chat/completions
     */
    private String defaultEndpointPath(String channelType) {
        if (channelType == null) {
            return "chat/completions";
        }
        return switch (channelType.toLowerCase()) {
            case "anthropic", "claude" -> "v1/messages";
            case "vertex", "gemini" -> "models/{model}:generateContent";
            // 图像渠道默认路径（编辑路径由 OpenAiImageAdapter 在命中生图默认时切换）
            case "openai_image", "openai_images", "azure_image" -> "v1/images/generations";
            case "dashscope_image", "aliyun_image" -> "api/v1/services/aigc/multimodal-generation/generation";
            case "gemini_image" -> "gemini/v1beta/models/{model}:generateContent";
            // 生视频渠道默认路径（当前渠道 type=openai，由视频适配器 effectiveEndpoint 兜底，此为未来渠道类型预留）
            case "dashscope_video", "aliyun_video" -> "api/v1/services/aigc/video-generation/video-synthesis";
            case "volcengine_video" -> "api/v3/contents/generations/tasks";
            default -> "chat/completions";
        };
    }

    /**
     * 按模型解析结果：渠道ID + 端点路径（null=未绑定，用协议默认路径）
     */
    public record ModelChannelEndpoint(Long channelId, String endpointPath) {
    }
}
