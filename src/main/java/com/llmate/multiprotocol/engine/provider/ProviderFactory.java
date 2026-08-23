package com.llmate.multiprotocol.engine.provider;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.converter.upstream.AnthropicFormatConverter;
import com.llmate.multiprotocol.converter.upstream.OpenAiFormatConverter;
import com.llmate.multiprotocol.converter.upstream.VertexFormatConverter;
import com.llmate.multiprotocol.engine.provider.anthropic.AnthropicProviderAdapter;
import com.llmate.multiprotocol.engine.provider.image.DashScopeImageAdapter;
import com.llmate.multiprotocol.engine.provider.image.GeminiImageAdapter;
import com.llmate.multiprotocol.engine.provider.image.OpenAiImageAdapter;
import com.llmate.multiprotocol.engine.provider.openai.AzureProviderAdapter;
import com.llmate.multiprotocol.engine.provider.openai.OpenAiCompatibleAdapter;
import com.llmate.multiprotocol.engine.provider.vertex.VertexProviderAdapter;
import com.llmate.multiprotocol.engine.provider.video.DashScopeVideoAdapter;
import com.llmate.multiprotocol.engine.provider.video.VolcengineVideoAdapter;
import com.llmate.multiprotocol.service.OssService;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

/**
 * Provider 工厂类
 * 根据配置中的 type 字段，创建对应的 ProviderAdapter 实例
 *
 * 支持的类型：
 * - openai_bearer: 标准 Bearer Token 认证的 OpenAI 兼容渠道
 * - openai_azure: Azure OpenAI（特殊认证 + flatMap 流式解析）
 * - anthropic: Anthropic Claude 原生协议
 * - vertex: Google Vertex AI 原生协议
 */
@Component
@Log4j2
public class ProviderFactory {

    private final OpenAiFormatConverter openAiFormatConverter;
    private final AnthropicFormatConverter anthropicFormatConverter;
    private final VertexFormatConverter vertexFormatConverter;
    private final ObjectMapper objectMapper;
    private final OssService ossService;

    public ProviderFactory(
            OpenAiFormatConverter openAiFormatConverter,
            AnthropicFormatConverter anthropicFormatConverter,
            VertexFormatConverter vertexFormatConverter,
            ObjectMapper objectMapper,
            OssService ossService) {
        this.openAiFormatConverter = openAiFormatConverter;
        this.anthropicFormatConverter = anthropicFormatConverter;
        this.vertexFormatConverter = vertexFormatConverter;
        // 注入 Spring 单例 ObjectMapper，供各 ProviderAdapter 共享使用，避免各自 new 一份
        this.objectMapper = objectMapper;
        // 图像适配器需要把 HTTP 上传的 base64 图片转成 OSS URL 传给上游（DashScope 渠道需要图片地址）
        this.ossService = ossService;
    }

    /**
     * 根据配置创建 ProviderAdapter 实例（渠道 type 模式，兼容老调用）
     * 走 normalizeType 模糊归一，然后按类型创建。
     */
    public ProviderAdapter create(ProviderProperties.ProviderConfig config) {
        String type = normalizeType(config.getType());
        log.info("[ProviderFactory] 创建 Provider: name={}, type={}, alias={}",
                config.getName(), type, config.getAlias());

        return switch (type) {
            case "openai_bearer" -> createOpenAiBearerProvider(config);
            case "openai_azure" -> createAzureProvider(config);
            case "anthropic" -> createAnthropicProvider(config);
            case "vertex" -> createVertexProvider(config);
            case "openai_image" -> createOpenAiImageProvider(config);
            case "dashscope_image" -> createDashScopeImageProvider(config);
            case "gemini_image" -> createGeminiImageProvider(config);
            default -> throw new IllegalArgumentException(
                    "不支持的 Provider 类型: " + config.getType() + ", 支持的类型: openai_bearer, openai_azure, anthropic, vertex, openai_image, dashscope_image, gemini_image");
        };
    }

    /**
     * 按 provider_alias 创建 ProviderAdapter 实例（模型级绑定模式，主入口）
     * 精确匹配枚举 alias，不做模糊归一；alias 必须存在于 {@link ProviderCapabilityCatalog}，
     * 否则抛 IllegalArgumentException（由 ProviderRegistry 启动/动态加载时跳过并告警）。
     *
     * 建实例按 alias 建，不是按绑定行 JSON 里的 class_name 反射——各 Adapter 构造器依赖
     * Spring 注入的 FormatConverter / ObjectMapper / OssService，类名反射拼不出。
     */
    public ProviderAdapter createByAlias(String providerAlias, ProviderProperties.ProviderConfig config) {
        log.info("[ProviderFactory] 按 providerAlias 创建 Provider: alias={}, name={}", providerAlias, config.getName());

        return switch (providerAlias) {
            case "openai_bearer" -> createOpenAiBearerProvider(config);
            case "openai_azure" -> createAzureProvider(config);
            case "anthropic" -> createAnthropicProvider(config);
            case "vertex" -> createVertexProvider(config);
            case "openai_image" -> createOpenAiImageProvider(config);
            case "dashscope_image" -> createDashScopeImageProvider(config);
            case "gemini_image" -> createGeminiImageProvider(config);
            case "dashscope_video" -> createDashScopeVideoProvider(config);
            case "volcengine_video" -> createVolcengineVideoProvider(config);
            // ==================== 声明式能力（占位） ====================
            // 这些 alias 的 domain（audio/embedding/rerank）或尚未接真实 Adapter 的未来能力，
            // 一律返回 DeclarativeCapabilityAdapter（chat 调用明确报错）；仅让绑定行可加载、admin 可绑定。
            case "zhipu" -> createDeclarativeCapability(config, "zhipu", "智谱 GLM",
                    "未接入真实 Adapter，请联系管理员配置后使用");
            case "moonshot" -> createDeclarativeCapability(config, "moonshot", "Moonshot Kimi",
                    "未接入真实 Adapter，请联系管理员配置后使用");
            case "azure_image" -> createDeclarativeCapability(config, "azure_image", "Azure 图像",
                    "未接入真实 Adapter，请联系管理员配置后使用");
            case "volcengine_image" -> createDeclarativeCapability(config, "volcengine_image", "火山 Seedream 图像",
                    "未接入真实 Adapter，请联系管理员配置后使用");
            case "gemini_video" -> createDeclarativeCapability(config, "gemini_video", "Gemini Veo 生视频",
                    "未接入真实 Adapter，请联系管理员配置后使用");
            case "openai_video" -> createDeclarativeCapability(config, "openai_video", "OpenAI Sora 生视频",
                    "未接入真实 Adapter，请联系管理员配置后使用");
            case "dashscope_asr" -> createDeclarativeCapability(config, "dashscope_asr", "通义语音转写",
                    "语音转写能力未启用（语音接口已下线）");
            case "dashscope_tts" -> createDeclarativeCapability(config, "dashscope_tts", "通义语音合成",
                    "语音合成能力未启用（语音接口已下线）");
            case "volcengine_tts" -> createDeclarativeCapability(config, "volcengine_tts", "火山语音合成",
                    "语音合成能力未启用（语音接口已下线）");
            case "azure_tts" -> createDeclarativeCapability(config, "azure_tts", "Azure 语音合成",
                    "语音合成能力未启用（语音接口已下线）");
            case "openai_embedding" -> createDeclarativeCapability(config, "openai_embedding", "OpenAI 向量",
                    "向量走 EmbeddingService，使用 /v1/embeddings");
            case "dashscope_embedding" -> createDeclarativeCapability(config, "dashscope_embedding", "通义向量",
                    "向量走 EmbeddingService，使用 /v1/embeddings");
            case "gemini_embedding" -> createDeclarativeCapability(config, "gemini_embedding", "Gemini 向量",
                    "向量走 EmbeddingService，使用 /v1/embeddings");
            case "volcengine_embedding" -> createDeclarativeCapability(config, "volcengine_embedding", "火山向量",
                    "向量走 EmbeddingService，使用 /v1/embeddings");
            case "dashscope_rerank" -> createDeclarativeCapability(config, "dashscope_rerank", "通义重排",
                    "重排走 RerankService（规划中），配置链路照 TTS/ASR");
            case "cohere_rerank" -> createDeclarativeCapability(config, "cohere_rerank", "Cohere 重排",
                    "重排走 RerankService（规划中），配置链路照 TTS/ASR");
            default -> throw new IllegalArgumentException(
                    "未知 providerAlias: " + providerAlias + ", 可用: " + ProviderCapabilityCatalog.list());
        };
    }

    /**
     * 规范化渠道类型
     * 将数据库中的类型映射到内部支持的类型
     */
    private String normalizeType(String type) {
        if (type == null) {
            return "openai_bearer";
        }
        return switch (type.toLowerCase()) {
            case "openai", "openai_bearer" -> "openai_bearer";
            case "openai_azure", "azure" -> "openai_azure";
            case "claude", "anthropic" -> "anthropic";
            case "vertex", "gemini" -> "vertex";
            case "openai_image", "openai_images", "azure_image" -> "openai_image";
            case "dashscope_image", "aliyun_image" -> "dashscope_image";
            case "gemini_image" -> "gemini_image";
            case "dashscope_video", "aliyun_video" -> "dashscope_video";
            case "volcengine_video" -> "volcengine_video";
            default -> type.toLowerCase();
        };
    }

    /**
     * 判断渠道类型是否为生视频类型（dashscope_video/aliyun_video/volcengine_video）。
     * 这类渠道由 {@link #createVideoAdapter} 注册到视频专用表，不参与通用（文本/图像）注册通道，
     * 通用通道调用 {@link #create} 时会因不支持视频类型抛 IllegalArgumentException。
     */
    public boolean isVideoType(String rawType) {
        String normalized = normalizeType(rawType);
        return "dashscope_video".equals(normalized) || "volcengine_video".equals(normalized);
    }

    // ==================== 各类型创建方法 ====================

    /**
     * 创建标准 Bearer Token 认证的 OpenAI 兼容 Provider
     * 适用于: DashScope, DeepSeek 等
     */
    private ProviderAdapter createOpenAiBearerProvider(ProviderProperties.ProviderConfig config) {
        return new OpenAiCompatibleAdapter(
                config.getBaseUrl(),
                openAiFormatConverter,
                builder -> builder.defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + config.getApiKey()),
                objectMapper,
                config.getApiKeys(),
                config.getTokenIds()
        ) {
            @Override
            public String getProviderAlias() {
                return config.getAlias();
            }

            @Override
            public String getProviderName() {
                return config.getName();
            }
        };
    }

    /**
     * 创建 Azure OpenAI Provider
     */
    private ProviderAdapter createAzureProvider(ProviderProperties.ProviderConfig config) {
        return new AzureProviderAdapter(
                config.getBaseUrl(),
                config.getApiKey(),
                openAiFormatConverter,
                config.getName(),
                config.getAlias(),
                objectMapper,
                config.getApiKeys(),
                config.getTokenIds()
        );
    }

    /**
     * 创建 Anthropic Claude Provider
     */
    private ProviderAdapter createAnthropicProvider(ProviderProperties.ProviderConfig config) {
        return new AnthropicProviderAdapter(
                config.getBaseUrl(),
                config.getApiKey(),
                anthropicFormatConverter,
                config.getName(),
                config.getAlias(),
                config.getApiKeys(),
                config.getTokenIds()
        );
    }

    /**
     * 创建 Gemini API Provider
     */
    private ProviderAdapter createVertexProvider(ProviderProperties.ProviderConfig config) {
        return new VertexProviderAdapter(
                config.getBaseUrl(),
                config.getApiKey(),
                vertexFormatConverter,
                config.getName(),
                config.getAlias(),
                objectMapper,
                config.getApiKeys(),
                config.getTokenIds()
        );
    }

    // ==================== 图像渠道 ====================

    /**
     * 创建 OpenAI / Azure 图像 Provider（images/generations + images/edits）
     */
    private ProviderAdapter createOpenAiImageProvider(ProviderProperties.ProviderConfig config) {
        return new OpenAiImageAdapter(
                config.getBaseUrl(),
                config.getApiKey(),
                config.getName(),
                config.getAlias(),
                objectMapper,
                config.getApiKeys(),
                config.getTokenIds()
        );
    }

    /**
     * 创建阿里云 DashScope 图像 Provider（qwen-image，multimodal-generation 单接口）
     */
    private ProviderAdapter createDashScopeImageProvider(ProviderProperties.ProviderConfig config) {
        return new DashScopeImageAdapter(
                config.getBaseUrl(),
                config.getApiKey(),
                config.getName(),
                config.getAlias(),
                objectMapper,
                ossService,
                config.getApiKeys(),
                config.getTokenIds()
        );
    }

    /**
     * 创建 Gemini 图像 Provider（nano banana / vapeur，generateContent 单接口）
     */
    private ProviderAdapter createGeminiImageProvider(ProviderProperties.ProviderConfig config) {
        return new GeminiImageAdapter(
                config.getBaseUrl(),
                config.getApiKey(),
                config.getName(),
                config.getAlias(),
                objectMapper,
                config.getApiKeys(),
                config.getTokenIds()
        );
    }

    // ==================== 视频渠道 ====================

    /**
     * 创建视频 ProviderAdapter（生视频专用，与文本/图像渠道独立注册表）
     *
     * 选择规则：优先按渠道 type（dashscope_video/aliyun_video → DashScope，volcengine_video → Volcengine），
     * 无显式视频类型时按 alias 兜底（volcengine → Volcengine，其余 → DashScope）。
     * 当前生视频渠道 type=openai（与聊天共用渠道记录），靠 alias 判定：
     *   aliyun → DashScopeVideoAdapter（happyhorse/wan），volcengine → VolcengineVideoAdapter（seedance）
     */
    public ProviderAdapter createVideoAdapter(ProviderProperties.ProviderConfig config) {
        String type = normalizeType(config.getType());
        String alias = config.getAlias() != null ? config.getAlias().toLowerCase() : "";

        if (type.equals("volcengine_video") || alias.equals("volcengine")) {
            return createByAlias("volcengine_video", config);
        }
        return createByAlias("dashscope_video", config);
    }

    // ==================== 视频创建方法 ====================

    private ProviderAdapter createDashScopeVideoProvider(ProviderProperties.ProviderConfig config) {
        log.info("[ProviderFactory] 创建视频 Provider(DashScope): name={}, alias={}", config.getName(), config.getAlias());
        return new DashScopeVideoAdapter(
                config.getBaseUrl(), config.getApiKey(), config.getName(), config.getAlias(), objectMapper,
                config.getApiKeys(), config.getTokenIds());
    }

    private ProviderAdapter createVolcengineVideoProvider(ProviderProperties.ProviderConfig config) {
        log.info("[ProviderFactory] 创建视频 Provider(Volcengine): name={}, alias={}", config.getName(), config.getAlias());
        return new VolcengineVideoAdapter(
                config.getBaseUrl(), config.getApiKey(), config.getName(), config.getAlias(), objectMapper,
                config.getApiKeys(), config.getTokenIds());
    }

    // ==================== 声明式能力创建方法 ====================

    /**
     * 创建【声明式】能力 Provider（audio/embedding/rerank 域 + 未接真实 Adapter 的未来能力）。
     * 这些能力运行时走独立服务编排（EmbeddingService 等）
     * 或尚未接入，不经 LlmGateway；此实例仅让 proxy_channel_models 绑定行可被注册表加载
     * （不报错）+ provider_capabilities 能力条目的实例存在，chat 调用会明确报错（不会被触发）。
     *
     * @param config     渠道配置
     * @param alias      能力 alias（与 ProviderCapabilityCatalog 一致）
     * @param name       能力展示名
     * @param routeHint  真实路由去向，用于 chat 误调时的错误提示
     */
    private ProviderAdapter createDeclarativeCapability(ProviderProperties.ProviderConfig config,
                                                        String alias, String name, String routeHint) {
        log.info("[ProviderFactory] 创建声明式能力 Provider: alias={}, name={}, channel={}",
                alias, name, config.getName());
        return new DeclarativeCapabilityAdapter(alias, name, routeHint);
    }
}
