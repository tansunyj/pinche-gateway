package com.llmate.multiprotocol.engine.provider;

import java.util.Arrays;
import java.util.List;

/**
 * Adapter 能力清单（真相源，不落库）
 *
 * 描述网关支持的 Provider Adapter 能力，三个字段与 {@code proxy_channel_models.provider_capability}
 * JSON 快照对应：
 * - {@code provider_alias}：路由用，绑定行 JSON 里的 provider_alias 必须在此枚举中存在；
 * - {@code domain}：能力域（chat/image/video/audio/embedding/rerank），注册表据此分域
 *   （providers / videoProviders）；audio/embedding/rerank 域是<b>声明式</b>——运行时走
 *   独立服务编排（如 EmbeddingService）或仅占位，不经 LlmGateway，
 *   注册表按非 video 桶装载仅为让绑定行可加载、不报错；
 * - {@code class_name}：Adapter 实现类全限定名，作为绑定行自描述快照（后台展示/排查用），
 *   <b>不参与实例化</b>——建 adapter 走 {@link ProviderFactory#createByAlias}。
 *
 * 角色：
 * - 后台下拉数据源（{@code GET /admin/provider-capabilities} 从枚举生成，按 domain 分组）
 * - alias 存在性校验、domain/class_name 推导（网关校验用）
 * - 注册表分域（isVideo/isImage）
 *
 * 注意：协议只能由绑定界面在 proxy_channel_models.provider_capability 显式设置，本枚举
 * <b>不做</b>按渠道 type 推导默认 alias（proxy_channels.type 单值字段无意义，不参与路由）。
 *
 * 新增 Adapter 能力 = 改本枚举（含 class_name）+ ProviderFactory.createByAlias 加分支，
 * 两处同源不易漂移；重启后后台下拉自动出现。
 */
public enum ProviderCapabilityCatalog {

    // 域划分对齐 model_library.category：chat(含 reasoning) / image / video / audio(ASR+TTS) / embedding / rerank
    // 真实路由 domain（chat/image/video）走 LlmGateway；声明式 domain（audio/embedding/rerank）
    // 以及未接真实 Adapter 的未来 alias 一律用 DeclarativeCapabilityAdapter 占位（见其类注释）。
    // 每加一个 alias 必须同步给 ProviderFactory.createByAlias 加 case，否则绑定行加载报 error。

    // domain, provider_alias, name, class_name
    OPENAI_BEARER("chat", "openai_bearer", "OpenAI 兼容",
            "com.llmate.multiprotocol.engine.provider.openai.OpenAiCompatibleAdapter"),
    OPENAI_AZURE("chat", "openai_azure", "Azure OpenAI",
            "com.llmate.multiprotocol.engine.provider.openai.AzureProviderAdapter"),
    ANTHROPIC("chat", "anthropic", "Anthropic Claude",
            "com.llmate.multiprotocol.engine.provider.anthropic.AnthropicProviderAdapter"),
    VERTEX("chat", "vertex", "Google Vertex",
            "com.llmate.multiprotocol.engine.provider.vertex.VertexProviderAdapter"),
    OPENAI_IMAGE("image", "openai_image", "OpenAI 图像",
            "com.llmate.multiprotocol.engine.provider.image.OpenAiImageAdapter"),
    DASHSCOPE_IMAGE("image", "dashscope_image", "通义万相图像",
            "com.llmate.multiprotocol.engine.provider.image.DashScopeImageAdapter"),
    GEMINI_IMAGE("image", "gemini_image", "Gemini 图像",
            "com.llmate.multiprotocol.engine.provider.image.GeminiImageAdapter"),
    DASHSCOPE_VIDEO("video", "dashscope_video", "通义万相生视频",
            "com.llmate.multiprotocol.engine.provider.video.DashScopeVideoAdapter"),
    VOLCENGINE_VIDEO("video", "volcengine_video", "豆包生视频",
            "com.llmate.multiprotocol.engine.provider.video.VolcengineVideoAdapter"),

    DASHSCOPE_ASR("audio", "dashscope_asr", "通义语音转写",
            "com.llmate.multiprotocol.engine.provider.DeclarativeCapabilityAdapter"),
    DASHSCOPE_TTS("audio", "dashscope_tts", "通义语音合成",
            "com.llmate.multiprotocol.engine.provider.DeclarativeCapabilityAdapter"),

    DASHSCOPE_EMBEDDING("embedding", "dashscope_embedding", "通义向量",
            "com.llmate.multiprotocol.engine.provider.DeclarativeCapabilityAdapter");

    private final String domain;
    private final String providerAlias;
    private final String name;
    private final String className;

    ProviderCapabilityCatalog(String domain, String providerAlias, String name, String className) {
        this.domain = domain;
        this.providerAlias = providerAlias;
        this.name = name;
        this.className = className;
    }

    public String getDomain() {
        return domain;
    }

    public String getProviderAlias() {
        return providerAlias;
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    /** 按 provider_alias 精确查找；未知返回 null */
    public static ProviderCapabilityCatalog findByAlias(String providerAlias) {
        if (providerAlias == null || providerAlias.isBlank()) {
            return null;
        }
        for (ProviderCapabilityCatalog c : values()) {
            if (c.providerAlias.equals(providerAlias)) {
                return c;
            }
        }
        return null;
    }

    /** alias → domain；未知返回 null */
    public static String findDomain(String providerAlias) {
        ProviderCapabilityCatalog c = findByAlias(providerAlias);
        return c != null ? c.domain : null;
    }

    /** alias → class_name；未知返回 null */
    public static String findClassName(String providerAlias) {
        ProviderCapabilityCatalog c = findByAlias(providerAlias);
        return c != null ? c.className : null;
    }

    /** alias 是否 video 域 */
    public static boolean isVideo(String providerAlias) {
        return "video".equals(findDomain(providerAlias));
    }

    /** alias 是否 image 域 */
    public static boolean isImage(String providerAlias) {
        return "image".equals(findDomain(providerAlias));
    }

    /** 全量清单（保持枚举声明顺序） */
    public static List<ProviderCapabilityCatalog> list() {
        return Arrays.asList(values());
    }
}
