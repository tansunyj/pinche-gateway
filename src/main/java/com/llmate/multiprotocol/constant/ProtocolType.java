package com.llmate.multiprotocol.constant;

import lombok.Getter;

/**
 * 协议类型枚举
 * 标识系统支持的外部请求协议以及内部核心协议标准
 */
@Getter
public enum ProtocolType {

    /**
     * 网关内部核心标准协议 (通常基于标准的 OpenAI 规范进行扩展)
     */
    INTERNAL("internal", "LLMate 内部标准协议"),

    /**
     * OpenAI 聊天补全协议 (入口: /v1/chat/completions)
     */
    OPENAI_CHAT_COMPLETIONS("openai_chat", "OpenAI Chat Completions 协议"),

    /**
     * OpenAI Responses API 协议 (入口: /v1/responses)
     */
    OPENAI_RESPONSES("openai_responses", "OpenAI Responses API 协议"),

    /**
     * OpenAI 图像生成协议 (入口: /v1/images/generations)
     */
    OPENAI_IMAGES("openai_images", "OpenAI Images 协议"),

    /**
     * OpenAI 视频生成协议 (入口: /v1/videos/generations)
     */
    OPENAI_VIDEOS("openai_videos", "OpenAI Videos 协议"),

    /**
     * OpenAI 向量嵌入协议 (入口: /v1/embeddings)
     */
    OPENAI_EMBEDDINGS("openai_embeddings", "OpenAI Embeddings 协议"),

    /**
     * Anthropic Claude 消息协议 (入口: /v1/messages)
     */
    ANTHROPIC_MESSAGES("anthropic_messages", "Anthropic Messages 协议"),

    /**
     * Google Gemini 官方原生协议 (入口: /v1beta/models/...:generateContent)
     */
    GOOGLE_GEMINI("google_gemini", "Google Gemini 原生协议"),

    /**
     * Ollama 本地部署服务原生协议 (入口: /api/chat 或 /api/generate)
     */
    OLLAMA_NATIVE("ollama_native", "Ollama 本地原生协议");

    /**
     * 协议的唯一字符串标识，常用于数据库配置、YAML 路由匹配或日志 MDC 埋点
     */
    private final String code;

    /**
     * 协议的可读性描述，方便在管理后台可视化看板中展示
     */
    private final String description;

    ProtocolType(String code, String description) {
        this.code = code;
        this.description = description;
    }

    /**
     * 根据字符串 code 安全地反查对应的枚举对象（支持兜底，防止系统报 IllegalArgumentException）
     * * @param code 外部传入的协议标识符
     * @return 对应的 ProtocolType，若匹配不到则返回 INTERNAL 内部原生协议进行兜底
     */
    public static ProtocolType fromCode(String code) {
        if (code == null || code.isBlank()) {
            return INTERNAL;
        }
        for (ProtocolType type : ProtocolType.values()) {
            if (type.getCode().equalsIgnoreCase(code.trim())) {
                return type;
            }
        }
        return INTERNAL;
    }
}
