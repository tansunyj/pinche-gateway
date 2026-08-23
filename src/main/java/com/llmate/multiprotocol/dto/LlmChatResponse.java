package com.llmate.multiprotocol.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import java.util.List;

@Data
public class LlmChatResponse {
    private String id;
    private String model;
    private List<Choice> choices;
    private Usage usage; // 💡 确保这里有声明 usage 属性

    /**
     * 图像生成/编辑结果（requestType 为 IMAGE_* 时使用）
     */
    private List<LlmImage> images;

    /**
     * 原始请求中的模型ID（用于反向映射）
     * @JsonIgnore：内部路由上下文，不随响应对象序列化（避免泄漏进 proxy_request_logs.response_body）
     */
    @JsonIgnore
    private String originalModelId;

    /**
     * 渠道ID（用于反向映射）
     * @JsonIgnore：内部路由上下文，不随响应对象序列化
     */
    @JsonIgnore
    private Long channelId;

    /**
     * 响应应返回的模型名（经过反向映射后）
     * @JsonIgnore：内部路由上下文，不随响应对象序列化
     */
    @JsonIgnore
    private String responseModelId;

    @Data
    public static class Choice {
        private int index;
        private Message message;
        private String finishReason;
    }

    @Data
    public static class Message {
        private String role;
        private String content;

        /** assistant 消息中的工具调用列表 */
        private List<LlmToolCall> toolCalls;

        /** tool 角色消息中，对应的工具调用 ID */
        private String toolCallId;

        /** tool 角色消息中，被调用的函数名称 */
        private String name;
    }

    // 💡 补充缺少的 Usage 静态内部类
    @Data
    public static class Usage {
        private int promptTokens;
        private int completionTokens;
        private int totalTokens;

        /** 推理/思考 tokens（DeepSeek/DashScope: completion_tokens_details.reasoning_tokens） */
        private int reasoningTokens;

        /** 缓存命中 tokens（DeepSeek: prompt_cache_hit_tokens / prompt_tokens_details.cached_tokens） */
        private int cacheHitTokens;

        /** 缓存未命中 tokens（DeepSeek: prompt_cache_miss_tokens） */
        private int cacheMissTokens;

        /** 缓存创建 tokens（Anthropic: cache_creation_input_tokens） */
        private int cacheCreationTokens;

        /** 缓存读取 tokens（Anthropic: cache_read_input_tokens） */
        private int cacheReadTokens;

        /** 缓存命中 tokens（OpenAI/Azure: prompt_tokens_details.cached_tokens，与流式 LlmUsage.cachedTokens 对齐） */
        private int cachedTokens;

        /** 生成的图片张数（image 计费模式用） */
        private int imageCount;

        // ========== image_token 计费模式维度（gpt-image 等图文混合模型） ==========
        private long inputTextTokens;
        private long inputImageTokens;
        private long outputTextTokens;
        private long outputImageTokens;
    }
}