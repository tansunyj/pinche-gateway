package com.llmate.multiprotocol.dto.vertex;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Google Vertex AI GenerateContent 响应体
 * 同时用于非流式和流式响应
 *
 * 流式时每个 SSE data 行都是完整的响应对象，delta 在 candidates[0].content.parts[0].text 中
 *
 * 参考: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.publishers.models/generateContent
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VertexGenerateContentResponse {

    /** 候选回复列表 */
    private List<VertexCandidate> candidates;

    /** Token 使用统计 */
    @JsonProperty("usageMetadata")
    private VertexUsageMetadata usageMetadata;

    /** 模型反馈（安全过滤等） */
    @JsonProperty("promptFeedback")
    private VertexPromptFeedback promptFeedback;

    // ========== 内嵌类 ==========

    /**
     * 候选回复
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VertexCandidate {
        private VertexContent content;

        @JsonProperty("finishReason")
        private String finishReason; // "STOP", "MAX_TOKENS", "SAFETY", "RECITATION", "OTHER"

        private Integer index;

        /** 安全评分 */
        @JsonProperty("safetyRatings")
        private List<Object> safetyRatings;
    }

    /**
     * 内容单元
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VertexContent {
        private List<VertexPart> parts;
        private String role; // "model"
    }

    /**
     * 内容片段
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VertexPart {
        private String text;

        /** 函数调用（模型返回工具调用时携带 {"name": "...", "args": {...}}） */
        @JsonProperty("functionCall")
        private Object functionCall;

        /** Gemini thinking 模式下的思考签名，用于后续请求中回传以通过校验 */
        @JsonProperty("thought_signature")
        private String thoughtSignature;
    }

    /**
     * Token 使用统计
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VertexUsageMetadata {
        @JsonProperty("promptTokenCount")
        private Integer promptTokenCount;

        @JsonProperty("candidatesTokenCount")
        private Integer candidatesTokenCount;

        @JsonProperty("totalTokenCount")
        private Integer totalTokenCount;

        /** 缓存命中的 tokens（Gemini cachedContent：promptTokenCount 已含该部分，此字段单列命中数量） */
        @JsonProperty("cachedContentTokenCount")
        private Integer cachedContentTokenCount;

        /** 思考/推理 tokens（Gemini 2.5：candidatesTokenCount 已含该部分，此字段单列思考用量） */
        @JsonProperty("thoughtsTokenCount")
        private Integer thoughtsTokenCount;
    }

    /**
     * 提示反馈（安全过滤信息）
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VertexPromptFeedback {
        @JsonProperty("blockReason")
        private String blockReason;

        @JsonProperty("safetyRatings")
        private List<Object> safetyRatings;
    }
}
