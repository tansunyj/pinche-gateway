package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * OpenAI Responses API 响应体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OpenAiResponsesResponse {

    private String id;
    private String object; // "response"
    @JsonProperty("created_at")
    private Long createdAt;
    private String model;
    private List<OutputItem> output;
    private Usage usage;
    private String status;
    private Error error;
    private IncompleteDetails incompleteDetails;
    private Map<String, String> metadata;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OutputItem {
        private String type; // "message", "reasoning", "function_call", "function_call_output"
        private String id;
        private String status;
        private String role;
        private List<ContentItem> content;
        private Object arguments; // for function_call: JSON string per OpenAI SDK ResponseFunctionToolCall spec
        private String name; // for function_call
        private String callId; // for function_call_output
        private String output; // for function_call_output
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ContentItem {
        private String type; // "output_text", "refusal"
        private String text;
        private List<Annotation> annotations;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Annotation {
        private String type; // "file_citation", "url_citation"
        private String index;
        private String fileId;
        private String filename;
        private String url;
        private String title;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Usage {
        @JsonProperty("input_tokens")
        private int inputTokens;
        @JsonProperty("output_tokens")
        private int outputTokens;
        @JsonProperty("total_tokens")
        private int totalTokens;
        @JsonProperty("input_tokens_details")
        private InputTokensDetails inputTokensDetails;
        @JsonProperty("output_tokens_details")
        private OutputTokensDetails outputTokensDetails;

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class InputTokensDetails {
            /** 缓存命中 tokens（上游 prompt_tokens_details.cached_tokens） */
            @JsonProperty("cached_tokens")
            private int cachedTokens;
        }

        @Data
        @Builder
        @NoArgsConstructor
        @AllArgsConstructor
        public static class OutputTokensDetails {
            /** 推理/思考 tokens（上游 completion_tokens_details.reasoning_tokens） */
            @JsonProperty("reasoning_tokens")
            private int reasoningTokens;
        }
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Error {
        private String type;
        private String message;
        private String param;
        private String code;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class IncompleteDetails {
        private String reason; // "max_output_tokens", "content_filter"
    }
}
