package com.llmate.multiprotocol.dto.anthropic;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Anthropic 流式响应事件（SSE格式）
 * 用于流式返回时的各种事件类型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnthropicStreamEvent {

    private String type; // message_start, content_block_start, content_block_delta, content_block_stop, message_delta, message_stop

    // message_start fields
    private AnthropicMessage message;

    // content_block_start / content_block_delta fields
    private Integer index;
    private AnthropicDelta delta;

    // content_block_start: Anthropic 官方格式使用 "content_block" 字段（非 "delta"）
    @JsonProperty("content_block")
    private java.util.Map<String, Object> contentBlock;

    // message_delta fields (官方格式: type + delta + usage 顶层平铺)
    private AnthropicUsage usage;

    // error fields
    private AnthropicError error;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnthropicMessage {
        private String id;
        private String type;
        private String role;
        private String model;
        // 官方 message_start 格式必有 content:[]（严格客户端会校验该字段，缺失可能导致解析中止）。
        // @Builder.Default 保证 builder 未显式设置时仍序列化为空数组而非 null。
        @Builder.Default
        private List<Object> content = java.util.List.of();
        @JsonProperty("stop_reason")
        private String stopReason;
        @JsonProperty("stop_sequence")
        private String stopSequence;
        private AnthropicUsage usage;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnthropicDelta {
        private String type; // text_delta, input_json_delta, thinking_delta, signature_delta
        private String text;
        @JsonProperty("stop_reason")
        private String stopReason;
        /** tool_use 块的增量 JSON 片段（input_json_delta 事件） */
        @JsonProperty("partial_json")
        private String partialJson;
        /** thinking 块的增量文本（thinking_delta 事件） */
        private String thinking;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnthropicMessageDelta {
        @JsonProperty("stop_reason")
        private String stopReason;
        @JsonProperty("stop_sequence")
        private String stopSequence;
        private AnthropicUsage usage;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnthropicError {
        private String type;
        private String message;
    }
}
