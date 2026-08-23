package com.llmate.multiprotocol.dto.anthropic;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 符合 Anthropic 规范的 /v1/messages 响应体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnthropicMessagesResponse {

    private String id;
    private String type; // "message"
    private String role; // "assistant"
    private String model;
    private List<AnthropicContent> content;
    private String stopReason;
    private String stopSequence;
    private AnthropicUsage usage;

    /**
     * Anthropic 内容块（支持文本、工具调用等）
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnthropicContent {
        private String type; // "text", "tool_use", "thinking"
        private String text;
        private String id; // tool_use id
        private String name; // tool name
        private Object input; // tool input
    }
}
