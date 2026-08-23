package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 符合 OpenAI 标准的 Chat Completions 阻塞响应体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OpenAiChatResponse {

    private String id;

    @Builder.Default
    private String object = "chat.completion";

    private Long created;

    private String model;

    @JsonProperty("system_fingerprint")
    private String systemFingerprint;

    private List<Choice> choices;

    private OpenAiUsage usage;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Choice {
        private Integer index;
        private ResponseMessage message;

        @JsonProperty("finish_reason")
        private String finishReason;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResponseMessage {
        private String role;
        private String content;

        /** assistant 消息中的工具调用列表 */
        @JsonProperty("tool_calls")
        private List<OpenAiToolCall> toolCalls;

        /** tool 角色消息中，对应的工具调用 ID */
        @JsonProperty("tool_call_id")
        private String toolCallId;

        /** tool 角色消息中，被调用的函数名称 */
        private String name;
    }
}