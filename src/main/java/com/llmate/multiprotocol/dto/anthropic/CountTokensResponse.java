package com.llmate.multiprotocol.dto.anthropic;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * POST /v1/messages/count_tokens 的响应体（Claude 官方格式）。
 * 官方响应：{"context_management":{"original_input_tokens":0},"input_tokens":2095}
 *
 * context_management 是官方字段，Claude Code 会读取，必须完整保留。
 * 中继类上游（如 dreamfly.art）可能在响应里追加额外字段（output_tokens 等），
 * 这里忽略未知字段，避免像流式 SSE 尾部数组那样把合法响应解析失败。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CountTokensResponse {

    @JsonProperty("input_tokens")
    private int inputTokens;

    /** Claude 官方字段：上下文管理信息（original_input_tokens 等） */
    @JsonProperty("context_management")
    private ContextManagement contextManagement;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ContextManagement {
        @JsonProperty("original_input_tokens")
        private long originalInputTokens;
    }
}
