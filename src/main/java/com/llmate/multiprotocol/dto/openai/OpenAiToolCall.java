package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 符合 OpenAI 规范的非流式工具调用数据结构
 * 对应响应中 message.tool_calls 数组中的元素
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiToolCall {

    /** 工具调用的唯一标识 (如 "call_abc123") */
    private String id;

    /** 工具类型，OpenAI 标准固定为 "function" */
    @Builder.Default
    private String type = "function";

    /** 函数调用详情 */
    private OpenAiFunctionCall function;

    /**
     * OpenAI 函数调用详情
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class OpenAiFunctionCall {
        /** 被调用的函数名称 */
        private String name;
        /** 函数入参的完整 JSON 字符串 */
        private String arguments;
    }
}
