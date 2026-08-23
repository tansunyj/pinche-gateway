package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiMessage {

    @JsonProperty("role")
    private String role;

    /**
     * 💡 将 content 的类型改为 Object
     * 这样无论是前端传来的字符串 "猪八戒有多少岁？" 还是包含多模态的数组 [{"type":"text", "text":"..."}]
     * Jackson 都能正确解析，并且完全兼容 MessageConverter 的转换逻辑！
     */
    @JsonProperty("content")
    private Object content;

    /** 工具调用列表（assistant 角色时使用，对应 OpenAI 标准的 tool_calls 数组） */
    @JsonProperty("tool_calls")
    private List<OpenAiToolCall> toolCalls;

    /** 工具调用 ID（tool 角色时使用，关联到对应的 assistant tool_call） */
    @JsonProperty("tool_call_id")
    private String toolCallId;

    /** 函数名称（tool 角色时使用，标识被调用的工具名称） */
    @JsonProperty("name")
    private String name;
}