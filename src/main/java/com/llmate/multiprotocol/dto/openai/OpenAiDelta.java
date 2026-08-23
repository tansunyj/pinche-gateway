package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder // 💡 加上这个注解，完美解决 OpenAiDeltaBuilder 和 builder() 找不到的问题
@NoArgsConstructor // 💡 确保 Jackson 反序列化需要的无参构造函数存在
@AllArgsConstructor // 💡 配合 @Builder 必须存在的全参构造函数
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiDelta {

    private String role;

    private String content;

    @JsonProperty("reasoning_content")
    private String reasoningContent;

    /**
     * 流式工具/函数调用的增量内容列表
     * 当模型决定调用外部 Function 时，增量参数会流式塞入这里
     */
    @JsonProperty("tool_calls")
    private List<OpenAiToolCallDelta> toolCalls;
}