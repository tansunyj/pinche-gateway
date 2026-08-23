package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiStreamChoice {

    /** 选择分支的下标索引，单路输出时固定为 0 */
    private Integer index;

    /** * 核心增量文本块（包含当前吐出的字符、或者是工具函数调用的参数碎片）
     */
    private OpenAiDelta delta;

    /**
     * 流的结束标识原因
     * 流进行中为 null，当到达最后一个文本块时，官方标准通常返回 "stop"；触发工具调用时返回 "tool_calls"
     */
    @JsonProperty("finish_reason")
    private String finishReason;

    /**
     * 上游 Provider（如阿里云百炼）可能返回的 logprobs 字段
     * 本网关不消费该字段，仅声明以避免 Jackson 反序列化报错
     */
    private Object logprobs;
}