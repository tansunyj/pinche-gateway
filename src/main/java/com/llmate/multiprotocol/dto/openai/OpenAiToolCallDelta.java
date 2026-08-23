package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiToolCallDelta {

    /**
     * 区分并映射多个并发工具调用的索引下标（从 0 开始）
     */
    private Integer index;

    /**
     * 工具调用的唯一 ID（例如: call_abc123），通常也在前几个 Chunk 中返回
     */
    private String id;

    /**
     * 具体的工具类型，目前 OpenAI 标准固定为 "function"
     */
    @Builder.Default
    private String type = "function";

    /**
     * 具体的函数增量详情
     */
    private OpenAiFunctionDelta function;
}