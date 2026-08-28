package com.llmate.multiprotocol.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmStreamChunk {
    private String id;
    private String model;
    private boolean isFirstChunk;
    private boolean isFinished;
    private String deltaContent;
    /** 推理/思考增量（reasoning_content）：与正文 deltaContent 分开存放，下游按协议映射到推理字段（如 OpenAI reasoning_content），不得拼入 content */
    private String deltaReasoningContent;

    // 流式复杂工具调用增量级属性组
    private String toolCallId;
    private Integer toolCallIndex;
    private String toolCallName;
    private String toolCallArgumentsDelta; // 增量 JSON 片段，例如： "{\"loca", "tion\":", "\"Beijing\"}"

    private LlmUsage usage; // 尾部 Chunk 或特有事件中携带的 Token 统计

    /** 上游原始结束原因，如 "stop"、"tool_calls"、"length" 等；由 Provider 转换层写入，由协议转换层透传 */
    private String finishReason;

    // ========== 路由上下文信息（用于模型映射）==========

    /** 用户原始请求的模型ID（用于反向映射）
     * @JsonIgnore：内部路由上下文，不随流式块对象序列化（避免泄漏进日志/SSE回退JSON） */
    @JsonIgnore
    private String originalModelId;

    /** 响应应返回的模型名（经过反向映射后）
     * @JsonIgnore：内部路由上下文，不随流式块对象序列化 */
    @JsonIgnore
    private String responseModelId;
}