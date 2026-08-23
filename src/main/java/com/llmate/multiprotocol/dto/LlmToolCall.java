package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 网关内部标准工具调用数据结构
 * 用于在 LlmMessage 和 LlmChatResponse.Message 中传递工具调用信息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmToolCall {

    /** 工具调用的唯一标识 (如 OpenAI 的 "call_abc123" 或 Anthropic 的 "toolu_xxx") */
    private String id;

    /** 工具类型，目前固定为 "function" */
    @Builder.Default
    private String type = "function";

    /** 被调用的函数名称 */
    private String name;

    /** 函数入参的完整 JSON 字符串 (如 "{\"city\": \"Beijing\"}") */
    private String arguments;

    /** Gemini thinking 模式下的思考签名，用于跨请求保留以通过 Gemini 校验 */
    private String thoughtSignature;
}
