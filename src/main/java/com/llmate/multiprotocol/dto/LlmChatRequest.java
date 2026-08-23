package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 网关内部标准统一聊天请求体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmChatRequest {

    /** 目标模型名称 */
    private String model;

    /** 结构化消息列表 */
    @Builder.Default
    private List<LlmMessage> messages = new ArrayList<>();

    /** 采样温度 (0.0 ~ 2.0) */
    private Double temperature;

    /** 最大生成 Token 数 */
    private Integer maxTokens;

    /** 是否为流式（SSE）请求 */
    private Boolean stream;

    /** 请求类型（默认 CHAT_COMPLETION），LlmGateway 据此派发到 ProviderAdapter 对应方法 */
    private LlmRequestType requestType;

    /** 图像生成/编辑参数（requestType 为 IMAGE_* 时使用） */
    private LlmImageParams imageParams;

    /** 视频生成参数（requestType 为 VIDEO_GENERATION 时使用） */
    private LlmVideoParams videoParams;

    // ========== 透传字段（跨协议零遗漏）==========

    /** 工具定义列表（Anthropic tools / OpenAI tools / Responses tools 统一转成此格式） */
    private List<LlmToolDefinition> tools;

    /** 工具选择策略（Anthropic tool_choice / OpenAI tool_choice / Responses tool_choice） */
    private Object toolChoice;

    /** 推理/思考配置（Anthropic thinking / OpenAI reasoning，原样透传给上游转换器） */
    private Object thinking;

    /** 核采样 top_p */
    private Double topP;

    /** 核采样 top_k */
    private Integer topK;

    /** 停止序列（Anthropic stop_sequences / OpenAI stop） */
    private List<String> stopSequences;

    /**
     * 入口协议中未被显式建模的其余字段（metadata / output_config / stream_options / cache_control 等）。
     * 由各入口转换器把外部请求的 @JsonAnySetter 收集结果搬进来，再由各上游转换器以
     * @JsonAnyGetter 原样合并回目标渠道请求体，保证任意新增字段不遗漏。
     */
    @Builder.Default
    private Map<String, Object> extraParams = new LinkedHashMap<>();

    // ========== 路由上下文信息（用于模型映射）==========

    /** 用户原始请求的模型ID */
    private String originalModelId;

    /** 渠道ID（用于反向映射） */
    private Long channelId;

    /** 渠道代码 */
    private String channelCode;
}