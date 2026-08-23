package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenAI Responses API 请求体 (兼容 /v1/responses)
 * 这是 OpenAI 较新的 API，用于 o1 等推理模型
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OpenAiResponsesRequest {

    private String model;

    /** 输入内容，可以是字符串或结构化输入列表 */
    private Object input;

    /** 推理参数（o1系列模型专用） */
    private Reasoning reasoning;

    /** 工具列表 */
    private List<Map<String, Object>> tools;

    /** 工具选择策略 */
    @JsonProperty("tool_choice")
    private Object toolChoice;

    /** 最大输出token数 */
    @JsonProperty("max_output_tokens")
    private Integer maxOutputTokens;

    /** 是否流式输出 */
    private Boolean stream;

    /** 流式选项 */
    @JsonProperty("stream_options")
    private StreamOptions streamOptions;

    /** 元数据 */
    private Map<String, String> metadata;

    /** 是否包含 reasoning 内容 */
    @JsonProperty("include_reasoning")
    private Boolean includeReasoning;

    // ===== 透传字段：跨协议零遗漏 =====
    private Double temperature;
    @JsonProperty("top_p")
    private Double topP;
    @JsonProperty("top_k")
    private Integer topK;

    /** 未显式建模字段兜底收集 */
    private Map<String, Object> extraParams = new LinkedHashMap<>();

    @JsonAnySetter
    public void putExtraParam(String key, Object value) {
        if (this.extraParams == null) {
            this.extraParams = new LinkedHashMap<>();
        }
        this.extraParams.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getExtraParams() {
        return this.extraParams;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Reasoning {
        /** 推理努力程度: low, medium, high */
        private String effort;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamOptions {
        @JsonProperty("include_usage")
        private Boolean includeUsage;
    }
}
