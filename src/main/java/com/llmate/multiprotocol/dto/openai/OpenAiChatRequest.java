package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 符合 OpenAI 标准的 Chat Completions 请求体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiChatRequest {

    private String model;

    private List<OpenAiMessage> messages;

    private Double temperature;

    /** @deprecated 新模型（GPT-5.x 等）请使用 maxCompletionTokens */
    @JsonProperty("max_tokens")
    private Integer maxTokens;

    /** 新模型（GPT-4o/GPT-5.x 等）使用此字段替代 max_tokens */
    @JsonProperty("max_completion_tokens")
    private Integer maxCompletionTokens;

    private Boolean stream;

    @JsonProperty("stream_options")
    private StreamOptions streamOptions;

    // ===== 透传字段：跨协议零遗漏 =====

    /** 工具定义列表 [{type: function, function: {name, description, parameters}}] */
    private List<Object> tools;

    /** 工具选择策略：auto|none|required 或 {type, function:{name}} */
    @JsonProperty("tool_choice")
    private Object toolChoice;

    @JsonProperty("top_p")
    private Double topP;

    @JsonProperty("top_k")
    private Integer topK;

    /** 停止序列 */
    private List<String> stop;

    /** 未显式建模字段兜底收集（presence_penalty / frequency_penalty / logit_bias / user / seed 等） */
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

    /**
     * 流式响应附加选项 (OpenAI 专属特异性字段)
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StreamOptions {
        /** 是否在流式响应的末尾追加包含 token 计量的 chunk */
        @JsonProperty("include_usage")
        private Boolean includeUsage;
    }
}