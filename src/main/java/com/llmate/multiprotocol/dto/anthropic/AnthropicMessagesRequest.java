package com.llmate.multiprotocol.dto.anthropic;

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
 * 符合 Anthropic 规范的 /v1/messages 请求体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AnthropicMessagesRequest {

    private String model;

    /** Anthropic 将 system 消息作为顶层字段单独抽离，支持 String 或 List（结构化内容块）两种格式 */
    private Object system;

    private List<AnthropicMessage> messages;

    @JsonProperty("max_tokens")
    private Integer maxTokens;

    private Double temperature;

    private Boolean stream;

    /** 工具定义列表（Claude Code / Agent SDK 会携带大量工具） */
    private List<AnthropicTool> tools;

    /** 工具选择策略：{type: auto|any|tool, name} */
    @JsonProperty("tool_choice")
    private Object toolChoice;

    /** 思考/推理配置：{type: adaptive|enabled, budget_tokens} */
    private Object thinking;

    @JsonProperty("top_p")
    private Double topP;

    @JsonProperty("top_k")
    private Integer topK;

    @JsonProperty("stop_sequences")
    private List<String> stopSequences;

    /** 请求元数据（Claude Code 携带 device_id / session_id 等） */
    private Object metadata;

    /** 输出配置（Claude Code 携带 effort 等） */
    @JsonProperty("output_config")
    private Object outputConfig;

    /**
     * 未显式建模的其余字段（cache_control、以及未来 Anthropic 新增字段）收集到这里，
     * 供上游转换器以 @JsonAnyGetter 原样合并回目标渠道请求体，保证零遗漏。
     */
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
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnthropicMessage {
        private String role; // user, assistant
        private Object content; // 支持 String 文本或 List 多模态结构
    }
}