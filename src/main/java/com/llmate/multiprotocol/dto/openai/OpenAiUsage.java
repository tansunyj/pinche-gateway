package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OpenAiUsage {
    @JsonProperty("prompt_tokens")
    private Integer promptTokens;

    @JsonProperty("completion_tokens")
    private Integer completionTokens;

    @JsonProperty("total_tokens")
    private Integer totalTokens;

    /** 推理/思考 tokens（DeepSeek: completion_tokens_details.reasoning_tokens） */
    @JsonProperty("reasoning_tokens")
    private Integer reasoningTokens;

    /** 缓存命中 tokens（DeepSeek: prompt_cache_hit_tokens） */
    @JsonProperty("prompt_cache_hit_tokens")
    private Integer cacheHitTokens;

    /** 缓存未命中 tokens（DeepSeek: prompt_cache_miss_tokens） */
    @JsonProperty("prompt_cache_miss_tokens")
    private Integer cacheMissTokens;

    /** 缓存创建 tokens（Anthropic: cache_creation_input_tokens） */
    @JsonProperty("cache_creation_input_tokens")
    private Integer cacheCreationTokens;

    /** 缓存读取 tokens（Anthropic: cache_read_input_tokens） */
    @JsonProperty("cache_read_input_tokens")
    private Integer cacheReadTokens;

    /** DeepSeek 嵌套结构：completion_tokens_details.reasoning_tokens */
    @JsonProperty("completion_tokens_details")
    private CompletionTokensDetails completionTokensDetails;

    /** DeepSeek 嵌套结构：prompt_tokens_details.cached_tokens */
    @JsonProperty("prompt_tokens_details")
    private PromptTokensDetails promptTokensDetails;

    /**
     * 获取 reasoningTokens（兼容直接字段和嵌套字段）
     */
    public Integer getReasoningTokens() {
        if (reasoningTokens != null) {
            return reasoningTokens;
        }
        if (completionTokensDetails != null && completionTokensDetails.getReasoningTokens() != null) {
            return completionTokensDetails.getReasoningTokens();
        }
        return null;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class CompletionTokensDetails {
        @JsonProperty("reasoning_tokens")
        private Integer reasoningTokens;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PromptTokensDetails {
        @JsonProperty("cached_tokens")
        private Integer cachedTokens;
    }
}