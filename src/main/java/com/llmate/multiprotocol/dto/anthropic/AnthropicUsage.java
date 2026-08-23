package com.llmate.multiprotocol.dto.anthropic;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 符合 Anthropic 规范的 Token 计量统计数据 DTO
 * 对应 /v1/messages 响应中的 "usage": { "input_tokens": 10, "output_tokens": 20 }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnthropicUsage {

    /**
     * 输入提示词消耗的 Token 数 (对应 OpenAI 的 prompt_tokens)
     */
    @JsonProperty("input_tokens")
    private int inputTokens;

    /**
     * 模型生成回复消耗的 Token 数 (对应 OpenAI 的 completion_tokens)
     */
    @JsonProperty("output_tokens")
    private int outputTokens;

    /**
     * 缓存创建消耗的 Token 数（Anthropic 特有可选字段，用于 Prompt 缓存计费）
     */
    @JsonProperty("cache_creation_input_tokens")
    private Integer cacheCreationInputTokens;

    /**
     * 命中的缓存 Token 数（Anthropic 特有可选字段）
     */
    @JsonProperty("cache_read_input_tokens")
    private Integer cacheReadInputTokens;

    /**
     * 快捷构造器：只传入核心输入输出 Token
     */
    public AnthropicUsage(int inputTokens, int outputTokens) {
        this.inputTokens = inputTokens;
        this.outputTokens = outputTokens;
    }
}