package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 网关内部标准 Token 计量数据
 *
 * 涵盖所有上游渠道可能返回的 usage 维度：
 * - OpenAI 标准：prompt_tokens, completion_tokens, total_tokens
 * - DeepSeek：reasoning_tokens (completion_tokens_details), prompt_cache_hit_tokens, prompt_cache_miss_tokens
 * - Anthropic：cache_creation_input_tokens, cache_read_input_tokens
 * - 通用扩展：cached_tokens (prompt_tokens_details.cached_tokens)
 *
 * 所有字段默认 0，不存在时为 0；计费系统按需取用。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmUsage {

    // ========== 基础维度（所有平台都有）==========
    /** 输入 Prompt 消耗的 Token 数 */
    @Builder.Default
    private int promptTokens = 0;

    /** 模型生成回复消耗的 Token 数 */
    @Builder.Default
    private int completionTokens = 0;

    /** 总消耗 Token 数 */
    @Builder.Default
    private int totalTokens = 0;

    // ========== 推理维度（DeepSeek / o1 等）==========
    /** 推理/思考 tokens（completion_tokens_details.reasoning_tokens） */
    @Builder.Default
    private int reasoningTokens = 0;

    // ========== 缓存维度（DeepSeek / Anthropic）==========
    /** 缓存命中 tokens（DeepSeek prompt_cache_hit_tokens） */
    @Builder.Default
    private int cacheHitTokens = 0;

    /** 缓存未命中 tokens（DeepSeek prompt_cache_miss_tokens） */
    @Builder.Default
    private int cacheMissTokens = 0;

    /** 缓存创建 tokens（Anthropic cache_creation_input_tokens） */
    @Builder.Default
    private int cacheCreationTokens = 0;

    /** 缓存读取 tokens（Anthropic cache_read_input_tokens） */
    @Builder.Default
    private int cacheReadTokens = 0;

    /** 缓存 tokens（通用/DeepSeek prompt_tokens_details.cached_tokens） */
    @Builder.Default
    private int cachedTokens = 0;
}
