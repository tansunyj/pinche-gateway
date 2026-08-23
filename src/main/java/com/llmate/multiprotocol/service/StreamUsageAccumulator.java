package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.UsageData;

/**
 * 流式使用统计累加器
 *
 * 在流式响应过程中累计 chunk 数量与 token 用量，
 * 流结束后转换为 BillingService 结算所需的 UsageData。
 * 支持多维度 token 统计：input/output/reasoning/cache 等。
 */
public class StreamUsageAccumulator {

    private long inputTokens = 0;
    private long outputTokens = 0;
    private long totalTokens = 0;
    private long reasoningTokens = 0;
    private long cacheHitTokens = 0;
    private long cacheMissTokens = 0;
    private long cacheCreationTokens = 0;
    private long cacheReadTokens = 0;
    private long cachedTokens = 0;
    private int chunkCount = 0;

    public void accumulate(LlmStreamChunk chunk) {
        chunkCount++;
        // 流式 chunk 通常不包含完整 usage，需要在最后估算或从最后一个 chunk 获取
        if (chunk.getUsage() != null) {
            inputTokens = chunk.getUsage().getPromptTokens();
            outputTokens = chunk.getUsage().getCompletionTokens();
            totalTokens = chunk.getUsage().getTotalTokens();
            reasoningTokens = chunk.getUsage().getReasoningTokens();
            cacheHitTokens = chunk.getUsage().getCacheHitTokens();
            cacheMissTokens = chunk.getUsage().getCacheMissTokens();
            cacheCreationTokens = chunk.getUsage().getCacheCreationTokens();
            cacheReadTokens = chunk.getUsage().getCacheReadTokens();
            cachedTokens = chunk.getUsage().getCachedTokens();
        }
    }

    public UsageData toUsageData() {
        return UsageData.builder()
            .inputTokens(inputTokens)
            .outputTokens(outputTokens)
            .totalTokens(totalTokens > 0 ? totalTokens : inputTokens + outputTokens)
            .reasoningTokens(reasoningTokens)
            .cacheHitTokens(cacheHitTokens)
            .cacheMissTokens(cacheMissTokens)
            .cacheCreationTokens(cacheCreationTokens)
            .cacheReadTokens(cacheReadTokens)
            .cachedTokens(cachedTokens)
            // 关键：BillingCalculator.calcInputTokens 用 cachedInputTokens 做「新输入 vs 缓存输入」拆分计费，
            // 而流式累加器存的是 cachedTokens（OpenAI/Azure prompt_tokens_details.cached_tokens）。
            // 之前漏了这一步 → 缓存命中 tokens 计费恒为 0，全部 input 按全价计费（缓存命中白省）。
            // 三源合并（覆盖所有上游缓存字段口径）：
            //   ① cachedTokens     —— OpenAI/Azure/Gemini 新格式 prompt_tokens_details.cached_tokens / cached_content_token_count
            //   ② cacheReadTokens  —— Anthropic 风格 cache_read_input_tokens
            //   ③ cacheHitTokens   —— 旧格式 prompt_cache_hit_tokens（DeepSeek 等）
            // 任一非 0 即激活拆分计费。
            .cachedInputTokens(cachedTokens > 0 ? cachedTokens
                    : (cacheReadTokens > 0 ? cacheReadTokens : cacheHitTokens))
            .build();
    }

    public int getChunkCount() {
        return chunkCount;
    }
}
