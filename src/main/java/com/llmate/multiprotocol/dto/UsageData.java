package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 统一 Token 使用统计（跨平台标准化）
 *
 * 设计原则：
 * 1. 所有维度都有默认值 0，不存在时为 0
 * 2. 计费时如果值为 0 或对应价格为 0/空，则该维度不参与计费
 * 3. 各平台的原始 usage 数据通过 Converter 转换为这个统一结构
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UsageData {

    // ========== 基础维度（所有平台都有）==========
    private long inputTokens;

    private long outputTokens;

    private long totalTokens;

    // ========== 扩展维度（部分平台支持）==========
    /** 推理/思考 tokens（DeepSeek/Gemini 支持） */
    private long reasoningTokens;

    /** 纯文本 tokens（OpenAI 支持） */
    private long textTokens;

    /** 缓存 tokens（DeepSeek 支持） */
    private long cachedTokens;

    /** 缓存命中 tokens（DeepSeek 支持） */
    private long cacheHitTokens;

    /** 缓存未命中 tokens（DeepSeek 支持） */
    private long cacheMissTokens;

    /** 缓存创建 tokens（Anthropic 支持） */
    private long cacheCreationTokens;

    /** 缓存读取 tokens（Anthropic 支持） */
    private long cacheReadTokens;

    // ========== 对话上下文拆分计费维度 ==========
    /**
     * 历史消息（已缓存）的 input tokens。
     * 计费规则：最后一条 message 按 inputPer1m 价格计费，
     * 历史 messages 按 cacheHitPer1m 价格计费。
     * 若上游未拆分，则全部视为新 input（cachedInputTokens=0）。
     */
    private long cachedInputTokens;

    // ========== 多模态维度（Gemini 支持）==========
    /** 输入 tokens 按模态分类: {"TEXT": 100, "IMAGE": 50} */
    private Map<String, Long> promptTokensByModality;

    /** 输出 tokens 按模态分类 */
    private Map<String, Long> completionTokensByModality;

    // ========== 多模态计费维度（非文本模型）==========
    private int imageCount;

    private long inputTextTokens;

    private long inputImageTokens;

    private long outputTextTokens;

    private long outputImageTokens;

    private long videoSeconds;

    /** ASR 语音转写音频时长（秒），asr 计费模式使用 */
    private long audioSeconds;

    private boolean is1080p;

    private String resolution;

    private boolean hasInputImage;

    private long textTokensEmbedding;

    private long imageTokensEmbedding;

    private long vectorTokens;

    private long characterCount;

    /**
     * 获取实际用于计费的 input tokens
     */
    public long getEffectiveInputTokens() {
        return inputTokens;
    }

    /**
     * 获取实际用于计费的 output tokens
     * 注意：reasoning_tokens 是 completion_tokens 的子集
     */
    public long getEffectiveOutputTokens() {
        if (reasoningTokens > 0) {
            // 如果 reasoning 单独计费，则 output 中不包含 reasoning
            return Math.max(0, outputTokens - reasoningTokens);
        }
        return outputTokens;
    }

    // ========== Builder 模式默认值 ==========
    public static class UsageDataBuilder {
        private long inputTokens = 0;
        private long outputTokens = 0;
        private long totalTokens = 0;
        private long reasoningTokens = 0;
        private long textTokens = 0;
        private long cachedTokens = 0;
        private long cacheHitTokens = 0;
        private long cacheMissTokens = 0;
        private long cacheCreationTokens = 0;
        private long cacheReadTokens = 0;
        private long cachedInputTokens = 0;
        private int imageCount = 0;
        private long inputTextTokens = 0;
        private long inputImageTokens = 0;
        private long outputTextTokens = 0;
        private long outputImageTokens = 0;
        private long videoSeconds = 0;
        private long audioSeconds = 0;
        private boolean is1080p = false;
        private boolean hasInputImage = false;
        private long textTokensEmbedding = 0;
        private long imageTokensEmbedding = 0;
        private long vectorTokens = 0;
        private long characterCount = 0;
    }
}
