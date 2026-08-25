package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.dto.BillingParams;
import com.llmate.multiprotocol.dto.BillingResult;
import com.llmate.multiprotocol.dto.UsageData;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * BillingCalculator「仅同侧兜底」计费规则测试（用户确认规则，见 resolveTokenPrice javadoc）：
 * - token 维度价有效(>0) → 按维度价；
 * - 维度价无效(0/null/负) → 兜底同侧主价（输入侧 inputPer1m / 输出侧 outputPer1m）；
 * - 维度价与同侧主价都无效且有用量 → 抛 PRICE_NOT_CONFIGURED（拒绝计费，避免静默漏收）；
 * - 推理未配价时并入输出按输出主价计费（不独立兜底，避免双算）。
 *
 * 额度换算：quota = round(费用元 × 100000)。
 */
class BillingCalculatorTest {

    private final BillingCalculator calc = new BillingCalculator();

    private static BillingParams params() {
        return new BillingParams();
    }

    private long quotaOf(BillingParams bp, UsageData usage, String mode) {
        BillingResult r = calc.calculateCost(mode, usage, bp, "USD").block();
        assertThat(r).isNotNull();
        return r.getQuota();
    }

    // ==================== TOKEN ====================

    @Test
    void cacheHitDiscountsWhenCachePriced() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setOutputPer1m(new BigDecimal("20"));
        bp.setCacheHitPer1m(new BigDecimal("2"));
        UsageData usage = UsageData.builder().inputTokens(10).outputTokens(0)
            .cacheMissTokens(7).cachedInputTokens(3).build();
        // (7×10 + 3×2)/1M = 0.000076 元 → quota = round(7.6) = 8
        assertThat(quotaOf(bp, usage, "token")).isEqualTo(8);
    }

    @Test
    void cachePriceZeroFallsBackToInputMain() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setOutputPer1m(new BigDecimal("20"));
        bp.setCacheHitPer1m(new BigDecimal("0"));
        UsageData usage = UsageData.builder().inputTokens(10).outputTokens(0)
            .cacheMissTokens(7).cachedInputTokens(3).build();
        // 缓存价 0 → 兜底同侧输入主价 10：整体输入 10×10/1M = 0.0001 → quota = 10
        assertThat(quotaOf(bp, usage, "token")).isEqualTo(10);
    }

    @Test
    void unpricedReasoningMergesIntoOutput() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setOutputPer1m(new BigDecimal("20"));
        bp.setReasoningPer1m(new BigDecimal("0"));
        UsageData usage = UsageData.builder().inputTokens(10).outputTokens(50)
            .cacheMissTokens(10).reasoningTokens(5).build();
        // 推理未配价并入输出：输入 10×10 + 输出 50×20 = 1100/1M → quota = 110（推理不双算）
        assertThat(quotaOf(bp, usage, "token")).isEqualTo(110);
    }

    @Test
    void inputMainMissingWithInputUsageRejects() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("0"));
        bp.setOutputPer1m(new BigDecimal("20"));
        UsageData usage = UsageData.builder().inputTokens(10).outputTokens(5).cacheMissTokens(10).build();
        assertThatThrownBy(() -> quotaOf(bp, usage, "token"))
            .isInstanceOf(LlmGatewayException.class)
            .satisfies(e -> assertThat(((LlmGatewayException) e).getErrorCode()).isEqualTo(LlmErrorCode.PRICE_NOT_CONFIGURED));
    }

    @Test
    void outputMainMissingWithOutputUsageRejects() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setOutputPer1m(new BigDecimal("0"));
        UsageData usage = UsageData.builder().inputTokens(10).outputTokens(5).cacheMissTokens(10).build();
        assertThatThrownBy(() -> quotaOf(bp, usage, "token"))
            .isInstanceOf(LlmGatewayException.class)
            .satisfies(e -> assertThat(((LlmGatewayException) e).getErrorCode()).isEqualTo(LlmErrorCode.PRICE_NOT_CONFIGURED));
    }

    @Test
    void zeroPricesButNoUsageDoesNotThrow() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("0"));
        bp.setOutputPer1m(new BigDecimal("0"));
        UsageData usage = UsageData.builder().inputTokens(0).outputTokens(0).build();
        assertThat(quotaOf(bp, usage, "token")).isZero();
    }

    // ==================== IMAGE_TOKEN ====================

    @Test
    void imageTokenMissingInputImagePriceFallsBackToInputMain() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setOutputPer1m(new BigDecimal("30"));
        bp.setInputTextPer1m(new BigDecimal("10"));
        bp.setOutputTextPer1m(new BigDecimal("30"));
        bp.setInputImagePer1m(new BigDecimal("0"));   // 未配 → 兜底输入主价 10
        bp.setOutputImagePer1m(new BigDecimal("0"));  // 无用量，不触发
        UsageData usage = UsageData.builder()
            .inputTextTokens(5).inputImageTokens(100).outputTextTokens(3).outputImageTokens(0).build();
        // 5×10 + 100×10(兜底) + 3×30 = 1140/1M = 0.00114 → quota = 114
        assertThat(quotaOf(bp, usage, "image_token")).isEqualTo(114);
    }

    // ==================== EMBEDDING ====================

    @Test
    void embeddingMissingVectorPriceFallsBackToInputMain() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setVectorTokensPer1m(new BigDecimal("0"));  // 未配 → 兜底输入主价 10
        UsageData usage = UsageData.builder().vectorTokens(50).build();
        // 50×10/1M = 0.0005 → quota = 50
        assertThat(quotaOf(bp, usage, "embedding")).isEqualTo(50);
    }

    // ==================== VIDEO_TOKEN ====================

    @Test
    void videoTokenMissingResolutionPriceFallsBackToOutputMain() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("10"));
        bp.setOutputPer1m(new BigDecimal("20"));
        UsageData usage = UsageData.builder().outputTokens(50).resolution("1080p").hasInputImage(false).build();
        // 档位价未配 → 兜底输出主价 20：50×20/1M = 0.001 → quota = 100
        assertThat(quotaOf(bp, usage, "video_token")).isEqualTo(100);
    }

    @Test
    void videoTokenAllZeroPricesWithUsageRejects() {
        BillingParams bp = params();
        bp.setInputPer1m(new BigDecimal("0"));
        bp.setOutputPer1m(new BigDecimal("0"));
        UsageData usage = UsageData.builder().outputTokens(50).resolution("1080p").hasInputImage(false).build();
        assertThatThrownBy(() -> quotaOf(bp, usage, "video_token"))
            .isInstanceOf(LlmGatewayException.class)
            .satisfies(e -> assertThat(((LlmGatewayException) e).getErrorCode()).isEqualTo(LlmErrorCode.PRICE_NOT_CONFIGURED));
    }
}
