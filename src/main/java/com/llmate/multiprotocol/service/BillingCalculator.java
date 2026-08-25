package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.constant.BusinessConstants;
import com.llmate.multiprotocol.dto.BillingParams;
import com.llmate.multiprotocol.dto.BillingResult;
import com.llmate.multiprotocol.dto.UsageData;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.util.LogBox;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * 计费计算器
 * 支持 9 种计费模式：
 * 1. TOKEN - 按 token 计费
 * 2. IMAGE - 按图片数量计费
 * 3. IMAGE_TOKEN - 按图文 token 计费
 * 4. VIDEO_SECOND - 按视频时长计费
 * 5. VIDEO_TOKEN - 按视频 token 计费
 * 6. TTS - 按字符数计费
 * 7. EMBEDDING - 按向量 token 计费
 * 8. FLAT - 按次计费
 * 9. ASR - 按音频时长（秒）计费
 *
 * 计费规则：
 * - 内部统一按元计算
 * - 额度 = 金额 × QUOTA_PER_USD
 * - 显示货币仅用于前端展示，不影响实际计费
 * - 套餐折扣：最终费用 = 基础费用 × 套餐 price_markup
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class BillingCalculator {

    /**
     * TOKEN 计费模式中间结果：未缩放总账 + 各分项乘积（供总账日志显式列出相加项）
     */
    private record TokenCost(BigDecimal unscaled, List<BigDecimal> components) {}

    /**
     * 计算费用（无套餐折扣版本，向后兼容）
     */
    public Mono<BillingResult> calculateCost(
            String billingMode,
            UsageData usage,
            BillingParams params,
            String displayCurrency) {
        return calculateCost(billingMode, usage, params, displayCurrency, null, null, null);
    }

    /**
     * 计算费用（支持套餐折扣）
     *
     * 计费原则（token 类维度支持「仅同侧兜底」，用户确认规则）：
     * - 维度价有效（>0）→ 按维度价计费；
     * - 维度价无效（0/null/负）→ 兜底同侧主价：输入侧回落 inputPer1m，输出侧回落 outputPer1m，
     *   （缓存命中维度、图文/向量/视频token 子维度均适用），避免「有使用量却按 0 计费」的漏收；
     * - 维度价与同侧主价都无效且该维度有用量 → 抛 PRICE_NOT_CONFIGURED 拒绝计费（不做静默免费）；
     * - 非 token 维度（按次/按秒/按字符）维持原规则：未配置价格（为0/null/负）该维度费用为0。
     * 计算过程（各维度用量×单价+折扣）通过 LogBox.logBillingDetail 完整打印。
     *
     * @param billingMode 计费模式
     * @param usage 使用量
     * @param params 计费参数
     * @param displayCurrency 显示货币（仅用于显示，不参与计费计算）
     * @param requestId 请求ID（用于日志）
     * @param userId 用户ID（用于日志）
     * @param modelId 模型ID（用于日志）
     * @return 计费结果（内部统一按元计算）
     */
    public Mono<BillingResult> calculateCost(
            String billingMode,
            UsageData usage,
            BillingParams params,
            String displayCurrency,
            String requestId,
            Long userId,
            String modelId) {

        // 逐维度计算明细（每次调用独立 List，无并发共享状态）
        List<String> detailLines = new ArrayList<>();
        detailLines.add("计费模式: " + billingMode);

        return Mono.fromCallable(() -> calculateCostInUsd(billingMode, usage, params, detailLines))
            .map(baseCost -> {
                // 计费前统一打印"提取到的用量"：所有请求类型（文本聊天/生图/生视频/向量/TTS）在计算费用前
                // 都输出本块，保证日志标准一致（上游渠道响应 → 用量提取 → 计费明细 → 计费计算）。
                LogBox.logUsageExtraction(requestId, userId, modelId, usage);

                // 应用套餐折扣（priceMarkup，来自用户 API Key 的套餐配置）
                BigDecimal packageMarkup = params.getPriceMarkup() != null ? params.getPriceMarkup() : BigDecimal.ONE;
                BigDecimal finalCost = baseCost.multiply(packageMarkup);

                // 始终打印 API Key 折扣计算：基础费用 × priceMarkup = 最终费用。
                // 不管是否有折扣（>0 都有这一行），文本聊天/生图/生视频/向量/TTS 统一。
                detailLines.add("费用计算: " + baseCost.stripTrailingZeros().toPlainString()
                    + " 元 × priceMarkup(" + packageMarkup.stripTrailingZeros().toPlainString()
                    + ") = " + finalCost.stripTrailingZeros().toPlainString() + " 元");

                // 计算额度（直接使用金额 × 每元额度）
                BigDecimal quotaDecimal = finalCost.multiply(BusinessConstants.QUOTA_PER_USD);
                long quota = quotaDecimal.setScale(0, BusinessConstants.QUOTA_ROUNDING_MODE).longValue();
                detailLines.add("换算额度: " + finalCost.stripTrailingZeros().toPlainString()
                    + " 元 × " + BusinessConstants.QUOTA_PER_USD.toPlainString() + " = " + quota);

                // 计费多行明细（tokens 消耗 + 各维度费用，\n 拼接）→ 落库到合并日志表 billing_detail 字段
                String billingDetail = String.join("\n", detailLines);

                // 构建结果
                BillingResult result = BillingResult.builder()
                    .costInUsd(finalCost.setScale(BusinessConstants.COST_SCALE, BusinessConstants.COST_ROUNDING_MODE))
                    .quota(quota)
                    .currency(displayCurrency)
                    .promptTokens(usage.getInputTokens())
                    .completionTokens(usage.getOutputTokens())
                    // 车次折扣归属：从 BillingParams 复制(applyRideDiscount 已填充)，供统计/结算落库
                    .rideIds(params.getRideIds())
                    .effectiveRideId(params.getEffectiveRideId())
                    .effectiveRideName(params.getEffectiveRideName())
                    .reasoningTokens(usage.getReasoningTokens())
                    .cacheHitTokens(usage.getCacheHitTokens())
                    .cacheCreationTokens(usage.getCacheCreationTokens())
                    .cacheReadTokens(usage.getCacheReadTokens())
                    .packageMarkup(packageMarkup)
                    .billingDetail(billingDetail)
                    .build();

                // 输出带方框的计费明细
                LogBox.logBillingDetail(billingMode, requestId, userId, modelId, params, detailLines);

                // 输出带方框的计费日志
                LogBox.logBillingCalculation(billingMode, requestId, userId, modelId, params, result, quota);

                if (packageMarkup.compareTo(BigDecimal.ONE) < 0) {
                    log.info("[BillingCalculator] 套餐折扣已应用: userId={}, modelId={}, " +
                             "baseCost={}, markup={}, finalCost={}",
                        userId, modelId, baseCost, packageMarkup, finalCost);
                }

                return result;
            });
    }

    /**
     * 按元计算费用（内部计算统一用元），并逐维度记录计算明细
     */
    private BigDecimal calculateCostInUsd(String billingMode, UsageData usage, BillingParams params, List<String> details) {
        BigDecimal cost = BigDecimal.ZERO;

        switch (billingMode.toLowerCase()) {
            case BusinessConstants.BILLING_MODE_TOKEN -> {
        TokenCost tokenCost = calculateTokenCost(usage, params, details);
        cost = cost.add(finishTokenTotal("TOKEN", tokenCost.unscaled(), tokenCost.components(), details));
            }
            case BusinessConstants.BILLING_MODE_IMAGE -> {
                if (usage.getImageCount() > 0) {
                    if (isValidPrice(params.getImagePerCall())) {
                        BigDecimal dimCost = params.getImagePerCall()
                            .multiply(BigDecimal.valueOf(usage.getImageCount()));
                        details.add("图片: 数量=" + usage.getImageCount() + " × 单价=" + params.getImagePerCall()
                            + "/次 = " + dimCost.stripTrailingZeros().toPlainString() + " 元");
                        cost = cost.add(dimCost);
                    } else {
                        details.add("图片: 数量=" + usage.getImageCount() + "，未配置价格(=" + params.getImagePerCall() + ")，费用=0");
                    }
                } else {
                    details.add("图片: 无用量，费用=0");
                }
            }
            case BusinessConstants.BILLING_MODE_IMAGE_TOKEN -> {
                // 先累加未缩放总账，最后统一 ÷1M
                cost = cost.add(finishTokenTotal("IMAGE_TOKEN", calculateImageTokenCost(usage, params, details), null, details));
            }
            case BusinessConstants.BILLING_MODE_VIDEO_SECOND -> {
                if (usage.getVideoSeconds() > 0) {
                    BigDecimal price = usage.is1080p()
                        ? params.getVideoPerSecond1080p()
                        : params.getVideoPerSecond720p();
                    if (isValidPrice(price)) {
                        BigDecimal dimCost = price.multiply(BigDecimal.valueOf(usage.getVideoSeconds()));
                        details.add("视频(" + (usage.is1080p() ? "1080p" : "720p") + "): 时长=" + usage.getVideoSeconds()
                            + "s × 单价=" + price + "/s = " + dimCost.stripTrailingZeros().toPlainString() + " 元");
                        cost = cost.add(dimCost);
                    } else {
                        details.add("视频: 时长=" + usage.getVideoSeconds() + "s，未配置价格(=" + price + ")，费用=0");
                    }
                } else {
                    details.add("视频: 无时长用量，费用=0");
                }
            }
            case BusinessConstants.BILLING_MODE_ASR -> {
                // 完全镜像 VIDEO_SECOND：audioPerSecond × audioSeconds，价格 null/0/负不计费
                if (usage.getAudioSeconds() > 0) {
                    if (isValidPrice(params.getAudioPerSecond())) {
                        BigDecimal dimCost = params.getAudioPerSecond().multiply(BigDecimal.valueOf(usage.getAudioSeconds()));
                        details.add("ASR音频: 时长=" + usage.getAudioSeconds()
                            + "s × 单价=" + params.getAudioPerSecond() + "/s = " + dimCost.stripTrailingZeros().toPlainString() + " 元");
                        cost = cost.add(dimCost);
                    } else {
                        details.add("ASR音频: 时长=" + usage.getAudioSeconds() + "s，未配置价格(=" + params.getAudioPerSecond() + ")，费用=0");
                    }
                } else {
                    details.add("ASR音频: 无时长用量，费用=0");
                }
            }
            case BusinessConstants.BILLING_MODE_VIDEO_TOKEN -> {
                String key = usage.getResolution() + "_" + (usage.isHasInputImage() ? "withInput" : "noInput");
                BigDecimal price = params.getVideoTokenPrice(key);
                long tokens = usage.getOutputTokens();
                if (tokens <= 0) {
                    details.add("视频token(" + key + "): 无用量，费用=0");
                } else {
                    // 视频 token 属输出侧维度：未配置该档位价时「仅同侧兜底」到输出主价 outputPer1m；
                    // 档位价与输出主价都无效且有用量 → resolveTokenPrice 抛 PRICE_NOT_CONFIGURED 拒绝计费（避免漏收）
                    BigDecimal resolved = resolveTokenPrice(price, false, params, tokens, "视频token(" + key + ")", details);
                    // 费用 = completion_tokens × 单价(元/百万token) ÷ 1M
                    BigDecimal dimCost = BigDecimal.valueOf(tokens).multiply(resolved)
                        .divide(BusinessConstants.TOKEN_PER_1M, BusinessConstants.COST_SCALE, BusinessConstants.COST_ROUNDING_MODE);
                    details.add("视频token(" + key + "): tokens=" + tokens + " × 单价=" + resolved
                        + "/1M ÷ 1M = " + dimCost.stripTrailingZeros().toPlainString() + " 元");
                    cost = cost.add(dimCost);
                }
            }
            case BusinessConstants.BILLING_MODE_EMBEDDING -> {
                // 先累加未缩放总账，最后统一 ÷1M
                cost = cost.add(finishTokenTotal("EMBEDDING", calculateEmbeddingCost(usage, params, details), null, details));
            }
            case BusinessConstants.BILLING_MODE_TTS -> {
                if (usage.getCharacterCount() > 0) {
                    if (isValidPrice(params.getCharactersPer1k())) {
                        BigDecimal dimCost = BigDecimal.valueOf(usage.getCharacterCount())
                            .multiply(params.getCharactersPer1k())
                            .divide(BusinessConstants.CHARS_PER_1K, BusinessConstants.COST_SCALE, BusinessConstants.COST_ROUNDING_MODE);
                        details.add("TTS字符: 字符数=" + usage.getCharacterCount() + " × 单价=" + params.getCharactersPer1k()
                            + "/1k = " + dimCost.stripTrailingZeros().toPlainString() + " 元");
                        cost = cost.add(dimCost);
                    } else {
                        details.add("TTS字符: 字符数=" + usage.getCharacterCount() + "，未配置价格(=" + params.getCharactersPer1k() + ")，费用=0");
                    }
                } else {
                    details.add("TTS字符: 无用量，费用=0");
                }
            }
            case BusinessConstants.BILLING_MODE_FLAT -> {
                if (isValidPrice(params.getFlatPrice())) {
                    details.add("按次: 单价=" + params.getFlatPrice() + "，费用=" + params.getFlatPrice().stripTrailingZeros().toPlainString() + " 元");
                    cost = cost.add(params.getFlatPrice());
                } else {
                    details.add("按次: 未配置价格(=" + params.getFlatPrice() + ")，费用=0");
                }
            }
            default -> throw new IllegalArgumentException("Unsupported billing mode: " + billingMode);
        }

        return cost;
    }

    /**
     * 计算 Token 费用（逐维度：有价格才计费，无价格费用为0）
     *
     * 推理 tokens 计费规则：
     * - 若配置了 reasoningPer1m（>0）：输出 tokens 不含推理，推理单独按 reasoningPer1m 计费；
     * - 若未配置 reasoningPer1m（null 或 ≤0）：推理 tokens 合并到输出 tokens 中按 outputPer1m 计费。
     *
     * 输入 tokens 拆分计费规则：
     * - 若配置了 cacheHitPer1m（>0）：最后一条 message 按 inputPer1m 计费，
     *   历史 messages 按 cacheHitPer1m 计费；
     * - 若未配置 cacheHitPer1m（null 或 ≤0）：整个 input 按 inputPer1m 计费，不拆分。
     */
    private TokenCost calculateTokenCost(UsageData usage, BillingParams params, List<String> details) {
        BigDecimal cost = BigDecimal.ZERO;
        // 各 token 维度未缩放乘积（用于总账行显式列出相加项，顺序与明细行一致）
        List<BigDecimal> components = new ArrayList<>();

        // 1. 输入 tokens（支持拆分计费：新消息 + 历史缓存 两个分项）
        cost = cost.add(calcInputTokens(usage, params, details, components));

        // 2. 输出 tokens（含/不含推理，取决于 reasoningPer1m 是否配置）
        //    输出侧维度：未配置输出主价时「仅同侧兜底」无源 → 有用量则抛 PRICE_NOT_CONFIGURED（用户确认规则）
        boolean reasoningPriced = isValidPrice(params.getReasoningPer1m());
        long outputTokens = reasoningPriced ? usage.getEffectiveOutputTokens() : usage.getOutputTokens();
        BigDecimal outputPrice = resolveTokenPrice(
            params.getOutputPer1m(), false, params, outputTokens, "输出tokens", details);
        BigDecimal outputCost = calcTokenDimension("输出tokens",
            outputTokens, outputPrice, details);
        cost = cost.add(outputCost);
        components.add(outputCost);

        // 3. 推理 tokens：与缓存维度一致，无论是否配置价格都输出该维度行（价格/用量为 0 时费用为 0），
        //    便于核对推理计费；配置了 reasoningPer1m 时单独按思考价计费，未配置则并入输出价
        BigDecimal reasoningCost = calcTokenDimension("推理tokens",
            usage.getReasoningTokens(), params.getReasoningPer1m(), details);
        cost = cost.add(reasoningCost);
        components.add(reasoningCost);

        // 4. 缓存命中不重复计：命中输入已并入 step 1「输入tokens(缓存命中)」拆分计费
        //    （calcInputTokens 恒拆分，明细行已在上方打印），不再输出独立说明行；
        //    缓存创建/缓存读取同样不再独立计费（用户确认移除）：
        //    - 缓存创建 tokens 就是纯新输入，已含在输入tokens里按 inputPer1m 计费；
        //    - 缓存读取 tokens 就是缓存输入，已含在缓存输入tokens里按 cacheHitPer1m
        //      （缓存价=0 时按 inputPer1m）计费；
        //    此前若 DB 配置了 cache_creation_per_1m / cache_read_per_1m 会与拆分计费双算，
        //    移除后只保留 inputPer1m / cacheHitPer1m 两价模型。

        return new TokenCost(cost, components);
    }

    /**
     * 输入 tokens 计费（用户确认规则）：输入费用 = 缓存未命中费用 + 缓存命中费用，二者组合构成输入。
     *
     * 始终拆分为「缓存未命中 / 缓存命中」两个维度展示并计费（不再有整体「输入tokens」收费维度）：
     * - 未命中：维度价=输入主价 inputPer1m，无效时「仅同侧兜底」无源 → 有用量则拒绝（PRICE_NOT_CONFIGURED）；
     * - 命中：维度价=cacheHitPer1m，未配置时兜底同侧主价 inputPer1m（即命中按全价输入价计，
     *   与用户规则「命中价/未命中价有一方为 0 → 按总体输入 tokens 算」一致）；若 inputPer1m 也无效
     *   但命中价有效，直接按命中价计（修复边角：只配 cache_hit 时命中不再被静默计 0）。
     * 拆分行之前先打印总输入引用行「输入tokens: X (缓存命中 A + 缓存未命中 B)」，与用量提取口径一致。
     * 命中输入取 cachedInputTokens（cachedTokens / cacheReadTokens / cacheHitTokens 三源合并），
     * 未命中取 cacheMissTokens，缺失时用「输入 - 命中」兜底补齐。
     */
    private BigDecimal calcInputTokens(UsageData usage, BillingParams params, List<String> details, List<BigDecimal> components) {
        long totalInput = usage.getInputTokens();
        long hit = usage.getCachedInputTokens();
        long miss = usage.getCacheMissTokens();
        if (miss <= 0 && totalInput > 0) {
            miss = Math.max(0, totalInput - hit);
        }
        if (hit <= 0 && miss > 0) {
            hit = Math.max(0, totalInput - miss);
        }

        // 输入tokens 总引用行：输入 = 缓存命中 + 缓存未命中，二者组合构成输入费用（用户规则）
        details.add("输入tokens: " + totalInput + " (缓存命中 " + hit + " + 缓存未命中 " + miss + ")");
        // 未命中/命中两个维度均走「仅同侧兜底」：维度价无效 → 输入主价 inputPer1m；都无效且有用量 → 拒绝
        BigDecimal missPrice = resolveTokenPrice(
            params.getInputPer1m(), true, params, miss, "输入tokens(缓存未命中)", details);
        BigDecimal hitPrice = resolveTokenPrice(
            params.getCacheHitPer1m(), true, params, hit, "输入tokens(缓存命中)", details);
        BigDecimal missProduct = calcTokenDimension("输入tokens(缓存未命中)", miss, missPrice, details);
        BigDecimal hitProduct = calcTokenDimension("输入tokens(缓存命中)", hit, hitPrice, details);
        components.add(missProduct);
        components.add(hitProduct);
        return missProduct.add(hitProduct);
    }

    /**
     * 单个 token 维度计费：用量>0 且配置了价格(>0)才计费；未配置价格则费用为0。
     * 无论是否计费，都把该维度配置的单价打印出来（未配置则打印"未配置"），便于核对价格。
     */
    private BigDecimal calcTokenDimension(String name, long tokens, BigDecimal pricePer1m, List<String> details) {
        boolean priced = isValidPrice(pricePer1m);
        String priceText = priced
            ? pricePer1m.stripTrailingZeros().toPlainString() + "/1M"
            : (pricePer1m == null ? "null" : pricePer1m.stripTrailingZeros().toPlainString());
        if (tokens <= 0) {
            details.add(name + ": 用量=" + tokens + "，单价=" + priceText + "(无用量)，费用=0");
            return BigDecimal.ZERO;
        }
        if (!priced) {
            details.add(name + ": 用量=" + tokens + "，单价=未配置(" + priceText + ")，费用=0");
            return BigDecimal.ZERO;
        }
        // 只乘不除：返回 用量×单价 的未缩放乘积，由外层把所有 token 维度累加后
        // 最后统一 ÷1M，避免逐维度除法各自舍入再累加产生累计误差。
        BigDecimal dimProduct = BigDecimal.valueOf(tokens).multiply(pricePer1m);
        details.add(name + ": 用量=" + tokens + " × 单价=" + priceText
            + " = " + dimProduct.stripTrailingZeros().toPlainString() + " (未缩放，最后统一÷1M)");
        return dimProduct;
    }

    /**
     * token 维度总账收尾：把累加好的未缩放乘积【最后】统一 ÷1M，只做一次除法，
     * 避免每个维度各自除以 1M 的舍入误差累加放大。
     *
     * @param label    计费模式名（用于日志标识）
     * @param unscaled 该模式下各 token 维度未缩放乘积之和（Σ 用量×单价）
     * @param details  计费明细行集合
     * @return 除以 1M 后的费用（元）
     */
    private BigDecimal finishTokenTotal(String label, BigDecimal unscaled, List<BigDecimal> components, List<String> details) {
        BigDecimal totalCost = unscaled.divide(
            BusinessConstants.TOKEN_PER_1M,
            BusinessConstants.COST_SCALE,
            BusinessConstants.COST_ROUNDING_MODE);
        // 总账行显式列出相加的分项（未缩放乘积），便于核对总账由哪几项构成，如：
        // TOKEN总账: 13054 + 688.64 + 1172 + 0 = 14914.64 ÷ 1M = 0.01491464 元
        StringBuilder sb = new StringBuilder(label).append("总账: ");
        if (components != null && !components.isEmpty()) {
            for (int i = 0; i < components.size(); i++) {
                if (i > 0) {
                    sb.append(" + ");
                }
                sb.append(components.get(i).stripTrailingZeros().toPlainString());
            }
            sb.append(" = ");
        }
        sb.append(unscaled.stripTrailingZeros().toPlainString())
          .append(" ÷ 1M = ").append(totalCost.stripTrailingZeros().toPlainString()).append(" 元");
        details.add(sb.toString());
        return totalCost;
    }

    /**
     * 计算图文混合费用（逐维度「仅同侧兜底」：维度价无效 → 输入侧回落 inputPer1m / 输出侧回落 outputPer1m；
     * 都无效且有用量 → 抛 PRICE_NOT_CONFIGURED 拒绝计费，避免漏收）
     */
    private BigDecimal calculateImageTokenCost(UsageData usage, BillingParams params, List<String> details) {
        BigDecimal cost = BigDecimal.ZERO;

        cost = cost.add(calcTokenDimension("图片输入文本tokens", usage.getInputTextTokens(),
            resolveTokenPrice(params.getInputTextPer1m(), true, params, usage.getInputTextTokens(), "图片输入文本tokens", details), details));
        cost = cost.add(calcTokenDimension("图片输入图片tokens", usage.getInputImageTokens(),
            resolveTokenPrice(params.getInputImagePer1m(), true, params, usage.getInputImageTokens(), "图片输入图片tokens", details), details));
        cost = cost.add(calcTokenDimension("图片输出文本tokens", usage.getOutputTextTokens(),
            resolveTokenPrice(params.getOutputTextPer1m(), false, params, usage.getOutputTextTokens(), "图片输出文本tokens", details), details));
        cost = cost.add(calcTokenDimension("图片输出图片tokens", usage.getOutputImageTokens(),
            resolveTokenPrice(params.getOutputImagePer1m(), false, params, usage.getOutputImageTokens(), "图片输出图片tokens", details), details));

        return cost;
    }

    /**
     * 计算 Embedding 费用（逐维度「仅同侧兜底」：维度价无效 → 输入侧回落 inputPer1m，embedding 是纯输入操作；
     * 都无效且有用量 → 抛 PRICE_NOT_CONFIGURED 拒绝计费，避免漏收）
     */
    private BigDecimal calculateEmbeddingCost(UsageData usage, BillingParams params, List<String> details) {
        BigDecimal cost = BigDecimal.ZERO;

        cost = cost.add(calcTokenDimension("文本向量tokens", usage.getTextTokensEmbedding(),
            resolveTokenPrice(params.getTextTokensPer1m(), true, params, usage.getTextTokensEmbedding(), "文本向量tokens", details), details));
        cost = cost.add(calcTokenDimension("图片向量tokens", usage.getImageTokensEmbedding(),
            resolveTokenPrice(params.getImageTokensPer1m(), true, params, usage.getImageTokensEmbedding(), "图片向量tokens", details), details));
        cost = cost.add(calcTokenDimension("通用向量tokens", usage.getVectorTokens(),
            resolveTokenPrice(params.getVectorTokensPer1m(), true, params, usage.getVectorTokens(), "通用向量tokens", details), details));

        return cost;
    }

    /**
     * 解析 token 维度有效单价（计费价格兜底，用户确认规则：**仅同侧兜底**）。
     *
     * - 维度价有效（>0）→ 直接用维度价；
     * - 维度价无效（0/null/负）→ 兜底同侧主价：输入侧回落 inputPer1m，输出侧回落 outputPer1m；
     * - 维度价与同侧主价都无效，且该维度有用量 → 抛 {@link LlmErrorCode#PRICE_NOT_CONFIGURED} 拒绝计费，
     *   避免「有使用量却按 0 计费」的费用缺失（用户确认：全无价格可兜时拒绝而非静默免费）；
     *   无用量（tokens<=0）时不抛，返回 null（由调用方按无用量处理，费用=0）。
     *
     * 应用范围：TOKEN（输入未命中/命中、输出）、IMAGE_TOKEN（图文 4 子维度）、
     * EMBEDDING（向量 3 子维度）、VIDEO_TOKEN（分辨率档位）。
     * 推理维度除外——其未配置推理价时并入输出按输出主价计费（见 {@link #calculateTokenCost}），
     * 不在此兜底，避免与输出 tokens 重复计费。
     *
     * @return 有效单价；无用量且无价可兜时为 null
     */
    private BigDecimal resolveTokenPrice(BigDecimal dimPrice, boolean isInput, BillingParams params,
                                         long tokens, String dimName, List<String> details) {
        if (isValidPrice(dimPrice)) {
            return dimPrice;
        }
        String sideName = isInput ? "输入侧主价 inputPer1m" : "输出侧主价 outputPer1m";
        BigDecimal sideMain = isInput ? params.getInputPer1m() : params.getOutputPer1m();
        if (isValidPrice(sideMain)) {
            details.add(dimName + ": 维度价=未配置(" + (dimPrice == null ? "null" : dimPrice.stripTrailingZeros().toPlainString())
                + ")，兜底" + sideName + "=" + sideMain.stripTrailingZeros().toPlainString());
            return sideMain;
        }
        if (tokens > 0) {
            throw new LlmGatewayException(LlmErrorCode.PRICE_NOT_CONFIGURED,
                dimName + " 有用量=" + tokens + " 但维度价与" + sideName + "均未配置(≤0)，无法计费，已拒绝本次扣费");
        }
        return null;
    }

    /**
     * 检查价格是否有效（参与计费）
     * 规则：价格为 null、0 或负数时，该维度不参与计费
     */
    private boolean isValidPrice(BigDecimal price) {
        return price != null && price.compareTo(BigDecimal.ZERO) > 0;
    }
}
