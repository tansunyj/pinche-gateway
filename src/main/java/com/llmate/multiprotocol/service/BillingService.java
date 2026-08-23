package com.llmate.multiprotocol.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.BusinessConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.BillingContext;
import com.llmate.multiprotocol.dto.BillingParams;
import com.llmate.multiprotocol.dto.BillingResult;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.RideCandidateRow;
import com.llmate.multiprotocol.dto.RoutingResult;
import com.llmate.multiprotocol.dto.UsageData;
import com.llmate.multiprotocol.entity.ModelPricesEntity;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.repository.ModelPricesRepository;
import com.llmate.multiprotocol.repository.RideDiscountRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 计费服务 —— 计费全生命周期编排
 *
 * 职责：
 * 1. 价格配置查询（model_prices，渠道专属优先 + 全局兜底 + 默认配置兜底）
 * 2. 额度预占（reserve）/ 释放（release）
 * 3. 结算（settle）：费用计算 → 余额扣减 → 结算日志（proxy_logs）+ 请求日志回填（proxy_request_logs）
 * 4. 失败清理（abort）：释放预占 + 回填失败日志
 *
 * 依赖关系（分层）：
 * - BillingCalculator：纯费用计算（不含任何持久化）
 * - UserBalanceService：余额预占/扣减/释放（Redis 原子操作）
 * - SettlementService：结算/审计日志持久化
 *
 * LlmGateway 只调用本服务暴露的编排方法，不关心计费细节。
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class BillingService {

    private final ModelPricesRepository modelPricesRepository;
    private final RideDiscountRepository rideDiscountRepository;
    private final BillingCalculator billingCalculator;
    private final UserBalanceService userBalanceService;
    private final SettlementService settlementService;
    private final StatsService statsService;
    private final ChannelTokenStatsService channelTokenStatsService;
    private final ObjectMapper objectMapper;
    private final TimeBasedPricingService timeBasedPricingService;

    // ==================== 价格配置 ====================

    /**
     * 获取模型价格配置（渠道专属优先，其次全局）。
     *
     * 关键安全修复：渠道专属 + 全局都查不到价格时，直接抛 {@link LlmErrorCode#PRICE_NOT_CONFIGURED}
     * 拒绝本次调用，绝不兜底成全 0 价格 —— 否则未配价的模型会被静默 0 元计费（可被用户白嫖）。
     */
    public Mono<ModelPricesEntity> getPriceConfig(String modelId, Long channelId) {
        return modelPricesRepository.findByModelIdAndChannelId(modelId, channelId)
            .switchIfEmpty(modelPricesRepository.findByModelIdAndChannelIdIsNull(modelId))
            .switchIfEmpty(Mono.error(new LlmGatewayException(LlmErrorCode.PRICE_NOT_CONFIGURED, modelId)));
    }

    /**
     * 预估额度（用于预占余额）
     * 统一固定一口价估算，不按价格计算：
     * - 文本聊天 / 向量 / TTS 等：预占 5000 额度（5 分钱）
     * - 生图（image / image_token 计费模式）：预占 20000 额度（0.2 元）
     */
    private long estimateQuota(ModelPricesEntity priceConfig) {
        String mode = priceConfig != null ? priceConfig.getBillingMode() : null;
        if (BusinessConstants.BILLING_MODE_IMAGE.equalsIgnoreCase(mode)
                || BusinessConstants.BILLING_MODE_IMAGE_TOKEN.equalsIgnoreCase(mode)) {
            return BusinessConstants.DEFAULT_RESERVE_QUOTA_IMAGE;
        }
        return BusinessConstants.DEFAULT_RESERVE_QUOTA;
    }

    // ==================== 预占 / 释放 / 扣减 ====================

    /**
     * 预占余额（内部先估算额度再预占；异常统一转换为网关错误码）
     * BALANCE_INSUFFICIENT 透传，其余映射为 BALANCE_RESERVE_FAILED
     */
    public Mono<Long> reserve(Long userId, String requestId, ModelPricesEntity priceConfig) {
        if (userId == null) {
            return Mono.just(0L);
        }
        long estimatedQuota = estimateQuota(priceConfig);
        return userBalanceService.reserveBalance(userId, requestId, estimatedQuota)
            .doOnSuccess(v ->
                log.info("[BillingService] 余额预占成功: userId={}, requestId={}, reservedQuota={}",
                    userId, requestId, estimatedQuota))
            .onErrorResume(e -> {
                if (e instanceof LlmGatewayException &&
                    ((LlmGatewayException) e).getErrorCode() == LlmErrorCode.BALANCE_INSUFFICIENT) {
                    return Mono.error(e);
                }
                log.error("[BillingService] 余额预占失败: userId={}, requestId={}", userId, requestId, e);
                // 保 cause 链：原 (code, String) 构造器会走 errorCode.format()（模板无 %s 时丢弃 detail）且丢 cause，
                // 内部日志必须能看到真实失败原因与原始异常
                return Mono.error(new LlmGatewayException(LlmErrorCode.BALANCE_RESERVE_FAILED,
                        "Failed to reserve balance: " + e.getMessage(), e));
            });
    }

    /**
     * 释放预占余额（异步，不阻塞主响应流）
     */
    public Mono<Void> release(Long userId, String requestId) {
        if (userId == null) {
            return Mono.empty();
        }
        return userBalanceService.releaseReservedBalance(userId, requestId)
            .doOnSuccess(v -> log.info("[BillingService] 余额释放成功: userId={}, requestId={}", userId, requestId))
            .doOnError(e -> log.error("[BillingService] 余额释放失败: userId={}, requestId={}", userId, requestId, e))
            // 中心补写 requestId 到 Reactor Context：release 常以脱离主链的独立 .subscribe() 触发
            //（结算/失败兜底），新链 Context 为空 → automatic context propagation 无从恢复 MDC，
            // boundedElastic/lettuce 线程上日志 [reqId=] 恒为空。这里写进 Context，全链路日志自动带号。
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId))
            .then();
    }

    /**
     * 扣减余额（纯扣减，不处理预占；预占由调用方单独释放）
     * @param userId 用户ID
     * @param requestId 请求ID（用于扣减日志，与预占日志保持一致，便于排查）
     * @param amount 扣减额度
     */
    private Mono<Long> deduct(Long userId, String requestId, long amount) {
        if (userId == null) {
            return Mono.just(0L);
        }
        return userBalanceService.deductBalance(userId, requestId, amount);
    }

    // ==================== 非流式结算 ====================

    /**
     * 非流式结算：费用计算 → 释放预占 → 余额扣减 → 结算日志 + 请求日志回填。
     *
     * 返回 Mono<Void> 供调用方（LlmGateway）在响应返回前内联执行：保证计费日志
     * （用量提取 → 计费明细 → 计费计算 → 余额扣减 → 结算记录）在请求响应日志之前
     * 以固定顺序打印完，任何请求类型（文本聊天/生图/生视频）日志标准一致。
     * 结算失败不抛出（doOnError 释放预占 + 留痕），由调用方决定是否吞掉错误继续返回响应。
     */
    public Mono<Void> settleNonStream(BillingContext ctx, LlmChatResponse resp, long latency) {
        UsageData usageData = buildUsageData(resp);
        String upstreamModel = ctx.getRouting().getUpstreamModel();

        return buildBillingParams(ctx.getPriceConfig(), ctx.getTokenEntity(), upstreamModel, ctx.getRouting().getModelId())
            .flatMap(billingParams ->
                billingCalculator.calculateCost(
                        ctx.getPriceConfig().getBillingMode(),
                        usageData,
                        billingParams,
                        "USD",
                        ctx.getRequestId(),
                        ctx.getUserId(),
                        upstreamModel)
                    .flatMap(costResult ->
                        // 先释放预占，再扣减余额（避免预占泄漏）
                        release(ctx.getUserId(), ctx.getRequestId())
                            .then(deduct(ctx.getUserId(), ctx.getRequestId(), costResult.getQuota()))
                            .map(newBalance -> costResult))
            )
            .doOnSuccess(costResult -> {
                settlementService.recordSettlementLog(
                    ctx.getRequestId(), ctx.getUserId(), ctx.getTokenId(), ctx.getTokenEntity(),
                    ctx.getRouting(), resp, costResult, latency, "success", null);
                settlementService.recordRequestLogComplete(ctx.getRequestId(), resp, costResult, latency, 200, null);
                // 记录统计
                recordStats(ctx, buildUsageData(resp), costResult, latency, true, "chat");
                log.info("[BillingService] 非流式结算完成: requestId={}, latency={}ms, quota={}",
                    ctx.getRequestId(), latency, costResult.getQuota());
            })
            .doOnError(e -> {
                log.error("[BillingService] 非流式结算失败: requestId={}", ctx.getRequestId(), e);
                // 结算失败时也要释放预占（可能已经释放过了，但幂等操作不会重复释放）
                release(ctx.getUserId(), ctx.getRequestId()).subscribe();
            })
            .then();
    }

    /**
     * 非流式失败清理：释放预占 + 回填失败请求日志
     */
    public void abortNonStream(BillingContext ctx, long latency, Throwable e) {
        release(ctx.getUserId(), ctx.getRequestId()).subscribe();
        settlementService.recordRequestLogComplete(ctx.getRequestId(), null, null, latency, 500, e.getMessage());
    }

    /**
     * 向量接口结算：费用计算（EMBEDDING 模式）→ 释放预占 → 余额扣减 → 结算日志 + 请求日志回填。
     *
     * 返回 Mono&lt;Void&gt; 供调用方（EmbeddingService）在响应返回前内联执行：与 {@link #settleNonStream}
     * 同一模式，保证计费日志（用量提取 → 计费明细 → 计费计算 → 余额扣减 → 结算记录）在请求响应
     * 之前按固定顺序打印完（对齐日志标准：结算必须内联确定顺序）。结算失败不抛出
     * （doOnError 释放预占 + 留痕），由调用方吞掉错误继续返回响应。
     *
     * 与 {@link #settleNonStream} 的区别：不依赖 LlmChatResponse，直接用 UsageData
     * （向量响应是透传 JSON，usage 由 EmbeddingService 解析为 UsageData）。
     */
    public Mono<Void> settleEmbedding(BillingContext ctx, UsageData usageData, long latency, String responseBodyJson) {
        String upstreamModel = ctx.getRouting().getUpstreamModel();

        return buildBillingParams(ctx.getPriceConfig(), ctx.getTokenEntity(), upstreamModel, ctx.getRouting().getModelId())
            .flatMap(billingParams ->
                billingCalculator.calculateCost(
                        ctx.getPriceConfig().getBillingMode(),
                        usageData,
                        billingParams,
                        "USD",
                        ctx.getRequestId(),
                        ctx.getUserId(),
                        upstreamModel)
                    .flatMap(costResult ->
                        // 先释放预占，再扣减余额（避免预占泄漏）
                        release(ctx.getUserId(), ctx.getRequestId())
                            .then(deduct(ctx.getUserId(), ctx.getRequestId(), costResult.getQuota()))
                            .map(newBalance -> costResult))
            )
            .doOnSuccess(costResult -> {
                settlementService.recordStreamSettlementLog(
                    ctx.getRequestId(), ctx.getUserId(), ctx.getTokenId(), ctx.getTokenEntity(),
                    ctx.getRouting(), usageData, costResult, latency, "success");
                settlementService.recordEmbeddingRequestLogComplete(
                    ctx.getRequestId(), usageData, costResult, latency, 200, responseBodyJson, null);
                // 记录统计
                recordStats(ctx, usageData, costResult, latency, true, "embedding");
                log.info("[BillingService] 向量结算完成: requestId={}, latency={}ms, quota={}",
                    ctx.getRequestId(), latency, costResult.getQuota());
            })
            .doOnError(e -> {
                log.error("[BillingService] 向量结算失败: requestId={}", ctx.getRequestId(), e);
                // 结算失败时也要释放预占（可能已经释放过了，但幂等操作不会重复释放）
                release(ctx.getUserId(), ctx.getRequestId()).subscribe();
            })
            .doOnNext(costResult -> log.debug("[BillingService] 向量扣减成功: requestId={}, quota={}",
                ctx.getRequestId(), costResult.getQuota()))
            .doOnError(err -> log.error("[BillingService] 向量结算流程失败: requestId={}", ctx.getRequestId(), err))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, ctx.getRequestId()))
            .then();
    }

    /**
     * 从 billing_params JSON 解析视频 token 单价档位 Map（key: {480p|720p|1080p|4k}_{noInput|withInput}）
     */
    private Map<String, BigDecimal> parseVideoTokenPrices(ModelPricesEntity priceConfig) {
        Map<String, BigDecimal> map = new java.util.HashMap<>();
        if (priceConfig == null || priceConfig.getBillingParams() == null) {
            return map;
        }
        try {
            JsonNode root = objectMapper.readTree(priceConfig.getBillingParams());
            String[] keys = {"480p_noInput", "480p_withInput", "720p_noInput", "720p_withInput",
                    "1080p_noInput", "1080p_withInput", "4k_noInput", "4k_withInput"};
            for (String k : keys) {
                if (root.has(k) && !root.get(k).isNull()) {
                    map.put(k, new BigDecimal(root.get(k).asText()));
                }
            }
        } catch (Exception e) {
            log.warn("[BillingService] 解析视频 token 价格失败: {}", e.getMessage());
        }
        return map;
    }

    // ==================== 流式结算 ====================

    /**
     * 流式结算：费用计算 → 释放预占 → 余额扣减 → 结算日志 + 请求日志回填
     * 独立订阅执行，不阻塞主响应链路。
     */
    public void settleStream(BillingContext ctx, UsageData usageData, long latency, long firstChunkLatencyMs, int chunkCount) {
        String upstreamModel = ctx.getRouting().getUpstreamModel();

        buildBillingParams(ctx.getPriceConfig(), ctx.getTokenEntity(), upstreamModel, ctx.getRouting().getModelId())
            .flatMap(billingParams ->
                billingCalculator.calculateCost(
                        ctx.getPriceConfig().getBillingMode(),
                        usageData,
                        billingParams,
                        "USD",
                        ctx.getRequestId(),
                        ctx.getUserId(),
                        upstreamModel)
                    .flatMap(costResult ->
                        // 先释放预占，再扣减余额（避免预占泄漏）
                        release(ctx.getUserId(), ctx.getRequestId())
                            .then(deduct(ctx.getUserId(), ctx.getRequestId(), costResult.getQuota()))
                            .map(newBalance -> costResult))
            )
            .doOnSuccess(costResult -> {
                settlementService.recordStreamSettlementLog(
                    ctx.getRequestId(), ctx.getUserId(), ctx.getTokenId(), ctx.getTokenEntity(),
                    ctx.getRouting(), usageData, costResult, latency, "success");
                settlementService.recordStreamRequestLogComplete(
                    ctx.getRequestId(), usageData, costResult, latency, firstChunkLatencyMs, 200, null, chunkCount);
                // 记录统计
                recordStats(ctx, usageData, costResult, latency, true, "stream");
                log.info("[BillingService] 流式结算完成: requestId={}, latency={}ms, firstChunkLatency={}ms, streamChunks={}, totalTokens={}",
                    ctx.getRequestId(), latency, firstChunkLatencyMs, chunkCount, usageData.getTotalTokens());
            })
            .doOnError(err -> {
                log.error("[BillingService] 流式结算失败: requestId={}", ctx.getRequestId(), err);
                settlementService.recordStreamRequestLogComplete(
                    ctx.getRequestId(), usageData, null, latency, firstChunkLatencyMs, 500, err.getMessage(), chunkCount);
                // 结算失败时也要释放预占（可能已经释放过了，但幂等操作不会重复释放）
                release(ctx.getUserId(), ctx.getRequestId()).subscribe();
            })
            .doOnNext(costResult -> log.info("[BillingService] 流式扣减成功: requestId={}, quota={}",
                ctx.getRequestId(), costResult.getQuota()))
            .doOnError(err -> log.error("[BillingService] 流式结算流程失败: requestId={}", ctx.getRequestId(), err))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, ctx.getRequestId()))
            .subscribe(v -> {}, err -> {});
    }

    /**
     * 流式失败清理：释放预占 + 回填失败请求日志
     */
    public void abortStream(BillingContext ctx, UsageData usageData, long latency, long firstChunkLatencyMs, Throwable e, int chunkCount) {
        release(ctx.getUserId(), ctx.getRequestId()).subscribe();
        settlementService.recordStreamRequestLogComplete(
            ctx.getRequestId(), usageData, null, latency, firstChunkLatencyMs, 500,
            e != null ? e.getMessage() : "stream aborted", chunkCount);
    }

    // ==================== 内部工具方法 ====================

    /**
     * 从 billingParams JSON 解析价格
     */
    private BigDecimal getPriceFromBillingParams(ModelPricesEntity priceConfig, String priceKey) {
        if (priceConfig == null || priceConfig.getBillingParams() == null) {
            return BigDecimal.ZERO;
        }
        try {
            JsonNode root = objectMapper.readTree(priceConfig.getBillingParams());
            if (root.has(priceKey) && !root.get(priceKey).isNull()) {
                return new BigDecimal(root.get(priceKey).asText());
            }
        } catch (Exception e) {
            log.warn("[BillingService] 解析 billingParams 失败: {}", e.getMessage());
        }
        return BigDecimal.ZERO;
    }

    /**
     * 构建 BillingParams（从价格配置解析各维度单价，并整合车次折扣）
     * 折扣逻辑：用户加入 ACTIVE 车次且命中该车次分组内模型 → 用该组 discount_rate（多车次取最低）；
     * 未加入车次 / 模型不在车次分组 → 原价（×1.0）。
     *
     * 车次折扣使用「原始完整模型ID」匹配（渠道code + 模型名，如 aliyun/deepseek-v4-flash）
     *
     * @param modelId         纯模型ID（upstreamModel，仅作空值兜底判断）
     * @param discountModelId 原始完整模型ID（routing.getModelId()，车次折扣匹配用它）
     */
    private Mono<BillingParams> buildBillingParams(ModelPricesEntity priceConfig, ProxyTokensEntity tokenEntity,
                                                   String modelId, String discountModelId) {
        BillingParams params = new BillingParams();
        params.setInputPer1m(getPriceFromBillingParams(priceConfig, "input_per_1m"));
        params.setOutputPer1m(getPriceFromBillingParams(priceConfig, "output_per_1m"));
        // 推理/思考 tokens 单价：优先读 DB 惯例 key thinking_output_per_m，
        // 兼容旧 key reasoning_per_1m（之前只读后者，导致所有模型推理价恒为 0）
        BigDecimal reasoningPer1m = getPriceFromBillingParams(priceConfig, "thinking_output_per_m");
        if (reasoningPer1m.compareTo(BigDecimal.ZERO) == 0) {
            reasoningPer1m = getPriceFromBillingParams(priceConfig, "reasoning_per_1m");
        }
        params.setReasoningPer1m(reasoningPer1m);
        // 缓存计费两价模型：只有 cache_hit_per_1m（历史缓存输入价），input_per_1m 已在上方读取。
        // cache_creation / cache_read / cache_write 已确认不参与计费（缓存创建=纯新输入、
        // 缓存读取=缓存输入，并入输入拆分计费），不再读取。
        params.setCacheHitPer1m(getPriceFromBillingParams(priceConfig, "cache_hit_per_1m"));

        // 向量计费维度（EMBEDDING 模式）：从 billing_params 解析各向量单价，
        // 否则 calculateEmbeddingCost 判定"未配置价格"导致费用恒为 0（不计费 bug）
        params.setTextTokensPer1m(getPriceFromBillingParams(priceConfig, "text_tokens_per_1m"));
        params.setImageTokensPer1m(getPriceFromBillingParams(priceConfig, "image_tokens_per_1m"));
        params.setVectorTokensPer1m(getPriceFromBillingParams(priceConfig, "vector_tokens_per_1m"));

        // TTS 计费维度：从 billing_params 解析按字符单价，
        // 否则 calculateCost 的 TTS 分支判定"未配置价格"导致费用恒为 0（不计费 bug）
        params.setCharactersPer1k(getPriceFromBillingParams(priceConfig, "characters_per_1k"));

        // 图文计费维度（IMAGE_TOKEN 模式）：从 billing_params 解析图文单价，
        // 否则 calculateImageTokenCost 判定"未配置价格"导致费用恒为 0（不计费 bug）
        params.setInputTextPer1m(getPriceFromBillingParams(priceConfig, "input_text_per_1m"));
        params.setInputImagePer1m(getPriceFromBillingParams(priceConfig, "input_image_per_1m"));
        params.setOutputTextPer1m(getPriceFromBillingParams(priceConfig, "output_text_per_1m"));
        params.setOutputImagePer1m(getPriceFromBillingParams(priceConfig, "output_image_per_1m"));

        // 图像按张计费维度（IMAGE 模式）：从 billing_params 解析按张单价（如 {"image_per_call": 0.2}），
        // 否则 calculateCost 的 IMAGE 分支判定"未配置价格"导致费用恒为 0（不计费 bug）
        params.setImagePerCall(getPriceFromBillingParams(priceConfig, "image_per_call"));

        // 按次 / 视频时长计费维度（FLAT / VIDEO_SECOND 模式），同一类"漏读字段导致费用恒为 0"的问题一并补齐
        params.setFlatPrice(getPriceFromBillingParams(priceConfig, "flat_price"));
        params.setVideoPerSecond720p(getPriceFromBillingParams(priceConfig, "video_per_second_720p"));
        params.setVideoPerSecond1080p(getPriceFromBillingParams(priceConfig, "video_per_second_1080p"));

        // ASR 语音转写计费维度（ASR 模式）：从 billing_params 解析按秒单价，
        // 否则 calculateCost 的 ASR 分支判定"未配置价格"导致费用恒为 0（不计费 bug）
        params.setAudioPerSecond(getPriceFromBillingParams(priceConfig, "audio_per_second"));

        // 视频 token 计费维度（VIDEO_TOKEN 模式，Seedance）：解析各分辨率档位单价，
        // key 形如 {480p|720p|1080p|4k}_{noInput|withInput}，
        // 否则 calculateCost 的 VIDEO_TOKEN 分支 getVideoTokenPrice 恒为 null → 费用恒为 0（不计费 bug）
        params.setVideoTokenPrices(parseVideoTokenPrices(priceConfig));

        // ============ 车次折扣（取代原 API Key price_markup）============
        // pt_carpool 已删除套餐/用户模型折扣表，折扣来源改为「用户加入的已发车车次」：
        // 命中该车次分组内模型 → 用该组 discount_rate；无已发车车次/模型不在分组 → 原价(×1.0)。
        // 用户可同时在多个车次，多车次分组都覆盖该模型时取最优惠（最低折扣率）。
        // 生效条件（与产品确认）：只查「已发车」车次——车次 ACTIVE + 成员 ACTIVE + 成团
        // （established_at 非空 或 current_count>=min_count）+ 当前时间 >= start_time + 未过 end_time，
        // 在 SQL 预过滤；未发车 / 已取消 / 已结束的车次不再查询。
        Long userId = tokenEntity != null ? tokenEntity.getUserId() : null;
        if (userId == null) {
            log.info("[BillingService] 车次折扣: userId=null（无关联用户）, 无车次折扣, 原价(×1.0)");
            params.setPriceMarkup(BigDecimal.ONE);
            return timeBasedPricingService.applyTimePricing(params, priceConfig.getId(), ZonedDateTime.now());
        }

        return rideDiscountRepository.findUserRideCandidates(userId)
                .collectList()
                .flatMap(candidates -> {
                    applyRideDiscount(candidates, userId, discountModelId, params);

                    // 忙闲时（分时段定价）：价格解析阶段叠加「时段绝对价覆盖」。
                    // 命中时段 → 用该时段 price_overrides 覆盖 BillingParams 对应维度；
                    // 未命中/未配置 → 原样返回（基础价兜底）。与折扣（priceMarkup）正交。
                    return timeBasedPricingService.applyTimePricing(params, priceConfig.getId(), ZonedDateTime.now());
                });
    }

    /**
     * 车次折扣判定 + 详细日志。
     *
     * candidates 已由 SQL 预过滤为「已发车」车次（车次 ACTIVE + 成员 ACTIVE + 成团 +
     * 已到发车时间 + 未过结束时间），此处仅匹配模型：
     * 命中 = 该已发车车次某分组含该完整模型 ID（且该分组有有效 discount_rate）。
     * 多车次都命中时取最低 discount_rate；模型未命中的车次打未命中日志说明原因。
     * （状态 / 成团 / 发车时间 / 结束时间判定保留为防御：SQL 已保证，正常不触发。）
     *
     * @param candidates 用户加入的「已发车」车次候选行（可能多行：一车次 × 多分组 × 多模型）
     * @param userId     计费用户 ID（仅日志）
     * @param modelId    原始完整模型 ID（含渠道前缀，如 aliyun/qwen3.6-flash）
     * @param params     待写入 priceMarkup 的计费参数
     */
    private void applyRideDiscount(List<RideCandidateRow> candidates, Long userId, String modelId, BillingParams params) {
        if (candidates == null || candidates.isEmpty()) {
            log.info("[BillingService] 车次折扣: userId={}, modelId={}, 无已发车车次（未加入 / 未发车 / 已取消 / 已结束）, 原价(×1.0)", userId, modelId);
            params.setPriceMarkup(BigDecimal.ONE);
            return;
        }

        LocalDateTime nowLdt = LocalDateTime.now();

        // 按车次聚合（LinkedHashMap 保序）
        Map<Long, List<RideCandidateRow>> byRide = candidates.stream()
                .collect(Collectors.groupingBy(RideCandidateRow::getRideId,
                        LinkedHashMap::new, Collectors.toList()));

        BigDecimal bestRate = BigDecimal.ONE;       // 最终折扣（多车次取最低），无命中=1.0
        List<String> hitDescs = new ArrayList<>();  // 命中车次描述
        List<String> missDescs = new ArrayList<>(); // 未命中车次 + 原因

        for (List<RideCandidateRow> rows : byRide.values()) {
            RideCandidateRow first = rows.get(0);
            List<String> reasons = new ArrayList<>();

            boolean rideActive = "ACTIVE".equals(first.getRideStatus());
            if (!rideActive) {
                reasons.add("车次状态=" + first.getRideStatus() + "(非ACTIVE)");
            }

            boolean memberActive = "ACTIVE".equals(first.getMemberStatus());
            if (!memberActive) {
                reasons.add("成员状态=" + first.getMemberStatus() + "(非ACTIVE)");
            }

            // 发车判定（与产品确认）：满足人数门槛（成团）且到发车时间 = 自动发车。
            // 已发车才能享受折扣；未发车（未成团 / 未到发车时间）一律不打折。
            boolean established = first.getEstablishedAt() != null
                    || (first.getCurrentCount() != null && first.getMinCount() != null
                        && first.getCurrentCount() >= first.getMinCount());
            if (!established) {
                reasons.add("未成团 (current_count=" + first.getCurrentCount()
                        + " < min_count=" + first.getMinCount() + ")");
            }

            boolean timeReached = first.getStartTime() == null || !nowLdt.isBefore(first.getStartTime());
            if (!timeReached) {
                reasons.add("未到发车时间 start_time=" + first.getStartTime());
            }

            // 已发车 = 成团 && 已到发车时间
            boolean departed = established && timeReached;

            boolean notExpired = first.getEndTime() == null || nowLdt.isBefore(first.getEndTime());
            if (!notExpired) {
                reasons.add("已过结束时间 end_time=" + first.getEndTime());
            }

            // 该车次内命中该模型的所有分组折扣率，取最低（同车次多组兜底）
            BigDecimal rideBest = rows.stream()
                    .filter(r -> r.getModelId() != null && r.getModelId().equals(modelId))
                    .map(RideCandidateRow::getDiscountRate)
                    .filter(Objects::nonNull)
                    .min(BigDecimal::compareTo)
                    .orElse(null);

            boolean modelHit = rideBest != null;
            if (!modelHit) {
                reasons.add("车次分组未包含模型 " + modelId);
            }

            if (rideActive && memberActive && departed && notExpired && modelHit) {
                if (rideBest.compareTo(bestRate) < 0) {
                    bestRate = rideBest;
                }
                hitDescs.add(String.format("rideId=%d, rideName=%s, discountRate=%s",
                        first.getRideId(), first.getRideName(), rideBest.toPlainString()));
            } else {
                List<String> groupModels = rows.stream()
                        .map(RideCandidateRow::getModelId)
                        .filter(Objects::nonNull)
                        .distinct()
                        .toList();
                missDescs.add(String.format("rideId=%d, rideName=%s, rideStatus=%s, memberStatus=%s, startTime=%s, endTime=%s, 分组模型=%s, 原因=[%s]",
                        first.getRideId(), first.getRideName(), first.getRideStatus(), first.getMemberStatus(),
                        first.getStartTime(), first.getEndTime(), groupModels, String.join("; ", reasons)));
            }
        }

        for (String d : hitDescs) {
            log.info("[BillingService] 车次折扣命中: userId={}, modelId={}, {}", userId, modelId, d);
        }
        for (String d : missDescs) {
            log.info("[BillingService] 车次折扣未命中: userId={}, modelId={}, {}", userId, modelId, d);
        }
        if (hitDescs.isEmpty()) {
            log.info("[BillingService] 车次折扣: userId={}, modelId={}, 候选车次={}, 命中=0, 最终折扣率=1.0（原价）",
                    userId, modelId, byRide.size());
        } else {
            log.info("[BillingService] 车次折扣: userId={}, modelId={}, 候选车次={}, 命中={}, 最终折扣率={}（来源: 车次折扣，多车次取最低）",
                    userId, modelId, byRide.size(), hitDescs.size(), bestRate.toPlainString());
        }
        params.setPriceMarkup(bestRate);
    }

    /**
     * 从响应构建 UsageData
     */
    private UsageData buildUsageData(LlmChatResponse resp) {
        UsageData.UsageDataBuilder builder = UsageData.builder();

        if (resp.getUsage() != null) {
            LlmChatResponse.Usage usage = resp.getUsage();
            builder.inputTokens(usage.getPromptTokens())
                   .outputTokens(usage.getCompletionTokens())
                   .totalTokens(usage.getTotalTokens())
                   // 推理/缓存 tokens：非流式响应也携带，供 reasoningPer1m / cache 维度单独计费
                   .reasoningTokens(usage.getReasoningTokens())
                   .cacheHitTokens(usage.getCacheHitTokens())
                   .cacheMissTokens(usage.getCacheMissTokens())
                   .cacheCreationTokens(usage.getCacheCreationTokens())
                   .cacheReadTokens(usage.getCacheReadTokens())
                   // OpenAI/Azure: prompt_tokens_details.cached_tokens 是 input 中命中缓存的部分。
                   // 之前漏映射：cachedInputTokens 恒为 0 → calcInputTokens 不做拆分，缓存命中按全价计费。
                   .cachedTokens(usage.getCachedTokens())
                   // 历史缓存输入三源合并（覆盖所有上游缓存字段口径）：
                   //   ① cachedTokens     —— OpenAI/Azure/Gemini 新格式 prompt_tokens_details.cached_tokens / cached_content_token_count
                   //   ② cacheReadTokens  —— Anthropic 风格 cache_read_input_tokens
                   //   ③ cacheHitTokens   —— 旧格式 prompt_cache_hit_tokens（DeepSeek 等）
                   // 任一非 0 即激活 calcInputTokens 拆分计费，否则缓存命中按全价计。
                   .cachedInputTokens(usage.getCachedTokens() > 0 ? usage.getCachedTokens()
                           : (usage.getCacheReadTokens() > 0 ? usage.getCacheReadTokens() : usage.getCacheHitTokens()))
                   // 图像计费维度：image 模式按张数，image_token 模式按图文 token
                   .imageCount(usage.getImageCount())
                   .inputTextTokens(usage.getInputTextTokens())
                   .inputImageTokens(usage.getInputImageTokens())
                   .outputTextTokens(usage.getOutputTextTokens())
                   .outputImageTokens(usage.getOutputImageTokens());
        }

        return builder.build();
    }

    // ==================== 统计记录 ====================

    /**
     * 记录请求统计（Redis 实时统计 + unified_stats 表）
     * 异步执行，不阻塞主响应流
     *
     * @param requestType 请求类型: chat/stream/embedding/tts/fixed/video
     */
    private void recordStats(BillingContext ctx, UsageData usageData, BillingResult costResult,
                             long latency, boolean isSuccess, String requestType) {
        try {
            java.time.LocalDateTime now = java.time.LocalDateTime.now(java.time.ZoneId.of("Asia/Shanghai"));
            String date = now.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String hour = String.format("%02d", now.getHour());
            int minute = now.getMinute();

            // 1. Redis 实时统计
            StatsService.RequestStatsData statsData = new StatsService.RequestStatsData(
                date,
                hour,
                minute,
                ctx.getRouting().getChannelId(),
                ctx.getRouting().getChannelCode(),
                null, // channelType 暂不提供
                ctx.getTokenId(),
                ctx.getTokenEntity() != null ? ctx.getTokenEntity().getTokenName() : null,
                ctx.getUserId(),
                ctx.getRouting().getModelId(),
                (int) usageData.getInputTokens(),
                (int) usageData.getOutputTokens(),
                costResult != null ? costResult.getQuota() : 0,
                latency,
                isSuccess
            );
            statsService.recordRequest(statsData)
                .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, ctx.getRequestId()))
                .subscribe();

            // 2. 记录请求类型统计
            if (ctx.getUserId() != null && requestType != null) {
                statsService.recordRequestType(ctx.getUserId(), requestType)
                    .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, ctx.getRequestId()))
                    .subscribe();
            }

            // 3. 渠道Token统计
            if (ctx.getTokenId() != null) {
                channelTokenStatsService.recordUsage(ctx.getTokenId(), isSuccess)
                    .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, ctx.getRequestId()))
                    .subscribe();
            }
        } catch (Exception e) {
            log.error("[BillingService] 记录统计失败: requestId={}, error={}", ctx.getRequestId(), e.getMessage());
        }
    }

}
