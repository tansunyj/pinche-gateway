package com.llmate.multiprotocol.dto;

import lombok.Data;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * 计费参数（从数据库 model_prices 表的 billing_params JSON 解析）
 */
@Data
public class BillingParams {

    // 基础价格
    private BigDecimal inputPer1m;

    private BigDecimal outputPer1m;

    // 可选价格（为 0 或空时不参与计费）
    // 缓存计费只保留两价模型：inputPer1m（纯新输入）+ cacheHitPer1m（历史缓存输入）。
    // cache_creation / cache_read / cache_write 三价与输入口径重复（缓存创建=纯新输入、
    // 缓存读取=缓存输入），已确认移除独立计费，不再建模。
    private BigDecimal cacheHitPer1m;

    private BigDecimal reasoningPer1m;

    // 多模态价格
    private Map<String, BigDecimal> modalityPrices;

    // 多模态计费
    private BigDecimal imagePerCall;

    private BigDecimal inputTextPer1m;

    private BigDecimal inputImagePer1m;

    private BigDecimal outputTextPer1m;

    private BigDecimal outputImagePer1m;

    private BigDecimal videoPerSecond720p;

    private BigDecimal videoPerSecond1080p;

    private Map<String, BigDecimal> videoTokenPrices;

    private BigDecimal textTokensPer1m;

    private BigDecimal imageTokensPer1m;

    private BigDecimal vectorTokensPer1m;

    private BigDecimal charactersPer1k;

    private BigDecimal flatPrice;

    // ASR 语音转写按秒计费（billing_params key：audio_per_second，元/秒）
    private BigDecimal audioPerSecond;

    // 价格倍率
    private BigDecimal priceMarkup;

    // ============ 车次折扣归属(§5.1,由 BillingService.applyRideDiscount 填充) ============
    /** 命中的折扣车次(全部候选,供 proxy_logs 审计留存) */
    private List<Long> rideIds;

    /** 实际生效车次(多车次中最低折扣率的那一个,用于 ride 维度归属,一单只计一个) */
    private Long effectiveRideId;

    /** 生效车次名称(ride 维度 meta_json) */
    private String effectiveRideName;

    // 模型ID（用于日志）
    private String modelId;

    /**
     * 获取模态价格（安全获取）
     */
    public BigDecimal getModalityPrice(String modality) {
        if (modalityPrices == null) return null;
        return modalityPrices.get(modality);
    }

    /**
     * 获取视频 token 价格（安全获取）
     */
    public BigDecimal getVideoTokenPrice(String key) {
        if (videoTokenPrices == null) return null;
        return videoTokenPrices.get(key);
    }
}
