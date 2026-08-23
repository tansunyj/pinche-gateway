package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * 计费结果
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BillingResult {

    // 费用（USD）
    private BigDecimal costInUsd;

    private BigDecimal inputCostUsd;

    private BigDecimal outputCostUsd;

    private BigDecimal reasoningCostUsd;

    private BigDecimal cacheDiscountUsd;

    // 额度
    private long quota;

    // 显示币种
    private BigDecimal costInDisplay;

    private String currency;

    // 使用的 tokens（用于日志记录）
    private long promptTokens;

    private long completionTokens;

    private long reasoningTokens;

    private long cacheHitTokens;

    private long cacheCreationTokens;

    private long cacheReadTokens;

    // 套餐折扣比例（如 0.8 表示 8 折）
    private BigDecimal packageMarkup;

    /**
     * 计费多行明细（tokens 消耗 + 各维度费用，\n 拼接，落库到合并日志表 billing_detail 字段）。
     * 由 BillingCalculator 从 detailLines 拼出；错误/固定额度结算等无明细路径为 null。
     */
    private String billingDetail;
}
