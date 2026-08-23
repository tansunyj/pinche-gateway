package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.LocalTime;

/**
 * 分时段定价明细表实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("price_tier_time_ranges")
public class PriceTierTimeRangeEntity {

    @Id
    private Long id;

    @Column("tier_id")
    private Long tierId;

    /**
     * 时段名称，如 peak, off_peak
     */
    @Column("tier_name")
    private String tierName;

    /**
     * 时段开始时间
     */
    @Column("time_start")
    private LocalTime timeStart;

    /**
     * 时段结束时间
     */
    @Column("time_end")
    private LocalTime timeEnd;

    /**
     * 时区
     */
    @Column("timezone")
    private String timezone;

    /**
     * 价格倍率
     */
    @Column("price_multiplier")
    private BigDecimal priceMultiplier;

    /**
     * 生效星期（1=周一，逗号分隔）
     */
    @Column("days_of_week")
    private String daysOfWeek;

    /**
     * 同配置下优先级
     */
    @Column("priority")
    private Integer priority;

    /**
     * 时段绝对价格覆盖（JSON），格式同 model_prices.billing_params，如 {"input_per_1m":0.9,"output_per_1m":1.1}
     * 为 NULL 表示该时段不覆盖任何维度，维持基础价；忙闲时只做绝对价，price_multiplier 倍率弃用
     */
    @Column("price_overrides")
    private String priceOverrides;
}
