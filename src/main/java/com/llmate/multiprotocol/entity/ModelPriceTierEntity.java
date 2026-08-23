package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * 分层定价配置表实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("model_price_tiers")
public class ModelPriceTierEntity {

    @Id
    private Long id;

    /**
     * 关联 model_prices 表
     */
    @Column("price_id")
    private Long priceId;

    /**
     * time_of_day, usage_tier, combined
     */
    @Column("tier_type")
    private String tierType;

    /**
     * 配置名称，如 peak_hours, high_volume_discount
     */
    @Column("tier_name")
    private String tierName;

    /**
     * 优先级，高优先级覆盖低优先级
     */
    @Column("priority")
    private Integer priority;

    /**
     * 0=禁用, 1=启用
     */
    @Column("status")
    private Integer status;

    @Column("created_at")
    private LocalDateTime createdAt;
}
