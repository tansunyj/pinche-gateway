package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 模型价格表实体
 * 对应数据库表: model_prices
 *
 * 计费参数（如input_price, output_price, video_per_second_720p等）
 * 存储在 billing_params 字段（JSON格式）中
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("model_prices")
public class ModelPricesEntity {

    @Id
    private Long id;

    @Column("model_id")
    private String modelId;

    /**
     * 端点类型
     */
    @Column("endpoint_type")
    private String endpointType;

    /**
     * Token组代码
     */
    @Column("token_group_code")
    private String tokenGroupCode;

    /**
     * 是否自动派生
     */
    @Column("is_auto_derived")
    private Integer isAutoDerived;

    /**
     * 价格类型: official/platform/promotional
     */
    @Column("price_type")
    private String priceType;

    /**
     * 计费模式: token, image, video_second等
     */
    @Column("billing_mode")
    private String billingMode;

    /**
     * 基础价格
     */
    @Column("base_price")
    private BigDecimal basePrice;

    /**
     * 计费参数（JSON）
     * 包含input_price, output_price, video_per_second_720p等定价参数
     */
    @Column("billing_params")
    private String billingParams;

    /**
     * 阶梯配置（JSON）
     */
    @Column("tier_config")
    private String tierConfig;

    /**
     * 官方价格（JSON）
     */
    @Column("official_price")
    private String officialPrice;

    /**
     * 有效期开始
     */
    @Column("valid_from")
    private LocalDateTime validFrom;

    /**
     * 有效期结束
     */
    @Column("valid_until")
    private LocalDateTime validUntil;

    /**
     * 状态: 0=禁用, 1=启用
     */
    @Column("status")
    private Integer status;

    /**
     * 是否促销价
     */
    @Column("is_promotional")
    private Integer isPromotional;

    /**
     * 描述
     */
    @Column("description")
    private String description;

    @Column("created_at")
    private LocalDateTime createdAt;

    @Column("updated_at")
    private LocalDateTime updatedAt;

    /**
     * 渠道ID（用于渠道专属价格）
     */
    @Column("channel_id")
    private Long channelId;

    /**
     * 渠道名称
     */
    @Column("channel_name")
    private String channelName;
}
