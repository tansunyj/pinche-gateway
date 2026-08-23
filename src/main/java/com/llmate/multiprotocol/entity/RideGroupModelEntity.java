package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

/**
 * 车次-分组-模型关联表实体
 * 对应数据库表: pt_ride_group_models
 *
 * 车次折扣计费用：用户加入车次后，按「分组 → 模型」映射确定该模型在车次内的
 * 折扣率（pt_ride_groups.discount_rate）。本实体仅用于 R2DBC 查询锚点，
 * 实际折扣查询走 {@link com.llmate.multiprotocol.repository.RideDiscountRepository}。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("pt_ride_group_models")
public class RideGroupModelEntity {

    @Id
    private Long id;

    /**
     * 所属分组 ID（关联 pt_ride_groups.id）
     */
    @Column("group_id")
    private Long groupId;

    /**
     * 所属车次 ID（关联 pt_rides.id）
     */
    @Column("ride_id")
    private Long rideId;

    /**
     * 完整模型 ID（含渠道前缀，如 aliyun/qwen3.6-flash，与 proxy_channel_models.model_id 同口径）
     */
    @Column("model_id")
    private String modelId;

    /**
     * 模型展示名称
     */
    @Column("model_name")
    private String modelName;
}
