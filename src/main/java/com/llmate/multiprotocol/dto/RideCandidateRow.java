package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 用户加入的车次候选行（车次折扣详细日志用）
 *
 * 对应查询：用户加入的所有车次（含非 ACTIVE 车次 / KICKED 成员 / 全部分组模型），
 * 由 {@link com.llmate.multiprotocol.repository.RideDiscountRepository#findUserRideCandidates} 返回。
 * 在应用层逐车次判定是否命中车次折扣，以及未命中的具体原因（状态/成员/发车时间/结束时间/模型匹配），
 * 供计费日志打印"享受了哪个车次 / 哪些车次为何没享受到"。
 *
 * 字段列名与 SQL 别名对齐（R2DBC 下划线→驼峰映射）。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RideCandidateRow {

    /** 车次 ID（pt_rides.id） */
    private Long rideId;

    /** 车次名称 */
    private String rideName;

    /** 车次状态：PENDING / ACTIVE / EXPIRED / CLOSED / CANCELLED */
    private String rideStatus;

    /** 发车时间（可空，空=无发车时间约束） */
    private LocalDateTime startTime;

    /** 结束时间（可空，空=无截止） */
    private LocalDateTime endTime;

    /** 成团锁定时间（current_count >= min_count 时锁存，可空） */
    private LocalDateTime establishedAt;

    /** 当前成员数 */
    private Integer currentCount;

    /** 最低成团人数 */
    private Integer minCount;

    /** 用户在车次内的成员状态：ACTIVE / KICKED */
    private String memberStatus;

    /** 分组 ID（pt_ride_groups.id），该用户所属车次下的折扣分组 */
    private Long groupId;

    /** 分组折扣率（decimal(3,2)，如 0.70） */
    private BigDecimal discountRate;

    /** 分组内完整模型 ID（含渠道前缀，如 aliyun/qwen3.6-flash） */
    private String modelId;
}
