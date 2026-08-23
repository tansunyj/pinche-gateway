package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.dto.RideCandidateRow;
import com.llmate.multiprotocol.entity.RideGroupModelEntity;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

/**
 * 车次折扣查询 Repository
 *
 * 计费折扣来源：用户加入车次（ACTIVE 成员）后，命中该车次分组内模型 → 用该组
 * discount_rate；未加入任何车次 / 模型不在车次分组 → 原价（×1.0）。
 *
 * 规则（与产品确认）：
 * - 只查「已发车」的车次：车次 status='ACTIVE' 且成员 status='ACTIVE'，
 *   满足人数门槛成团（established_at 非空或 current_count>=min_count）且当前时间 >= start_time，
 *   且未过 end_time；未发车（未成团 / 未到发车时间）、已取消 / 已下线、已结束的车次一律不查。
 *   （SQL 预过滤，应用层不再对未发车 / 取消的车次逐条判定。）
 * - 已发车的车次不能再接受新成员上车（上车校验在 carpool/server，见 §2 车次语义）；
 * - 用户可同时在多个车次，多个车次分组都覆盖同一模型时取最优惠（最低 discount_rate）；
 * - model_id 用完整模型 ID（含渠道前缀，如 aliyun/qwen3.6-flash）。
 */
@Repository
public interface RideDiscountRepository extends R2dbcRepository<RideGroupModelEntity, Long> {

    /**
     * 查询用户加入且「已发车」的车次候选行。
     *
     * SQL 预过滤，只返回：车次 ACTIVE + 成员 ACTIVE + 已成团 + 已到发车时间 + 未过结束时间。
     * 取消 / 未发车（未成团、未到时间）/ 已结束的车次不会出现在结果里，无需应用层再判状态。
     * 返回每个已发车车次 × 每个分组 × 每个模型的明细行，供应用层匹配模型取最低折扣率。
     */
    @Query("""
            SELECT r.id            AS ride_id,
                   r.name          AS ride_name,
                   r.status        AS ride_status,
                   r.start_time    AS start_time,
                   r.end_time      AS end_time,
                   r.established_at AS established_at,
                   r.current_count AS current_count,
                   r.min_count     AS min_count,
                   rm.status       AS member_status,
                   rg.id           AS group_id,
                   rg.discount_rate AS discount_rate,
                   rgm.model_id    AS model_id
            FROM pt_ride_members rm
            JOIN pt_rides r ON r.id = rm.ride_id
            LEFT JOIN pt_ride_groups rg ON rg.ride_id = r.id
            LEFT JOIN pt_ride_group_models rgm ON rgm.group_id = rg.id
            WHERE rm.user_id = :userId
              AND r.status = 'ACTIVE'               -- 车次已上线
              AND rm.status = 'ACTIVE'              -- 成员仍有效
              -- 已发车：成团（established_at 非空 或 current_count>=min_count）且已到发车时间
              AND (r.established_at IS NOT NULL
                   OR (r.current_count IS NOT NULL AND r.min_count IS NOT NULL
                       AND r.current_count >= r.min_count))
              AND (r.start_time IS NULL OR r.start_time <= NOW())
              -- 未过结束时间（end_time 空 = 无截止）
              AND (r.end_time IS NULL OR r.end_time > NOW())
            ORDER BY r.id, rg.display_order, rg.id
            """)
    Flux<RideCandidateRow> findUserRideCandidates(@Param("userId") Long userId);
}
