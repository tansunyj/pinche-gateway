package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.PriceTierTimeRangeEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

/**
 * 分时段定价明细表 Repository
 */
@Repository
public interface PriceTierTimeRangeRepository extends R2dbcRepository<PriceTierTimeRangeEntity, Long> {

    /**
     * 根据分层配置ID查询时段配置
     */
    Flux<PriceTierTimeRangeEntity> findByTierId(Long tierId);

    /**
     * 根据分层配置ID查询时段配置（按优先级排序）
     */
    Flux<PriceTierTimeRangeEntity> findByTierIdOrderByPriorityDesc(Long tierId);
}
