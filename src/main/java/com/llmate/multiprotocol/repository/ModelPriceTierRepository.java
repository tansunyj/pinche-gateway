package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ModelPriceTierEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 分层定价配置表 Repository
 */
@Repository
public interface ModelPriceTierRepository extends R2dbcRepository<ModelPriceTierEntity, Long> {

    /**
     * 根据价格ID查询分层配置
     */
    Flux<ModelPriceTierEntity> findByPriceId(Long priceId);

    /**
     * 根据价格ID和状态查询启用的分层配置
     */
    Flux<ModelPriceTierEntity> findByPriceIdAndStatus(Long priceId, Integer status);

    /**
     * 查询启用的分层配置（按优先级排序）
     */
    Mono<ModelPriceTierEntity> findFirstByPriceIdAndStatusOrderByPriorityDesc(Long priceId, Integer status);
}
