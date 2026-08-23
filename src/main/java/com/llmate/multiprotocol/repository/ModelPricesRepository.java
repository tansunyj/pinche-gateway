package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ModelPricesEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 模型价格表 Repository
 */
@Repository
public interface ModelPricesRepository extends R2dbcRepository<ModelPricesEntity, Long> {

    /**
     * 根据模型ID查询价格配置
     */
    Flux<ModelPricesEntity> findByModelId(String modelId);

    /**
     * 根据模型ID和渠道ID查询价格配置
     */
    Mono<ModelPricesEntity> findByModelIdAndChannelId(String modelId, Long channelId);

    /**
     * 根据模型ID查询通用价格配置（channelId为null）
     */
    Mono<ModelPricesEntity> findByModelIdAndChannelIdIsNull(String modelId);

    /**
     * 根据模型ID和状态查询
     */
    Flux<ModelPricesEntity> findByModelIdAndStatus(String modelId, Integer status);
}
