package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ProxyChannelModelsEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 渠道-模型关联表 Repository
 */
@Repository
public interface ProxyChannelModelsRepository extends R2dbcRepository<ProxyChannelModelsEntity, Long> {

    /**
     * 根据渠道ID查询所有关联模型
     */
    Flux<ProxyChannelModelsEntity> findByChannelId(Long channelId);

    /**
     * 根据渠道ID和启用状态查询关联模型
     */
    Flux<ProxyChannelModelsEntity> findByChannelIdAndIsEnabled(Long channelId, Integer isEnabled);

    /**
     * 根据模型ID查询所有关联渠道
     */
    Flux<ProxyChannelModelsEntity> findByModelId(String modelId);

    /**
     * 根据渠道ID和模型ID查询
     */
    Mono<ProxyChannelModelsEntity> findByChannelIdAndModelId(Long channelId, String modelId);

    /**
     * 根据渠道ID、模型ID和状态查询
     */
    Mono<ProxyChannelModelsEntity> findByChannelIdAndModelIdAndIsEnabled(Long channelId, String modelId, Integer isEnabled);

    /**
     * 按模型ID查询首个启用的关联渠道（按优先级降序，用于 TTS/Embedding/ASR 等非聊天服务）
     */
    Mono<ProxyChannelModelsEntity> findFirstByModelIdAndIsEnabledOrderByPriorityDesc(String modelId, Integer isEnabled);
}
