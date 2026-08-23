package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ProxyChannelTokensEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

/**
 * 渠道上游 Token 池表 Repository
 */
@Repository
public interface ProxyChannelTokensRepository extends R2dbcRepository<ProxyChannelTokensEntity, Long> {

    /**
     * 根据渠道ID查询所有 Token
     */
    Flux<ProxyChannelTokensEntity> findByChannelId(Long channelId);

    /**
     * 根据渠道ID和状态查询 Token
     */
    Flux<ProxyChannelTokensEntity> findByChannelIdAndStatus(Long channelId, Integer status);
}
