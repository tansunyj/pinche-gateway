package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ProxyChannelsEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 上游渠道表 Repository
 */
@Repository
public interface ProxyChannelsRepository extends R2dbcRepository<ProxyChannelsEntity, Long> {

    /**
     * 根据渠道代码查询
     */
    Mono<ProxyChannelsEntity> findByChannelCode(String channelCode);

    /**
     * 根据渠道代码和状态查询
     */
    Mono<ProxyChannelsEntity> findByChannelCodeAndStatus(String channelCode, Integer status);

    /**
     * 查询所有启用的渠道
     */
    Flux<ProxyChannelsEntity> findAllByStatus(Integer status);
}
