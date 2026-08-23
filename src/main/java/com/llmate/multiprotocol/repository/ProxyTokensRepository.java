package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * 用户 API Key 表 Repository
 */
@Repository
public interface ProxyTokensRepository extends R2dbcRepository<ProxyTokensEntity, Long> {

    /**
     * 根据 API Key 查询
     */
    Mono<ProxyTokensEntity> findByApiKey(String apiKey);

    /**
     * 根据 API Key 和状态查询
     */
    Mono<ProxyTokensEntity> findByApiKeyAndStatus(String apiKey, Integer status);

    /**
     * 根据用户ID查询
     */
    Mono<ProxyTokensEntity> findByUserId(Long userId);
}
