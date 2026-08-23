package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ProviderCapabilityEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * Adapter 能力清单表 Repository（provider_capabilities）
 */
@Repository
public interface ProviderCapabilityRepository extends R2dbcRepository<ProviderCapabilityEntity, Long> {

    /** 按 provider_alias 查询 */
    Mono<ProviderCapabilityEntity> findByProviderAlias(String providerAlias);
}
