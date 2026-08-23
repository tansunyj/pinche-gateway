package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.EndpointEntity;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

/**
 * 端点表 Repository
 */
@Repository
public interface EndpointRepository extends ReactiveCrudRepository<EndpointEntity, Long> {
}
