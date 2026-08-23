package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ModelLibraryEntity;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 模型库表 Repository
 */
@Repository
public interface ModelLibraryRepository extends R2dbcRepository<ModelLibraryEntity, Long> {

    /**
     * 根据模型ID查询
     */
    Mono<ModelLibraryEntity> findByModelId(String modelId);

    /**
     * 根据模型ID和状态查询
     */
    Mono<ModelLibraryEntity> findByModelIdAndStatus(String modelId, Integer status);

    /**
     * 根据模型分类查询
     */
    Flux<ModelLibraryEntity> findByCategory(String category);

    /**
     * 根据状态查询所有模型
     */
    Flux<ModelLibraryEntity> findByStatus(Integer status);

    /**
     * 根据模型分类和状态查询
     */
    Flux<ModelLibraryEntity> findByCategoryAndStatus(String category, Integer status);
}
