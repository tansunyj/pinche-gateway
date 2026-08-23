package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * Adapter 能力清单表实体（provider_capabilities）
 *
 * 数据由网关启动时从 {@code ProviderCapabilityCatalog} 枚举 upsert 同步（枚举是真相源，
 * 本表是其物化副本，供 admin_backend 直连 MySQL 读取做下拉数据源与校验，避免跨进程 HTTP 依赖）。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("provider_capabilities")
public class ProviderCapabilityEntity {

    @Id
    private Long id;

    /** provider_alias，路由 key（绑定行 JSON 用），唯一 */
    @Column("provider_alias")
    private String providerAlias;

    /** 能力域：chat/image/video */
    @Column("domain")
    private String domain;

    /** 后台展示名 */
    @Column("name")
    private String name;

    /** Adapter 实现类全限定名 */
    @Column("class_name")
    private String className;

    @Column("created_at")
    private LocalDateTime createdAt;

    @Column("updated_at")
    private LocalDateTime updatedAt;
}
