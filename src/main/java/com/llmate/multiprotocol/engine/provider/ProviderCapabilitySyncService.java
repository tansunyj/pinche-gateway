package com.llmate.multiprotocol.engine.provider;

import com.llmate.multiprotocol.entity.ProviderCapabilityEntity;
import com.llmate.multiprotocol.repository.ProviderCapabilityRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;

/**
 * Adapter 能力清单同步服务
 *
 * 启动时把 {@link ProviderCapabilityCatalog} 枚举 upsert 到 provider_capabilities 表：
 * - 表内已存在且字段一致 → 跳过
 * - 表内已存在但字段变化（如改了展示名/类名） → 更新
 * - 表内不存在 → 插入
 *
 * 枚举是真相源，表是物化副本，供 admin_backend 直连 MySQL 读取（下拉数据源 + provider_capability 校验），
 * 避免 admin_backend 依赖跨进程 HTTP 调网关。新增 Adapter 能力 = 改枚举 + Factory switch，重启即同步入表。
 */
@Component
@RequiredArgsConstructor
@Log4j2
public class ProviderCapabilitySyncService {

    private final ProviderCapabilityRepository repository;

    @PostConstruct
    public void init() {
        try {
            syncFromCatalog()
                .subscribeOn(Schedulers.boundedElastic())
                .block();
        } catch (Exception e) {
            // 同步失败仅告警，不影响网关启动；下次启动（或手动调用 syncFromCatalog()）重试
            log.error("[ProviderCapabilitySync] 能力清单同步失败（不影响网关启动，下次启动重试）: {}",
                    e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
        }
    }

    /**
     * 枚举 → 表 upsert（可手动调用触发重新同步）
     */
    public Mono<Void> syncFromCatalog() {
        return Flux.fromIterable(ProviderCapabilityCatalog.list())
            .concatMap(this::upsertOne)
            .then()
            .doOnSuccess(v -> log.info("[ProviderCapabilitySync] 能力清单同步完成，共 {} 条",
                    ProviderCapabilityCatalog.list().size()))
            .doOnError(e -> log.error("[ProviderCapabilitySync] 能力清单同步失败", e));
    }

    private Mono<Void> upsertOne(ProviderCapabilityCatalog cat) {
        // 先查：collectList 区分"不存在"（空列表）与"存在但无变化"（非空且字段一致），
        // 避免用 switchIfEmpty 误把"存在但跳过"当成"不存在"而重复 INSERT（DuplicateKey）
        return repository.findByProviderAlias(cat.getProviderAlias())
            .flux()
            .collectList()
            .flatMap(list -> {
                if (list.isEmpty()) {
                    // 不存在：插入
                    ProviderCapabilityEntity entity = ProviderCapabilityEntity.builder()
                        .providerAlias(cat.getProviderAlias())
                        .domain(cat.getDomain())
                        .name(cat.getName())
                        .className(cat.getClassName())
                        .createdAt(LocalDateTime.now())
                        .updatedAt(LocalDateTime.now())
                        .build();
                    log.info("[ProviderCapabilitySync] 新增能力条目: alias={}, domain={}", cat.getProviderAlias(), cat.getDomain());
                    return repository.save(entity).then();
                }
                // 存在：字段变化才更新，无变化跳过
                ProviderCapabilityEntity existing = list.get(0);
                if (cat.getDomain().equals(existing.getDomain())
                        && cat.getName().equals(existing.getName())
                        && cat.getClassName().equals(existing.getClassName())) {
                    return Mono.empty();
                }
                existing.setDomain(cat.getDomain());
                existing.setName(cat.getName());
                existing.setClassName(cat.getClassName());
                existing.setUpdatedAt(LocalDateTime.now());
                log.info("[ProviderCapabilitySync] 更新能力条目: alias={}, domain={}", cat.getProviderAlias(), cat.getDomain());
                return repository.save(existing).then();
            });
    }
}
