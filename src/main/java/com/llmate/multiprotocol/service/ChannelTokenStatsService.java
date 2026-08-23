package com.llmate.multiprotocol.service;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;

/**
 * 渠道Token统计服务
 * 基于 Redis 的高性能实时统计，支持 least_used 负载均衡
 * 适配 WebFlux 响应式架构
 */
@Service
@Log4j2
public class ChannelTokenStatsService {

    /**
     * Redis key前缀：渠道Token统计（按日期）
     * 格式: channel:token:stats:{tokenId}:{date}
     */
    private static final String TOKEN_STATS_PREFIX = "channel:token:stats:";

    /**
     * Redis key前缀：渠道Token当前使用数（用于least_used负载均衡）
     * 格式: channel:token:usage:{tokenId}
     */
    private static final String TOKEN_USAGE_PREFIX = "channel:token:usage:";

    /**
     * 当前使用数过期时间（1小时）
     */
    private static final Duration USAGE_TTL = Duration.ofHours(1);

    /**
     * 统计数据过期时间（30天）
     */
    private static final Duration STATS_TTL = Duration.ofDays(30);

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public ChannelTokenStatsService(@Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Mono<Void> recordUsage(Long tokenId, boolean success) {
        if (tokenId == null) {
            return Mono.empty();
        }

        String date = java.time.LocalDate.now().toString();
        String key = TOKEN_STATS_PREFIX + tokenId + ":" + date;

        return redisTemplate.opsForHash().increment(key, "requests", 1)
            .then(redisTemplate.opsForHash().increment(key, success ? "success" : "error", 1))
            .then(redisTemplate.expire(key, STATS_TTL))
            .doOnError(e -> log.error("[ChannelTokenStats] 记录Token使用失败: tokenId={}, error={}", tokenId, e.getMessage()))
            .then();
    }

    public Mono<long[]> getRealtimeStats(Long tokenId) {
        if (tokenId == null) {
            return Mono.just(new long[]{0, 0, 0});
        }

        String date = java.time.LocalDate.now().toString();
        String key = TOKEN_STATS_PREFIX + tokenId + ":" + date;

        return redisTemplate.opsForHash().entries(key)
            .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
            .map(entries -> {
                long requests = parseLong(entries.get("requests"));
                long success = parseLong(entries.get("success"));
                long error = parseLong(entries.get("error"));
                return new long[]{requests, success, error};
            })
            .defaultIfEmpty(new long[]{0, 0, 0});
    }

    public Mono<Void> incrementCurrentUsage(Long tokenId) {
        if (tokenId == null) {
            return Mono.empty();
        }

        String usageKey = TOKEN_USAGE_PREFIX + tokenId;
        return redisTemplate.opsForValue().increment(usageKey)
            .then(redisTemplate.expire(usageKey, USAGE_TTL))
            .doOnError(e -> log.error("[ChannelTokenStats] 增加CurrentUsage失败: tokenId={}, error={}", tokenId, e.getMessage()))
            .then();
    }

    public Mono<Void> decrementCurrentUsage(Long tokenId) {
        if (tokenId == null) {
            return Mono.empty();
        }

        String usageKey = TOKEN_USAGE_PREFIX + tokenId;
        return redisTemplate.opsForValue().decrement(usageKey)
            .then(redisTemplate.expire(usageKey, USAGE_TTL))
            .doOnError(e -> log.error("[ChannelTokenStats] 减少CurrentUsage失败: tokenId={}, error={}", tokenId, e.getMessage()))
            .then();
    }

    public Mono<Void> resetCurrentUsage(Long tokenId) {
        if (tokenId == null) {
            return Mono.empty();
        }

        String usageKey = TOKEN_USAGE_PREFIX + tokenId;
        return redisTemplate.delete(usageKey)
            .doOnError(e -> log.error("[ChannelTokenStats] 重置CurrentUsage失败: tokenId={}, error={}", tokenId, e.getMessage()))
            .then();
    }

    private long parseLong(Object value) {
        if (value == null) return 0;
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
