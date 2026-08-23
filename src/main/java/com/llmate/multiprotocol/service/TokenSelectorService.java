package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.entity.ProxyChannelTokensEntity;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 渠道 Token 选择器
 * 支持多种负载均衡策略：round_robin / random / weighted / least_used
 *
 * 职责：
 * 1. 根据渠道配置的 token_lb_strategy 选择最优 Token
 * 2. 选择后调用 incrementCurrentUsage() 增加使用计数
 * 3. 请求完成后调用 decrementCurrentUsage() 减少使用计数
 *
 * 与源项目 ChannelResolverServiceImpl.pickUpstreamKey() 等价，适配 WebFlux 响应式架构。
 */
@Service
@Log4j2
public class TokenSelectorService {

    private static final String DEFAULT_STRATEGY = "round_robin";

    private final ChannelTokenStatsService channelTokenStatsService;
    private final Random random = new Random();

    // 轮询计数器：channelId -> AtomicInteger
    private final Map<Long, AtomicInteger> roundRobinCounters = new ConcurrentHashMap<>();

    public TokenSelectorService(ChannelTokenStatsService channelTokenStatsService) {
        this.channelTokenStatsService = channelTokenStatsService;
    }

    /**
     * 从 Token 列表中选择一个最优 Token
     *
     * @param channelId   渠道ID
     * @param tokens      可用 Token 列表
     * @param strategy    负载均衡策略（round_robin / random / weighted / least_used）
     * @return 选中的 Token
     */
    public Mono<ProxyChannelTokensEntity> selectToken(Long channelId, List<ProxyChannelTokensEntity> tokens, String strategy) {
        if (tokens == null || tokens.isEmpty()) {
            return Mono.empty();
        }

        if (tokens.size() == 1) {
            ProxyChannelTokensEntity token = tokens.get(0);
            return incrementUsage(token.getId()).thenReturn(token);
        }

        String actualStrategy = (strategy == null || strategy.isBlank()) ? DEFAULT_STRATEGY : strategy;

        ProxyChannelTokensEntity selected = switch (actualStrategy) {
            case "random" -> pickRandom(tokens);
            case "weighted" -> pickWeighted(tokens);
            case "least_used" -> pickLeastUsed(tokens);
            default -> pickRoundRobin(channelId, tokens);
        };

        // 增加当前使用计数（用于 least_used 负载均衡）
        return incrementUsage(selected.getId())
            .thenReturn(selected)
            .doOnNext(t -> log.info("[TokenSelector] 选择Token: channelId={}, tokenId={}, name={}, strategy={}",
                channelId, t.getId(), t.getName(), actualStrategy));
    }

    /**
     * 报告 Token 使用完成
     * 在请求结束后调用，减少当前使用计数并记录使用统计
     *
     * @param tokenId  Token ID
     * @param success  是否成功
     */
    public Mono<Void> reportTokenUsage(Long tokenId, boolean success) {
        if (tokenId == null) {
            return Mono.empty();
        }

        log.debug("[TokenSelector] 报告Token使用: tokenId={}, success={}", tokenId, success);

        // 1. 记录到 Redis 统计服务
        return channelTokenStatsService.recordUsage(tokenId, success)
            // 2. 减少当前使用计数
            .then(channelTokenStatsService.decrementCurrentUsage(tokenId))
            .doOnError(e -> log.error("[TokenSelector] 报告Token使用失败: tokenId={}, error={}", tokenId, e.getMessage()))
            .onErrorResume(e -> Mono.empty()); // 忽略错误，不影响主流程
    }

    // ==================== 私有选择策略 ====================

    /**
     * 随机选择
     */
    private ProxyChannelTokensEntity pickRandom(List<ProxyChannelTokensEntity> tokens) {
        return tokens.get(random.nextInt(tokens.size()));
    }

    /**
     * 按权重加权随机选择
     */
    private ProxyChannelTokensEntity pickWeighted(List<ProxyChannelTokensEntity> tokens) {
        int totalWeight = tokens.stream()
            .mapToInt(t -> t.getWeight() != null ? t.getWeight() : 1)
            .sum();

        int randomWeight = random.nextInt(totalWeight) + 1;
        int currentWeight = 0;

        for (ProxyChannelTokensEntity token : tokens) {
            currentWeight += token.getWeight() != null ? token.getWeight() : 1;
            if (randomWeight <= currentWeight) {
                return token;
            }
        }

        return tokens.get(tokens.size() - 1);
    }

    /**
     * 选择当前使用数最少的 Token（least_used 策略）
     * 优先使用 Redis 中的实时 usage 数据，回退到数据库 current_usage
     */
    private ProxyChannelTokensEntity pickLeastUsed(List<ProxyChannelTokensEntity> tokens) {
        return tokens.stream()
            .min((a, b) -> {
                // 获取实时使用数（异步转同步，统计场景可接受）
                long usageA = getCurrentUsageSync(a);
                long usageB = getCurrentUsageSync(b);
                return Long.compare(usageA, usageB);
            })
            .orElse(tokens.get(0));
    }

    /**
     * 轮询选择
     */
    private ProxyChannelTokensEntity pickRoundRobin(Long channelId, List<ProxyChannelTokensEntity> tokens) {
        AtomicInteger counter = roundRobinCounters.computeIfAbsent(
            channelId, k -> new AtomicInteger(0));
        int index = counter.getAndIncrement() % tokens.size();
        return tokens.get(index);
    }

    // ==================== 工具方法 ====================

    /**
     * 同步获取 Token 当前使用数
     * 优先 Redis 实时数据，回退到数据库 current_usage 字段
     */
    private long getCurrentUsageSync(ProxyChannelTokensEntity token) {
        try {
            long[] stats = channelTokenStatsService.getRealtimeStats(token.getId()).block();
            if (stats != null && stats.length > 0 && stats[0] > 0) {
                return stats[0]; // Redis 中的实时请求数
            }
        } catch (Exception e) {
            log.warn("[TokenSelector] 获取Redis实时统计失败, tokenId={}, 回退到数据库: {}",
                token.getId(), e.getMessage());
        }
        // 回退到数据库 current_usage
        return token.getCurrentUsage() != null ? token.getCurrentUsage() : 0;
    }

    /**
     * 增加 Token 当前使用计数
     */
    private Mono<Void> incrementUsage(Long tokenId) {
        if (tokenId == null) {
            return Mono.empty();
        }
        return channelTokenStatsService.incrementCurrentUsage(tokenId)
            .doOnError(e -> log.warn("[TokenSelector] 增加Token使用计数失败: tokenId={}, error={}",
                tokenId, e.getMessage()))
            .onErrorResume(e -> Mono.empty());
    }
}
