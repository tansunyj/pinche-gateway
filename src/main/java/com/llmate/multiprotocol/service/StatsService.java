package com.llmate.multiprotocol.service;

import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 统计服务
 * 基于 Redis 的高性能实时统计，支持多维度监控指标
 * 适配 WebFlux 响应式架构
 *
 * 数据结构：
 * - Hash：计数类指标（requests, tokens, quota, latency 等）
 * - HyperLogLog：去重计数（DAU, TAU, CAU, MAU 等）
 * - Sorted Set：排行榜（模型排行、Token排行）
 */
@Service
@Log4j2
public class StatsService {

    private static final long KEY_TTL_SECONDS = 30L * 24 * 3600; // 30天过期
    private static final long KEY_TTL_USER_SECONDS = 7L * 24 * 3600; // 用户数据保留7天
    // 按月键 TTL(§5.3)：需整月持续有效,单独设 90 天,避免月末前过期
    private static final long KEY_TTL_MONTHLY_SECONDS = 90L * 24 * 3600;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final ZoneId ZONE_SHANGHAI = ZoneId.of("Asia/Shanghai");

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public StatsService(@Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public Mono<Void> recordRequest(RequestStatsData data) {
        return Mono.fromRunnable(() -> {
            try {
                int totalTokens = data.promptTokens() + data.completionTokens();

                // 1. 全局统计
                updateGlobalStats(data, totalTokens);

                // 2. 用户维度统计
                if (data.userId() != null) {
                    updateUserStats(data, totalTokens);
                }

                // 3. 渠道统计
                updateChannelStats(data, totalTokens);

                // 4. Token统计
                updateTokenStats(data, totalTokens);

                // 5. 模型统计
                if (data.model() != null && !data.model().isEmpty()) {
                    updateModelStats(data, totalTokens);
                    updateCompositeStats(data);
                }

                // 6. 全局小时趋势
                updateHourlyStats(data);

                // 7. 排行榜
                updateRankings(data, totalTokens);

                // 8. 延迟分桶
                if (data.latencyMs() > 0) {
                    updateLatencyBucket(data);
                }

                // 9. 车次维度(命中折扣才归属)
                updateRideStats(data);

                // 10. 折扣维度(全局)
                updateDiscountStats(data);

                // 11. 按月聚合(与各维度同构,§5.3)
                updateMonthlyStats(data);

            } catch (Exception e) {
                log.error("[StatsService] 记录统计失败: {}", e.getMessage());
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    public Mono<Void> recordRequestType(Long userId, String requestType) {
        return Mono.fromRunnable(() -> {
            try {
                String date = LocalDate.now(ZONE_SHANGHAI).format(DATE_FORMATTER);
                String key = "stats:user:" + userId + ":" + date + ":requests:" + requestType;
                redisTemplate.opsForValue().increment(key).subscribe();
                redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();
            } catch (Exception e) {
                log.error("[StatsService] 记录请求类型失败: {}", e.getMessage());
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    // ==================== 私有更新方法 ====================

    private void updateGlobalStats(RequestStatsData data, int totalTokens) {
        String key = "stats:global:" + data.date();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "prompt_tokens", data.promptTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "completion_tokens", data.completionTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "total_tokens", totalTokens).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(key, data.isSuccess() ? "success" : "error", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_count", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_sum", data.latencyMs()).subscribe();

        updateMinMaxLatency(key, data.latencyMs());
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 活跃用户统计 (HyperLogLog)
        if (data.userId() != null) {
            String dauKey = "stats:dau:" + data.date();
            redisTemplate.opsForHyperLogLog().add(dauKey, "u:" + data.userId()).subscribe();
            redisTemplate.expire(dauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

            String requestersKey = "stats:user:requesters:" + data.date();
            redisTemplate.opsForHyperLogLog().add(requestersKey, "u:" + data.userId()).subscribe();
            redisTemplate.expire(requestersKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
        }

        // Token活跃统计
        String tauKey = "stats:tau:" + data.date();
        redisTemplate.opsForHyperLogLog().add(tauKey, "tk:" + data.tokenId()).subscribe();
        redisTemplate.expire(tauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 渠道活跃统计
        String cauKey = "stats:cau:" + data.date();
        redisTemplate.opsForHyperLogLog().add(cauKey, "ch:" + data.channelId()).subscribe();
        redisTemplate.expire(cauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
    }

    private void updateUserStats(RequestStatsData data, int totalTokens) {
        // 用户全局统计
        String globalKey = "stats:user:" + data.userId() + ":" + data.date() + ":global";

        redisTemplate.opsForHash().increment(globalKey, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(globalKey, "prompt_tokens", data.promptTokens()).subscribe();
        redisTemplate.opsForHash().increment(globalKey, "completion_tokens", data.completionTokens()).subscribe();
        redisTemplate.opsForHash().increment(globalKey, "total_tokens", totalTokens).subscribe();
        redisTemplate.opsForHash().increment(globalKey, "quota_consumed", data.quota()).subscribe();
        // 节省额度(§5.1)：命中车次折扣时累计,供 user/stats 展示"已为你节省"（metric_definitions.saved_quota）
        if (data.discountRate() != null && data.discountRate().compareTo(BigDecimal.ONE) < 0 && data.savedQuota() > 0) {
            redisTemplate.opsForHash().increment(globalKey, "saved_quota", data.savedQuota()).subscribe();
        }
        redisTemplate.opsForHash().increment(globalKey, data.isSuccess() ? "success_count" : "error_count", 1).subscribe();
        redisTemplate.opsForHash().increment(globalKey, "latency_count", 1).subscribe();
        redisTemplate.opsForHash().increment(globalKey, "latency_sum", data.latencyMs()).subscribe();

        updateMinMaxLatency(globalKey, data.latencyMs());
        redisTemplate.expire(globalKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();

        // 日活Token (HyperLogLog)
        String userTokensKey = "stats:user:" + data.userId() + ":" + data.date() + ":tokens";
        redisTemplate.opsForHyperLogLog().add(userTokensKey, "tk:" + data.tokenId()).subscribe();
        redisTemplate.expire(userTokensKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();

        // 日活模型
        if (data.model() != null) {
            String userModelsKey = "stats:user:" + data.userId() + ":" + data.date() + ":models";
            redisTemplate.opsForHyperLogLog().add(userModelsKey, "md:" + data.model()).subscribe();
            redisTemplate.expire(userModelsKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();
        }

        // 实时QPS计数 (按秒)
        long currentSecond = System.currentTimeMillis() / 1000;
        String qpsKey = "stats:user:" + data.userId() + ":qps:" + currentSecond;
        redisTemplate.opsForValue().increment(qpsKey).subscribe();
        redisTemplate.expire(qpsKey, Duration.ofSeconds(5)).subscribe();

        // 30分钟级统计
        int hour = Integer.parseInt(data.hour());
        int halfHourIndex = hour * 2 + (data.minute() < 30 ? 0 : 1);
        String halfHourKey = "stats:user:" + data.userId() + ":" + data.date() + ":halfhour:" + halfHourIndex;
        redisTemplate.opsForHash().increment(halfHourKey, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(halfHourKey, "total_tokens", totalTokens).subscribe();
        redisTemplate.opsForHash().increment(halfHourKey, "quota_consumed", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(halfHourKey, data.isSuccess() ? "success_count" : "error_count", 1).subscribe();
        redisTemplate.expire(halfHourKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();

        // 同时保留小时级统计
        String hourKey = "stats:user:" + data.userId() + ":" + data.date() + ":hour:" + data.hour();
        redisTemplate.opsForHash().increment(hourKey, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(hourKey, "total_tokens", totalTokens).subscribe();
        redisTemplate.opsForHash().increment(hourKey, "quota_consumed", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(hourKey, data.isSuccess() ? "success_count" : "error_count", 1).subscribe();
        redisTemplate.expire(hourKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();

        // 用户模型统计
        if (data.model() != null) {
            String modelKey = "stats:user:" + data.userId() + ":" + data.date() + ":model:" + data.model();
            redisTemplate.opsForHash().increment(modelKey, "requests", 1).subscribe();
            redisTemplate.opsForHash().increment(modelKey, "prompt_tokens", data.promptTokens()).subscribe();
            redisTemplate.opsForHash().increment(modelKey, "completion_tokens", data.completionTokens()).subscribe();
            redisTemplate.opsForHash().increment(modelKey, "total_tokens", totalTokens).subscribe();
            redisTemplate.opsForHash().increment(modelKey, "quota_consumed", data.quota()).subscribe();
            redisTemplate.expire(modelKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();

            // 用户模型排行 (Sorted Set)
            String rankKey = "stats:user:" + data.userId() + ":" + data.date() + ":rank:model";
            redisTemplate.opsForZSet().incrementScore(rankKey, data.model(), totalTokens).subscribe();
            redisTemplate.expire(rankKey, Duration.ofSeconds(KEY_TTL_USER_SECONDS)).subscribe();
        }
    }

    private void updateChannelStats(RequestStatsData data, int totalTokens) {
        String key = "stats:channel:" + data.channelId() + ":" + data.date();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "prompt_tokens", data.promptTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "completion_tokens", data.completionTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(key, data.isSuccess() ? "success" : "error", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_count", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_sum", data.latencyMs()).subscribe();

        updateMinMaxLatency(key, data.latencyMs());

        if (data.channelName() != null) {
            redisTemplate.opsForHash().put(key, "channel_name", data.channelName()).subscribe();
        }
        if (data.channelType() != null) {
            redisTemplate.opsForHash().put(key, "channel_type", data.channelType()).subscribe();
        }

        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 渠道Token活跃统计
        String channelTauKey = "stats:channel:" + data.channelId() + ":tau:" + data.date();
        redisTemplate.opsForHyperLogLog().add(channelTauKey, "tk:" + data.tokenId()).subscribe();
        redisTemplate.expire(channelTauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 渠道模型活跃统计
        if (data.model() != null) {
            String channelMauKey = "stats:channel:" + data.channelId() + ":mau:" + data.date();
            redisTemplate.opsForHyperLogLog().add(channelMauKey, "md:" + data.model()).subscribe();
            redisTemplate.expire(channelMauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
        }
    }

    private void updateTokenStats(RequestStatsData data, int totalTokens) {
        String key = "stats:token:" + data.tokenId() + ":" + data.date();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "prompt_tokens", data.promptTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "completion_tokens", data.completionTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_count", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_sum", data.latencyMs()).subscribe();

        if (data.tokenName() != null) {
            redisTemplate.opsForHash().put(key, "token_name", data.tokenName()).subscribe();
        }
        if (data.userId() != null) {
            redisTemplate.opsForHash().put(key, "user_id", String.valueOf(data.userId())).subscribe();
        }

        redisTemplate.opsForHash().increment(key, data.isSuccess() ? "success" : "error", 1).subscribe();
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // Token活跃统计
        String activeKey = "stats:tokens:active:" + data.date();
        redisTemplate.opsForHyperLogLog().add(activeKey, "tk:" + data.tokenId()).subscribe();
        redisTemplate.expire(activeKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
    }

    private void updateModelStats(RequestStatsData data, int totalTokens) {
        String key = "stats:model:" + data.model() + ":" + data.date();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "prompt_tokens", data.promptTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "completion_tokens", data.completionTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_count", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_sum", data.latencyMs()).subscribe();

        updateMinMaxLatency(key, data.latencyMs());
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 模型Token和渠道活跃统计
        String modelTauKey = "stats:model:" + data.model() + ":tau:" + data.date();
        String modelCauKey = "stats:model:" + data.model() + ":cau:" + data.date();
        redisTemplate.opsForHyperLogLog().add(modelTauKey, "tk:" + data.tokenId()).subscribe();
        redisTemplate.opsForHyperLogLog().add(modelCauKey, "ch:" + data.channelId()).subscribe();
        redisTemplate.expire(modelTauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
        redisTemplate.expire(modelCauKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 全局模型排行
        redisTemplate.opsForZSet().incrementScore("stats:rank:model:quota", data.model(), data.quota()).subscribe();
        redisTemplate.opsForZSet().incrementScore("stats:rank:model:requests", data.model(), 1).subscribe();
    }

    private void updateCompositeStats(RequestStatsData data) {
        String key = "stats:composite:ch:" + data.channelId() + ":md:" + data.model() + ":" + data.date();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(key, "prompt_tokens", data.promptTokens()).subscribe();
        redisTemplate.opsForHash().increment(key, "completion_tokens", data.completionTokens()).subscribe();
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
    }

    private void updateHourlyStats(RequestStatsData data) {
        String key = "stats:hourly:" + data.date() + ":" + data.hour();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_count", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "latency_sum", data.latencyMs()).subscribe();
        redisTemplate.opsForHash().increment(key, data.isSuccess() ? "success" : "error", 1).subscribe();
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
    }

    private void updateRankings(RequestStatsData data, int totalTokens) {
        // Token排行
        String tokenQuotaKey = "stats:rank:token:quota:" + data.date();
        String tokenReqKey = "stats:rank:token:requests:" + data.date();
        redisTemplate.opsForZSet().incrementScore(tokenQuotaKey, "tk:" + data.tokenId(), data.quota()).subscribe();
        redisTemplate.opsForZSet().incrementScore(tokenReqKey, "tk:" + data.tokenId(), 1).subscribe();
        redisTemplate.expire(tokenQuotaKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
        redisTemplate.expire(tokenReqKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();

        // 模型排行
        if (data.model() != null) {
            String modelQuotaKey = "stats:rank:model:quota:" + data.date();
            String modelReqKey = "stats:rank:model:requests:" + data.date();
            redisTemplate.opsForZSet().incrementScore(modelQuotaKey, data.model(), data.quota()).subscribe();
            redisTemplate.opsForZSet().incrementScore(modelReqKey, data.model(), 1).subscribe();
            redisTemplate.expire(modelQuotaKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
            redisTemplate.expire(modelReqKey, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
        }
    }

    private void updateLatencyBucket(RequestStatsData data) {
        String bucket;
        long latencyMs = data.latencyMs();

        if (latencyMs < 100) bucket = "latency_bucket_0_100";
        else if (latencyMs < 300) bucket = "latency_bucket_100_300";
        else if (latencyMs < 500) bucket = "latency_bucket_300_500";
        else if (latencyMs < 1000) bucket = "latency_bucket_500_1000";
        else if (latencyMs < 2000) bucket = "latency_bucket_1000_2000";
        else if (latencyMs < 5000) bucket = "latency_bucket_2000_5000";
        else bucket = "latency_bucket_5000_plus";

        String key = "stats:global:" + data.date();
        redisTemplate.opsForHash().increment(key, bucket, 1).subscribe();
    }

    private void updateMinMaxLatency(String key, long latencyMs) {
        // 使用 get + compare + put 模式（非原子，但统计场景可接受）
        redisTemplate.opsForHash().get(key, "latency_min")
            .defaultIfEmpty("0")
            .flatMap(minStr -> {
                long currentMin = parseLongSafe(minStr, 0);
                if (currentMin == 0 || latencyMs < currentMin) {
                    return redisTemplate.opsForHash().put(key, "latency_min", String.valueOf(latencyMs));
                }
                return Mono.just(false);
            })
            .subscribe();

        redisTemplate.opsForHash().get(key, "latency_max")
            .defaultIfEmpty("0")
            .flatMap(maxStr -> {
                long currentMax = parseLongSafe(maxStr, 0);
                if (currentMax == 0 || latencyMs > currentMax) {
                    return redisTemplate.opsForHash().put(key, "latency_max", String.valueOf(latencyMs));
                }
                return Mono.just(false);
            })
            .subscribe();
    }

    // ==================== 车次 / 折扣 / 按月 维度(§5.1 §5.3) ====================

    /**
     * 车次维度：stats:ride:{rideId}:{date}。
     * rideId = 实际生效车次(最低折扣率,单值),一单只计入一个车次,避免跨车次重复。
     * 字段名即 metric_definitions 目录名(ride_requests / ride_saved_quota),
     * 便于同步器 generic 映射;discount_rate 用 sum+count 累积(同步器算均值)。
     */
    private void updateRideStats(RequestStatsData data) {
        if (data.rideId() == null) {
            return; // 无命中车次,不入车次维度
        }
        String key = "stats:ride:" + data.rideId() + ":" + data.date();

        redisTemplate.opsForHash().increment(key, "ride_requests", 1).subscribe();
        redisTemplate.opsForHash().increment(key, "quota", data.quota()).subscribe();

        if (data.discountRate() != null && data.discountRate().compareTo(BigDecimal.ONE) < 0) {
            redisTemplate.opsForHash().increment(key, "discounted_requests", 1).subscribe();
            redisTemplate.opsForHash().increment(key, "ride_saved_quota", data.savedQuota()).subscribe();
            redisTemplate.opsForHash().increment(key, "discount_rate_sum", data.discountRate().doubleValue()).subscribe();
            updateMinRate(key, data.discountRate());
        }
        if (data.rideName() != null) {
            redisTemplate.opsForHash().put(key, "ride_name", data.rideName()).subscribe();
        }
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
    }

    /**
     * 折扣维度(全局)：stats:discount:{date}。
     * requests=全部请求, discounted_requests=享折扣请求, saved_quota=全平台优惠额度,
     * discount_rate_min=当日最低生效折扣率(代表最优惠)。
     */
    private void updateDiscountStats(RequestStatsData data) {
        String key = "stats:discount:" + data.date();

        redisTemplate.opsForHash().increment(key, "requests", 1).subscribe();
        if (data.discountRate() != null && data.discountRate().compareTo(BigDecimal.ONE) < 0) {
            redisTemplate.opsForHash().increment(key, "discounted_requests", 1).subscribe();
            redisTemplate.opsForHash().increment(key, "saved_quota", data.savedQuota()).subscribe();
            redisTemplate.opsForHash().increment(key, "discount_rate_sum", data.discountRate().doubleValue()).subscribe();
            updateMinRate(key, data.discountRate());
        }
        redisTemplate.expire(key, Duration.ofSeconds(KEY_TTL_SECONDS)).subscribe();
    }

    /**
     * 按月聚合(§5.3)：网关直接 INCR stats:monthly:{yyyy-MM}:{dim}:{key},与按天同构。
     * 同步器落库 dim_type='monthly', stat_date=当月首日。无需单独 rollup 任务。
     */
    private void updateMonthlyStats(RequestStatsData data) {
        String month = data.date().length() >= 7 ? data.date().substring(0, 7) : data.date();

        // global
        incrMonthly(month, "global", "global", "requests", 1);
        incrMonthly(month, "global", "global", "quota", data.quota());
        incrMonthly(month, "global", "global", "prompt_tokens", data.promptTokens());
        incrMonthly(month, "global", "global", "completion_tokens", data.completionTokens());
        incrMonthly(month, "global", "global", data.isSuccess() ? "success" : "error", 1);

        // channel
        if (data.channelId() != null) {
            String ch = "ch:" + data.channelId();
            incrMonthly(month, "channel", ch, "requests", 1);
            incrMonthly(month, "channel", ch, "quota", data.quota());
            incrMonthly(month, "channel", ch, "prompt_tokens", data.promptTokens());
            incrMonthly(month, "channel", ch, "completion_tokens", data.completionTokens());
            incrMonthly(month, "channel", ch, data.isSuccess() ? "success" : "error", 1);
        }

        // token
        if (data.tokenId() != null) {
            String tk = "tk:" + data.tokenId();
            incrMonthly(month, "token", tk, "requests", 1);
            incrMonthly(month, "token", tk, "quota", data.quota());
            incrMonthly(month, "token", tk, "prompt_tokens", data.promptTokens());
            incrMonthly(month, "token", tk, "completion_tokens", data.completionTokens());
        }

        // model
        if (data.model() != null && !data.model().isEmpty()) {
            String md = "md:" + data.model();
            incrMonthly(month, "model", md, "requests", 1);
            incrMonthly(month, "model", md, "quota", data.quota());
            incrMonthly(month, "model", md, "prompt_tokens", data.promptTokens());
            incrMonthly(month, "model", md, "completion_tokens", data.completionTokens());
        }

        // user
        if (data.userId() != null) {
            String u = "user:" + data.userId();
            incrMonthly(month, "user", u, "requests", 1);
            incrMonthly(month, "user", u, "quota", data.quota());
            if (data.discountRate() != null && data.discountRate().compareTo(BigDecimal.ONE) < 0 && data.savedQuota() > 0) {
                incrMonthly(month, "user", u, "saved_quota", data.savedQuota());
            }
        }

        // composite (ch × md),key 形如 ch:5:md:deepseek/deepseek-v4-flash
        if (data.channelId() != null && data.model() != null && !data.model().isEmpty()) {
            String ck = "ch:" + data.channelId() + ":md:" + data.model();
            incrMonthly(month, "composite", ck, "requests", 1);
            incrMonthly(month, "composite", ck, "quota", data.quota());
            incrMonthly(month, "composite", ck, "prompt_tokens", data.promptTokens());
            incrMonthly(month, "composite", ck, "completion_tokens", data.completionTokens());
        }

        // ride
        if (data.rideId() != null) {
            String rk = "ride:" + data.rideId();
            incrMonthly(month, "ride", rk, "ride_requests", 1);
            incrMonthly(month, "ride", rk, "quota", data.quota());
            if (data.discountRate() != null && data.discountRate().compareTo(BigDecimal.ONE) < 0) {
                incrMonthly(month, "ride", rk, "discounted_requests", 1);
                incrMonthly(month, "ride", rk, "ride_saved_quota", data.savedQuota());
                incrMonthlyDouble(month, "ride", rk, "discount_rate_sum", data.discountRate().doubleValue());
            }
        }

        // discount(全局)
        incrMonthly(month, "discount", "discount", "requests", 1);
        if (data.discountRate() != null && data.discountRate().compareTo(BigDecimal.ONE) < 0) {
            incrMonthly(month, "discount", "discount", "discounted_requests", 1);
            incrMonthly(month, "discount", "discount", "saved_quota", data.savedQuota());
            incrMonthlyDouble(month, "discount", "discount", "discount_rate_sum", data.discountRate().doubleValue());
        }
    }

    /** 按月 Hash 计数 INCR(整数),90 天 TTL */
    private void incrMonthly(String month, String dim, String key, String field, long incr) {
        String mkey = "stats:monthly:" + month + ":" + dim + ":" + key;
        redisTemplate.opsForHash().increment(mkey, field, incr).subscribe();
        redisTemplate.expire(mkey, Duration.ofSeconds(KEY_TTL_MONTHLY_SECONDS)).subscribe();
    }

    /** 按月 Hash 计数 INCR(浮点,折扣率累计) */
    private void incrMonthlyDouble(String month, String dim, String key, String field, double incr) {
        String mkey = "stats:monthly:" + month + ":" + dim + ":" + key;
        redisTemplate.opsForHash().increment(mkey, field, incr).subscribe();
        redisTemplate.expire(mkey, Duration.ofSeconds(KEY_TTL_MONTHLY_SECONDS)).subscribe();
    }

    /** 记录当日最低生效折扣率(get+compare+put,非原子但统计可接受) */
    private void updateMinRate(String key, BigDecimal rate) {
        redisTemplate.opsForHash().get(key, "discount_rate_min")
            .defaultIfEmpty("")
            .flatMap(cur -> {
                String curStr = cur == null ? "" : cur.toString();
                if (curStr.isEmpty()) {
                    return redisTemplate.opsForHash().put(key, "discount_rate_min", rate.toPlainString());
                }
                try {
                    if (rate.doubleValue() < Double.parseDouble(curStr)) {
                        return redisTemplate.opsForHash().put(key, "discount_rate_min", rate.toPlainString());
                    }
                } catch (NumberFormatException ignored) {
                    // 脏值直接覆盖
                    return redisTemplate.opsForHash().put(key, "discount_rate_min", rate.toPlainString());
                }
                return Mono.just(false);
            })
            .subscribe();
    }

    // ==================== 查询方法 ====================

    public Mono<Map<String, Object>> getDailyOverview(String date) {
        String key = "stats:global:" + date;

        return redisTemplate.opsForHash().entries(key)
            .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
            .flatMap(entries -> {
                if (entries.isEmpty()) {
                    return Mono.just(Collections.<String, Object>emptyMap());
                }

                Map<String, Object> result = new HashMap<>();
                long latencyCount = parseLongSafe(entries.get("latency_count"), 1);
                long latencySum = parseLongSafe(entries.get("latency_sum"), 0);

                result.put("date", date);
                result.put("requests", parseLongSafe(entries.get("requests"), 0));
                result.put("promptTokens", parseLongSafe(entries.get("prompt_tokens"), 0));
                result.put("completionTokens", parseLongSafe(entries.get("completion_tokens"), 0));
                result.put("totalTokens", parseLongSafe(entries.get("total_tokens"), 0));
                result.put("quota", parseLongSafe(entries.get("quota"), 0));
                result.put("successCount", parseLongSafe(entries.get("success"), 0));
                result.put("errorCount", parseLongSafe(entries.get("error"), 0));
                result.put("latencyAvgMs", latencyCount > 0 ? latencySum / latencyCount : 0);
                result.put("latencyMinMs", parseLongSafe(entries.get("latency_min"), 0));
                result.put("latencyMaxMs", parseLongSafe(entries.get("latency_max"), 0));

                // HyperLogLog 去重计数
                return Mono.zip(
                    redisTemplate.opsForHyperLogLog().size("stats:dau:" + date).defaultIfEmpty(0L),
                    redisTemplate.opsForHyperLogLog().size("stats:tau:" + date).defaultIfEmpty(0L),
                    redisTemplate.opsForHyperLogLog().size("stats:cau:" + date).defaultIfEmpty(0L)
                ).map(tuple -> {
                    result.put("uniqueUsers", tuple.getT1());
                    result.put("uniqueTokens", tuple.getT2());
                    result.put("uniqueChannels", tuple.getT3());
                    return result;
                });
            });
    }

    public Mono<List<Map<String, Object>>> getChannelStatsList(String date) {
        String pattern = "stats:channel:*:" + date;

        return redisTemplate.keys(pattern)
            .collectList()
            .flatMapMany(Flux::fromIterable)
            .flatMap(key -> {
                String[] parts = key.split(":");
                if (parts.length < 4) return Mono.empty();

                Long channelId = parseLongSafe(parts[2], 0L);
                return redisTemplate.opsForHash().entries(key)
                    .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                    .filter(entries -> !entries.isEmpty() && entries.containsKey("requests"))
                    .flatMap(entries -> {
                        long latencyCount = parseLongSafe(entries.get("latency_count"), 1);

                        return redisTemplate.opsForHyperLogLog()
                            .size("stats:channel:" + channelId + ":tau:" + date)
                            .defaultIfEmpty(0L)
                            .map(uniqueTokens -> {
                                Map<String, Object> result = new HashMap<>();
                                result.put("channelId", channelId);
                                result.put("channelName", entries.getOrDefault("channel_name", "渠道" + channelId));
                                result.put("channelType", entries.getOrDefault("channel_type", ""));
                                result.put("requests", parseLongSafe(entries.get("requests"), 0L));
                                result.put("quota", parseLongSafe(entries.get("quota"), 0L));
                                result.put("promptTokens", parseLongSafe(entries.get("prompt_tokens"), 0L));
                                result.put("completionTokens", parseLongSafe(entries.get("completion_tokens"), 0L));
                                result.put("latencyAvgMs", latencyCount > 0
                                    ? parseLongSafe(entries.get("latency_sum"), 0L) / latencyCount : 0L);
                                result.put("uniqueTokens", uniqueTokens);
                                result.put("online", entries.getOrDefault("online", 1));
                                return result;
                            });
                    });
            })
            .collectList()
            .map(list -> {
                list.sort((a, b) -> Long.compare((Long) b.get("quota"), (Long) a.get("quota")));
                return list;
            });
    }

    public Mono<List<Map<String, Object>>> getTokenStatsList(String date) {
        String pattern = "stats:token:*:" + date;

        return redisTemplate.keys(pattern)
            .collectList()
            .flatMapMany(Flux::fromIterable)
            .flatMap(key -> {
                String[] parts = key.split(":");
                if (parts.length < 4) return Mono.empty();

                Long tokenId = parseLongSafe(parts[2], 0L);
                return redisTemplate.opsForHash().entries(key)
                    .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                    .filter(entries -> !entries.isEmpty() && entries.containsKey("requests"))
                    .map(entries -> {
                        long latencyCount = parseLongSafe(entries.get("latency_count"), 1);

                        Map<String, Object> result = new HashMap<>();
                        result.put("tokenId", tokenId);
                        result.put("tokenName", entries.getOrDefault("token_name", "Token" + tokenId));
                        result.put("userId", parseLongSafe(entries.get("user_id"), 0L));
                        result.put("requests", parseLongSafe(entries.get("requests"), 0L));
                        result.put("quota", parseLongSafe(entries.get("quota"), 0L));
                        result.put("promptTokens", parseLongSafe(entries.get("prompt_tokens"), 0L));
                        result.put("completionTokens", parseLongSafe(entries.get("completion_tokens"), 0L));
                        result.put("latencyAvgMs", latencyCount > 0
                            ? parseLongSafe(entries.get("latency_sum"), 0L) / latencyCount : 0L);
                        return result;
                    });
            })
            .collectList()
            .map(list -> {
                list.sort((a, b) -> Long.compare((Long) b.get("quota"), (Long) a.get("quota")));
                return list;
            });
    }

    public Mono<List<Map<String, Object>>> getTopModels(String date, int n) {
        String key = "stats:rank:model:quota:" + date;

        return redisTemplate.opsForZSet().reverseRangeWithScores(key, Range.closed(0L, (long) (n - 1)))
            .switchIfEmpty(Flux.empty())
            .flatMap(tuple -> {
                String model = tuple.getValue();
                double quota = tuple.getScore() != null ? tuple.getScore() : 0;

                return redisTemplate.opsForHash().entries("stats:model:" + model + ":" + date)
                    .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                    .flatMap(dailyStats -> {
                        long latencyCount = parseLongSafe(dailyStats.get("latency_count"), 1);

                        return redisTemplate.opsForHyperLogLog()
                            .size("stats:model:" + model + ":tau:" + date)
                            .defaultIfEmpty(0L)
                            .map(uniqueTokens -> {
                                Map<String, Object> result = new HashMap<>();
                                result.put("model", model);
                                result.put("quota", (long) quota);
                                result.put("requests", parseLongSafe(dailyStats.get("requests"), 0L));
                                result.put("promptTokens", parseLongSafe(dailyStats.get("prompt_tokens"), 0L));
                                result.put("completionTokens", parseLongSafe(dailyStats.get("completion_tokens"), 0L));
                                result.put("latencyAvgMs", latencyCount > 0
                                    ? parseLongSafe(dailyStats.get("latency_sum"), 0L) / latencyCount : 0L);
                                result.put("uniqueTokens", uniqueTokens);
                                return result;
                            });
                    });
            })
            .collectList();
    }

    public Mono<List<Map<String, Object>>> getTopTokens(String date, int n) {
        String key = "stats:rank:token:quota:" + date;

        return redisTemplate.opsForZSet().reverseRangeWithScores(key, Range.closed(0L, (long) (n - 1)))
            .switchIfEmpty(Flux.empty())
            .flatMap(tuple -> {
                String tokenKey = tuple.getValue();
                if (tokenKey == null || !tokenKey.startsWith("tk:")) return Mono.empty();

                Long tokenId = parseLongSafe(tokenKey.substring(3), 0L);
                double quota = tuple.getScore() != null ? tuple.getScore() : 0;

                return redisTemplate.opsForHash().entries("stats:token:" + tokenId + ":" + date)
                    .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                    .map(stats -> {
                        Map<String, Object> result = new HashMap<>();
                        result.put("tokenId", tokenId);
                        result.put("tokenName", stats.getOrDefault("token_name", "Token" + tokenId));
                        result.put("userId", parseLongSafe(stats.get("user_id"), 0L));
                        result.put("quota", (long) quota);
                        result.put("requests", parseLongSafe(stats.get("requests"), 0L));
                        result.put("promptTokens", parseLongSafe(stats.get("prompt_tokens"), 0L));
                        result.put("completionTokens", parseLongSafe(stats.get("completion_tokens"), 0L));
                        return result;
                    });
            })
            .collectList();
    }

    public Mono<List<Map<String, Object>>> getHourlyTrend(String date) {
        List<Mono<Map<String, Object>>> hourMonos = new ArrayList<>();

        for (int hour = 0; hour < 24; hour++) {
            String hourStr = String.format("%02d", hour);
            String key = "stats:hourly:" + date + ":" + hourStr;

            final int finalHour = hour;
            Mono<Map<String, Object>> hourMono = redisTemplate.opsForHash().entries(key)
                .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                .map(entries -> {
                    long latencyCount = parseLongSafe(entries.get("latency_count"), 1);

                    Map<String, Object> result = new HashMap<>();
                    result.put("hour", finalHour);
                    result.put("requests", parseLongSafe(entries.get("requests"), 0L));
                    result.put("quota", parseLongSafe(entries.get("quota"), 0L));
                    result.put("latencyAvgMs", latencyCount > 0
                        ? parseLongSafe(entries.get("latency_sum"), 0L) / latencyCount : 0L);
                    return result;
                });

            hourMonos.add(hourMono);
        }

        return Flux.concat(hourMonos).collectList();
    }

    public Mono<Map<String, Integer>> getLatencyDistribution(String date) {
        String key = "stats:global:" + date;

        return redisTemplate.opsForHash().entries(key)
            .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
            .map(entries -> {
                Map<String, Integer> result = new HashMap<>();
                result.put("0-100ms", parseIntSafe(entries.get("latency_bucket_0_100"), 0));
                result.put("100-300ms", parseIntSafe(entries.get("latency_bucket_100_300"), 0));
                result.put("300-500ms", parseIntSafe(entries.get("latency_bucket_300_500"), 0));
                result.put("500-1000ms", parseIntSafe(entries.get("latency_bucket_500_1000"), 0));
                result.put("1000-2000ms", parseIntSafe(entries.get("latency_bucket_1000_2000"), 0));
                result.put("2000-5000ms", parseIntSafe(entries.get("latency_bucket_2000_5000"), 0));
                result.put("5000ms+", parseIntSafe(entries.get("latency_bucket_5000_plus"), 0));
                return result;
            });
    }

    public Mono<Map<String, Object>> getUserDailyOverview(Long userId, String date) {
        String key = "stats:user:" + userId + ":" + date + ":global";

        return redisTemplate.opsForHash().entries(key)
            .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
            .flatMap(stats -> {
                if (stats.isEmpty() || !stats.containsKey("requests")) {
                    Map<String, Object> emptyResult = new HashMap<>();
                    emptyResult.put("userId", userId);
                    emptyResult.put("date", date);
                    emptyResult.put("requests", 0L);
                    emptyResult.put("promptTokens", 0L);
                    emptyResult.put("completionTokens", 0L);
                    emptyResult.put("totalTokens", 0L);
                    emptyResult.put("quotaConsumed", 0L);
                    emptyResult.put("successCount", 0L);
                    emptyResult.put("errorCount", 0L);
                    emptyResult.put("errorRate", 0.0);
                    emptyResult.put("latencyAvgMs", 0L);
                    emptyResult.put("latencyMinMs", 0L);
                    emptyResult.put("latencyMaxMs", 0L);
                    emptyResult.put("uniqueTokens", 0L);
                    emptyResult.put("uniqueModels", 0L);
                    return Mono.just(emptyResult);
                }

                long latencyCount = parseLongSafe(stats.get("latency_count"), 1);
                long latencySum = parseLongSafe(stats.get("latency_sum"), 0);
                long requests = parseLongSafe(stats.get("requests"), 0);
                long errorCount = parseLongSafe(stats.get("error_count"), 0);

                return Mono.zip(
                    redisTemplate.opsForHyperLogLog()
                        .size("stats:user:" + userId + ":" + date + ":tokens").defaultIfEmpty(0L),
                    redisTemplate.opsForHyperLogLog()
                        .size("stats:user:" + userId + ":" + date + ":models").defaultIfEmpty(0L)
                ).map(tuple -> {
                    Map<String, Object> result = new HashMap<>();
                    result.put("userId", userId);
                    result.put("date", date);
                    result.put("requests", requests);
                    result.put("promptTokens", parseLongSafe(stats.get("prompt_tokens"), 0L));
                    result.put("completionTokens", parseLongSafe(stats.get("completion_tokens"), 0L));
                    result.put("totalTokens", parseLongSafe(stats.get("total_tokens"), 0L));
                    result.put("quotaConsumed", parseLongSafe(stats.get("quota_consumed"), 0L));
                    result.put("successCount", parseLongSafe(stats.get("success_count"), 0L));
                    result.put("errorCount", errorCount);
                    result.put("errorRate", requests > 0 ? (errorCount * 100.0 / requests) : 0.0);
                    result.put("latencyAvgMs", latencyCount > 0 ? latencySum / latencyCount : 0L);
                    result.put("latencyMinMs", parseLongSafe(stats.get("latency_min"), 0L));
                    result.put("latencyMaxMs", parseLongSafe(stats.get("latency_max"), 0L));
                    result.put("uniqueTokens", tuple.getT1());
                    result.put("uniqueModels", tuple.getT2());
                    return result;
                });
            });
    }

    public Mono<List<Map<String, Object>>> getUserHourlyTrend(Long userId, String date) {
        List<Mono<Map<String, Object>>> halfHourMonos = new ArrayList<>();

        for (int index = 0; index < 48; index++) {
            String halfHourKey = "stats:user:" + userId + ":" + date + ":halfhour:" + index;
            int hour = index / 2;
            int half = index % 2;

            final int finalIndex = index;
            final int finalHour = hour;
            final int finalHalf = half;

            Mono<Map<String, Object>> mono = redisTemplate.opsForHash().entries(halfHourKey)
                .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                .map(stats -> {
                    Map<String, Object> result = new HashMap<>();
                    result.put("index", finalIndex);
                    result.put("hour", finalHour);
                    result.put("half", finalHalf);
                    result.put("requests", parseLongSafe(stats.get("requests"), 0L));
                    result.put("totalTokens", parseLongSafe(stats.get("total_tokens"), 0L));
                    result.put("quotaConsumed", parseLongSafe(stats.get("quota_consumed"), 0L));
                    result.put("errorCount", parseLongSafe(stats.get("error_count"), 0L));
                    return result;
                });

            halfHourMonos.add(mono);
        }

        return Flux.concat(halfHourMonos).collectList();
    }

    public Mono<List<Map<String, Object>>> getUserModelDistribution(Long userId, String date, int limit) {
        String rankKey = "stats:user:" + userId + ":" + date + ":rank:model";

        return redisTemplate.opsForZSet().reverseRangeWithScores(rankKey, Range.closed(0L, (long) (limit - 1)))
            .switchIfEmpty(Flux.empty())
            .collectList()
            .flatMap(ranking -> {
                if (ranking.isEmpty()) {
                    return Mono.just(Collections.<Map<String, Object>>emptyList());
                }

                long totalTokens = 0;
                for (ZSetOperations.TypedTuple<String> tuple : ranking) {
                    totalTokens += tuple.getScore() != null ? tuple.getScore().longValue() : 0;
                }

                final long finalTotalTokens = totalTokens;
                List<Mono<Map<String, Object>>> monos = new ArrayList<>();

                for (ZSetOperations.TypedTuple<String> tuple : ranking) {
                    String model = tuple.getValue();
                    long tokens = tuple.getScore() != null ? tuple.getScore().longValue() : 0;

                    Mono<Map<String, Object>> mono = redisTemplate.opsForHash()
                        .entries("stats:user:" + userId + ":" + date + ":model:" + model)
                        .collectMap(e -> e.getKey().toString(), Map.Entry::getValue)
                        .map(stats -> {
                            Map<String, Object> item = new HashMap<>();
                            item.put("name", model);
                            item.put("tokens", tokens);
                            item.put("requests", parseLongSafe(stats.get("requests"), 0L));
                            item.put("quotaConsumed", parseLongSafe(stats.get("quota_consumed"), 0L));
                            double percentage = finalTotalTokens > 0
                                ? Math.round((tokens * 100.0 / finalTotalTokens) * 10) / 10.0 : 0;
                            item.put("value", percentage);
                            return item;
                        });

                    monos.add(mono);
                }

                return Flux.concat(monos).collectList();
            });
    }

    public Mono<Integer> getUserRealtimeQps(Long userId) {
        long currentSecond = System.currentTimeMillis() / 1000;

        List<Mono<String>> monos = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            String key = "stats:user:" + userId + ":qps:" + (currentSecond - i);
            monos.add(redisTemplate.opsForValue().get(key).defaultIfEmpty("0"));
        }

        return Flux.concat(monos)
            .map(Long::parseLong)
            .reduce(0L, Long::sum)
            .map(total -> (int) Math.round(total / 3.0));
    }

    // ==================== 工具方法 ====================

    private long parseLongSafe(Object value, long defaultValue) {
        if (value == null) return defaultValue;
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private int parseIntSafe(Object value, int defaultValue) {
        if (value == null) return defaultValue;
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 请求统计数据
     */
    public record RequestStatsData(
            String date,
            String hour,
            int minute,
            Long channelId,
            String channelName,
            String channelType,
            Long tokenId,
            String tokenName,
            Long userId,
            String model,
            int promptTokens,
            int completionTokens,
            long quota,
            long latencyMs,
            boolean isSuccess,
            // ============ 车次折扣归属(§5.1) ============
            List<Long> rideIds,          // 命中的折扣车次(全部候选)
            Long rideId,                 // 实际生效车次(最低折扣率,单值)
            String rideName,             // 生效车次名称(meta)
            BigDecimal discountRate,     // 实际生效折扣率(未命中=1.0)
            long originalQuota,          // 折前原价(反推 round(quota/discountRate))
            long savedQuota              // 优惠额度 = originalQuota - quota
    ) {}
}
