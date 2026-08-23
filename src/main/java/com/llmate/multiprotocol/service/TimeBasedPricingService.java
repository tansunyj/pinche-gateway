package com.llmate.multiprotocol.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.CacheConstants;
import com.llmate.multiprotocol.dto.BillingParams;
import com.llmate.multiprotocol.entity.ModelPriceTierEntity;
import com.llmate.multiprotocol.entity.PriceTierTimeRangeEntity;
import com.llmate.multiprotocol.repository.ModelPriceTierRepository;
import com.llmate.multiprotocol.repository.PriceTierTimeRangeRepository;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 忙闲时（分时段）定价服务 —— 价格解析阶段的「时段价格覆盖层」
 *
 * 命中规则（挂载在 model_prices 记录上，即 priceId）：
 * 1. 查该 priceId 所有 status=1 且 tier_type='time_of_day' 的 tier，汇总其时段明细
 * 2. 用当前时间（按各时段 timezone 转换的本地时间）匹配 [start,end) ∩ days_of_week
 * 3. 命中多条时取 priority 最高的一条
 * 4. 命中 → 用该时段 price_overrides 的绝对价覆盖 BillingParams 对应维度；未命中 → 基础价
 *
 * 与折扣体系正交：忙闲时在单价层面生效，priceMarkup 折扣在总费用层面生效，互不冲突。
 *
 * 未配置/未命中模型**零开销**：Redis 负缓存空快照（同 TTL），避免回源 DB；
 * 未命中不打印任何时段日志块。
 */
@Service
@Log4j2
public class TimeBasedPricingService {

    private static final String DEFAULT_TZ = "Asia/Shanghai";
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss");

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ModelPriceTierRepository modelPriceTierRepository;
    private final PriceTierTimeRangeRepository priceTierTimeRangeRepository;
    private final ObjectMapper objectMapper;

    public TimeBasedPricingService(
            @Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate,
            ModelPriceTierRepository modelPriceTierRepository,
            PriceTierTimeRangeRepository priceTierTimeRangeRepository,
            ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.modelPriceTierRepository = modelPriceTierRepository;
        this.priceTierTimeRangeRepository = priceTierTimeRangeRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * 应用忙闲时价格覆盖：命中时段用时段绝对价覆盖 BillingParams 对应维度，未命中/未配置原样返回。
     *
     * @param params  已解析好基础价的 BillingParams
     * @param priceId model_prices.id（忙闲时配置挂载点）
     * @param now     当前时间（服务器时区，匹配时按各时段 timezone 转换）
     * @return 可能被覆盖后的 BillingParams；任何异常（缓存/DB/解析）降级返回原 params，绝不阻断计费
     */
    public Mono<BillingParams> applyTimePricing(BillingParams params, Long priceId, ZonedDateTime now) {
        if (priceId == null || params == null) {
            return Mono.just(params);
        }
        String key = CacheConstants.tierConfigKey(priceId);
        return redisTemplate.opsForValue().get(key)
            .switchIfEmpty(loadAndCache(priceId, key))
            .flatMap(json -> applyOverrides(params, json, now))
            .onErrorResume(e -> {
                log.warn("[TimeBasedPricing] 忙闲时取价失败，降级基础价: priceId={}, err={}", priceId, e.getMessage());
                return Mono.just(params);
            });
    }

    /**
     * 缓存未命中时回源 DB 加载时段配置并写入缓存（含负缓存：无配置写空快照）
     */
    private Mono<String> loadAndCache(Long priceId, String key) {
        return modelPriceTierRepository.findByPriceIdAndStatus(priceId, 1)
            .filter(t -> "time_of_day".equals(t.getTierType()))
            .collectList()
            .flatMap(tiers -> {
                if (tiers.isEmpty()) {
                    return cacheAndReturn(key, "[]");
                }
                return Flux.fromIterable(tiers)
                    .flatMap(tier -> priceTierTimeRangeRepository.findByTierIdOrderByPriorityDesc(tier.getId())
                        .map(range -> toDto(range)))
                    .collectList()
                    .flatMap(list -> cacheAndReturn(key, serialize(list)));
            });
    }

    private Mono<String> cacheAndReturn(String key, String json) {
        return redisTemplate.opsForValue()
            .set(key, json, CacheConstants.TTL_TIER_CONFIG)
            .thenReturn(json);
    }

    private String serialize(List<TimeRangeCacheDTO> list) {
        try {
            return objectMapper.writeValueAsString(list);
        } catch (Exception e) {
            log.warn("[TimeBasedPricing] 序列化时段配置失败，按空配置处理: err={}", e.getMessage());
            return "[]";
        }
    }

    /**
     * 反序列化缓存 → 时段匹配 → 命中则覆盖价格维度
     */
    private Mono<BillingParams> applyOverrides(BillingParams params, String json, ZonedDateTime now) {
        List<TimeRangeCacheDTO> ranges;
        try {
            ranges = objectMapper.readValue(json, new TypeReference<List<TimeRangeCacheDTO>>() { });
        } catch (Exception e) {
            log.warn("[TimeBasedPricing] 解析时段配置失败，按未配置处理: err={}", e.getMessage());
            return Mono.just(params);
        }
        if (ranges == null || ranges.isEmpty()) {
            return Mono.just(params);
        }
        TimeRangeCacheDTO hit = match(ranges, now);
        if (hit == null) {
            return Mono.just(params);
        }
        if (hit.priceOverrides != null && !hit.priceOverrides.isEmpty()) {
            hit.priceOverrides.forEach((k, v) -> applyOverride(params, k, v));
        }
        log.info("[TimeBasedPricing] 命中忙闲时段: priceId 时段={} 开始={} 结束={} 时区={} 覆盖维度={}",
            hit.tierName, hit.timeStart, hit.timeEnd, hit.timezone, hit.priceOverrides != null ? hit.priceOverrides.keySet() : "无");
        return Mono.just(params);
    }

    /**
     * 多条命中时取 priority 最高的一条（priority null 视为 0）
     */
    private TimeRangeCacheDTO match(List<TimeRangeCacheDTO> ranges, ZonedDateTime now) {
        TimeRangeCacheDTO best = null;
        for (TimeRangeCacheDTO r : ranges) {
            if (matches(r, now)) {
                int rp = r.priority != null ? r.priority : 0;
                int bp = best != null && best.priority != null ? best.priority : 0;
                if (best == null || rp > bp) {
                    best = r;
                }
            }
        }
        return best;
    }

    /**
     * 单条时段命中判断：时区本地时间 ∈ [start, end)（跨天 start>end 时 ∈ [start,∞) ∪ [0,end)）且星期匹配
     */
    private boolean matches(TimeRangeCacheDTO r, ZonedDateTime now) {
        String tz = (r.timezone == null || r.timezone.isBlank()) ? DEFAULT_TZ : r.timezone;
        ZonedDateTime zoned;
        try {
            zoned = now.withZoneSameInstant(ZoneId.of(tz));
        } catch (Exception e) {
            zoned = now.withZoneSameInstant(ZoneId.of(DEFAULT_TZ));
        }
        // 星期过滤：days_of_week 为空 = 每天生效；1=周一 … 7=周日
        if (r.daysOfWeek != null && !r.daysOfWeek.isBlank()) {
            int today = zoned.getDayOfWeek().getValue();
            boolean dayOk = java.util.Arrays.stream(r.daysOfWeek.split(","))
                .map(String::trim)
                .anyMatch(s -> Integer.toString(today).equals(s));
            if (!dayOk) {
                return false;
            }
        }
        LocalTime t = zoned.toLocalTime();
        LocalTime start = LocalTime.parse(r.timeStart);
        LocalTime end = LocalTime.parse(r.timeEnd);
        if (start.isAfter(end)) {
            // 跨天（如 22:00–06:00）
            return !t.isBefore(start) || t.isBefore(end);
        }
        return !t.isBefore(start) && t.isBefore(end);
    }

    /**
     * price_overrides 的 key → BillingParams 维度映射（对齐 model_prices.billing_params）
     */
    private void applyOverride(BillingParams params, String key, BigDecimal value) {
        if (value == null) {
            return;
        }
        switch (key) {
            case "input_per_1m" -> params.setInputPer1m(value);
            case "output_per_1m" -> params.setOutputPer1m(value);
            case "thinking_output_per_m", "reasoning_per_1m" -> params.setReasoningPer1m(value);
            case "cache_hit_per_1m" -> params.setCacheHitPer1m(value);
            case "text_tokens_per_1m" -> params.setTextTokensPer1m(value);
            case "image_tokens_per_1m" -> params.setImageTokensPer1m(value);
            case "vector_tokens_per_1m" -> params.setVectorTokensPer1m(value);
            case "characters_per_1k" -> params.setCharactersPer1k(value);
            case "input_text_per_1m" -> params.setInputTextPer1m(value);
            case "input_image_per_1m" -> params.setInputImagePer1m(value);
            case "output_text_per_1m" -> params.setOutputTextPer1m(value);
            case "output_image_per_1m" -> params.setOutputImagePer1m(value);
            case "image_per_call" -> params.setImagePerCall(value);
            case "flat_price" -> params.setFlatPrice(value);
            case "video_per_second_720p" -> params.setVideoPerSecond720p(value);
            case "video_per_second_1080p" -> params.setVideoPerSecond1080p(value);
            case "audio_per_second" -> params.setAudioPerSecond(value);
            default -> applyVideoTokenOverride(params, key, value); // 480p_noInput / 4k_withInput 等
        }
    }

    private void applyVideoTokenOverride(BillingParams params, String key, BigDecimal value) {
        if (params.getVideoTokenPrices() == null) {
            params.setVideoTokenPrices(new HashMap<>());
        }
        params.getVideoTokenPrices().put(key, value);
    }

    private TimeRangeCacheDTO toDto(PriceTierTimeRangeEntity r) {
        TimeRangeCacheDTO dto = new TimeRangeCacheDTO();
        dto.setTierName(r.getTierName());
        dto.setTimeStart(r.getTimeStart() != null ? r.getTimeStart().format(TIME_FMT) : "00:00:00");
        dto.setTimeEnd(r.getTimeEnd() != null ? r.getTimeEnd().format(TIME_FMT) : "23:59:59");
        dto.setTimezone(r.getTimezone());
        dto.setDaysOfWeek(r.getDaysOfWeek());
        dto.setPriority(r.getPriority());
        if (r.getPriceOverrides() != null && !r.getPriceOverrides().isBlank()) {
            try {
                dto.setPriceOverrides(objectMapper.readValue(
                    r.getPriceOverrides(), new TypeReference<Map<String, BigDecimal>>() { }));
            } catch (Exception e) {
                log.warn("[TimeBasedPricing] 解析 price_overrides 失败，该时段不覆盖价格: tierId={}, err={}", r.getId(), e.getMessage());
            }
        }
        return dto;
    }

    /**
     * 缓存快照 DTO（tier:config:{priceId} 的 JSON 元素）
     */
    @Data
    public static class TimeRangeCacheDTO {
        private String tierName;
        private String timeStart;
        private String timeEnd;
        private String timezone;
        private String daysOfWeek;
        private Integer priority;
        private Map<String, BigDecimal> priceOverrides;
    }
}
