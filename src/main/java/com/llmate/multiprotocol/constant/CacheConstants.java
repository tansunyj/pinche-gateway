package com.llmate.multiprotocol.constant;

import lombok.experimental.UtilityClass;

import java.time.Duration;

/**
 * 缓存相关常量
 * 缓存TTL、缓存Key前缀等
 */
@UtilityClass
public class CacheConstants {

    // ==================== 缓存TTL ====================
    public static final Duration TTL_BALANCE_CACHE = Duration.ofMinutes(10);
    public static final Duration TTL_RESERVE_CACHE = Duration.ofMinutes(10);
    public static final Duration TTL_API_KEY = Duration.ofHours(24);
    public static final Duration TTL_EXCHANGE_RATE = Duration.ofHours(1);
    public static final Duration TTL_CHANNEL = Duration.ofHours(12);  // 渠道数据缓存12小时
    public static final Duration TTL_MODEL_LIB = Duration.ofMinutes(5);
    public static final Duration TTL_PROVIDER_CONFIG = Duration.ofHours(12);  // Provider配置缓存12小时
    public static final Duration TTL_TIER_CONFIG = Duration.ofMinutes(5);     // 忙闲时时段配置缓存5分钟

    // ==================== Redis Key 前缀 ====================
    public static final String PREFIX_USER_BALANCE = "user:balance:";
    public static final String PREFIX_USER_RESERVED_HASH = "user:reserved:hash:";
    public static final String PREFIX_USER_RESERVED_BALANCE = "user:reserved:balance:";
    public static final String PREFIX_API_KEY = "user:apikey:";
    public static final String PREFIX_CHANNEL_CODE = "channel:code:";
    public static final String PREFIX_PROVIDER_CONFIG = "provider:config:";
    public static final String PREFIX_EXCHANGE_RATE = "exchange:";
    public static final String PREFIX_MODEL_LIB = "model:lib:";
    public static final String PREFIX_TIER_CONFIG = "tier:config:";

    // ==================== Redis Key 构建方法 ====================
    public static String userBalanceKey(Long userId) {
        return PREFIX_USER_BALANCE + userId;
    }

    public static String userReservedHashKey(Long userId) {
        return PREFIX_USER_RESERVED_HASH + userId;
    }

    public static String userReservedBalanceKey(Long userId, String requestId) {
        return PREFIX_USER_RESERVED_BALANCE + userId + ":" + requestId;
    }

    public static String apiKeyKey(String apiKey) {
        return PREFIX_API_KEY + apiKey;
    }

    public static String channelCodeKey(String channelCode) {
        return PREFIX_CHANNEL_CODE + channelCode;
    }

    public static String exchangeRateKey(String fromCurrency, String toCurrency) {
        return PREFIX_EXCHANGE_RATE + fromCurrency + ":" + toCurrency;
    }

    public static String modelLibKey(String modelId) {
        return PREFIX_MODEL_LIB + modelId;
    }

    /**
     * Provider 配置缓存Key
     */
    public static String providerConfigKey(String channelCode) {
        return PREFIX_PROVIDER_CONFIG + channelCode;
    }

    /**
     * 忙闲时时段配置缓存Key（挂载在 model_prices.id 即 priceId 上）
     */
    public static String tierConfigKey(Long priceId) {
        return PREFIX_TIER_CONFIG + priceId;
    }
}
