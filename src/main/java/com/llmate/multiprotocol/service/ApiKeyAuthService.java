package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.constant.CacheConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.repository.ProxyTokensRepository;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * API Key 认证服务
 */
@Service
@Log4j2
public class ApiKeyAuthService {

    private final ProxyTokensRepository proxyTokensRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public ApiKeyAuthService(ProxyTokensRepository proxyTokensRepository,
                             @Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate) {
        this.proxyTokensRepository = proxyTokensRepository;
        this.redisTemplate = redisTemplate;
    }

    /**
     * 验证 API Key
     *
     * @param apiKey API Key
     * @return 验证通过的 Token 实体
     */
    public Mono<ProxyTokensEntity> validateApiKey(String apiKey) {
        if (apiKey == null || apiKey.isEmpty()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.AUTH_INVALID_KEY, "缺少 API Key"));
        }

        // 1. 先从 Redis 缓存查询
        String cacheKey = CacheConstants.apiKeyKey(apiKey);
        return redisTemplate.opsForValue().get(cacheKey)
            .flatMap(cached -> {
                // 缓存命中，解析缓存数据
                try {
                    Long tokenId = Long.parseLong(cached);
                    return proxyTokensRepository.findById(tokenId);
                } catch (NumberFormatException e) {
                    return Mono.empty();
                }
            })
            .switchIfEmpty(
                // 2. 缓存未命中，查询数据库
                proxyTokensRepository.findByApiKey(apiKey)
                    .flatMap(this::validateTokenStatus)
                    .flatMap(token ->
                        // 3. 写入缓存
                        redisTemplate.opsForValue()
                            .set(cacheKey, String.valueOf(token.getId()), CacheConstants.TTL_API_KEY)
                            .thenReturn(token)
                    )
            )
            .switchIfEmpty(Mono.error(new LlmGatewayException(LlmErrorCode.AUTH_INVALID_KEY, "无效的 API Key")));
    }

    /**
     * 验证 Token 状态
     */
    private Mono<ProxyTokensEntity> validateTokenStatus(ProxyTokensEntity token) {
        // 检查是否启用
        if (token.getStatus() == null || token.getStatus() != SystemConstants.STATUS_ENABLED) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.AUTH_INVALID_KEY, "API Key 已被禁用"));
        }

        // 检查是否过期
        if (token.getExpiredAt() != null && token.getExpiredAt().isBefore(LocalDateTime.now())) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.AUTH_KEY_EXPIRED, token.getApiKey()));
        }

        return Mono.just(token);
    }

    /**
     * 清除 API Key 缓存
     */
    public Mono<Void> clearCache(String apiKey) {
        String cacheKey = CacheConstants.apiKeyKey(apiKey);
        return redisTemplate.delete(cacheKey).then();
    }
}
