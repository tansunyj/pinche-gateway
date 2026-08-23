package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.constant.BusinessConstants;
import com.llmate.multiprotocol.constant.CacheConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.entity.UserUsersEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.repository.UserUsersRepository;
import com.llmate.multiprotocol.util.LogBox;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 用户余额服务
 * 使用 Redis Lua 脚本保证预占/扣减的原子性
 */
@Service
@Log4j2
public class UserBalanceService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final UserUsersRepository userUsersRepository;

    public UserBalanceService(@Qualifier("reactiveStringRedisTemplate") ReactiveRedisTemplate<String, String> redisTemplate,
                              UserUsersRepository userUsersRepository) {
        this.redisTemplate = redisTemplate;
        this.userUsersRepository = userUsersRepository;
    }

    /**
     * 查询用户当前余额（额度单位）
     *
     * 优先读 Redis 余额缓存，缓存未命中（过期/不存在）时自动从 MySQL 同步并回填。
     * 供 {@code GET /v1/user/balance} 使用（CC-Switch 用量查询「通用模板」读取）。
     *
     * @param userId 用户ID
     * @return 当前余额
     */
    public Mono<Long> getBalance(Long userId) {
        String balanceKey = CacheConstants.userBalanceKey(userId);
        // 与 reserveBalance 同款模式：switchIfEmpty 只作用于源头（缓存读取），
        // syncBalanceFromDb 返回非空余额字符串，不会被误判为空结果。
        return redisTemplate.opsForValue().get(balanceKey)
            .switchIfEmpty(syncBalanceFromDb(userId, balanceKey))
            .map(Long::parseLong);
    }

    /**
     * 查询用户当前余额（元，xxx.yyy 格式）
     *
     * 库内余额为「额度」（quota），不能直接返回给用户。按比例换算为元：
     * 1 元 = {@link BusinessConstants#QUOTA_PER_USD}（100000）额度，即 元 = 额度 ÷ 100000。
     * 保留 3 位小数（HALF_UP 四舍五入）——注意不可用计费专用的 QUOTA_ROUNDING_MODE(UP)，
     * 那是扣费防遗漏用的，展示余额应标准四舍五入。
     *
     * @param userId 用户ID
     * @return 余额（元，scale 3）
     */
    public Mono<BigDecimal> getBalanceInYuan(Long userId) {
        return getBalance(userId)
            .map(balance -> BigDecimal.valueOf(balance)
                .divide(BusinessConstants.QUOTA_PER_USD)
                .setScale(3, RoundingMode.HALF_UP));
    }

    /**
     * 预占余额
     *
     * @param userId 用户ID
     * @param requestId 请求ID
     * @param amount 预占额度
     * @return 剩余可用余额
     */
    public Mono<Long> reserveBalance(Long userId, String requestId, long amount) {
        String balanceKey = CacheConstants.userBalanceKey(userId);
        String reservedHashKey = CacheConstants.userReservedHashKey(userId);
        String reservedKey = CacheConstants.userReservedBalanceKey(userId, requestId);

        // 先检查 Redis 余额是否存在，不存在则从 MySQL 同步
        return redisTemplate.opsForValue().get(balanceKey)
            .switchIfEmpty(syncBalanceFromDb(userId, balanceKey))
            .flatMap(balanceStr -> {
                long balance = Long.parseLong(balanceStr);

                // 获取已预占额度
                return redisTemplate.opsForHash().get(reservedHashKey, "total")
                    .defaultIfEmpty("0")
                    .flatMap(reservedStr -> {
                        long reserved = Long.parseLong(reservedStr.toString());
                        long available = balance - reserved;

                        if (available < amount) {
                            // 预占被拒绝：累计已预占 + 本次预占将超过余额，余额保持不变。
                            LogBox.logBalanceReserve(userId, requestId, balance, reserved, amount, available, false);
                            return Mono.error(new LlmGatewayException(
                                LlmErrorCode.BALANCE_INSUFFICIENT, amount, available));
                        }

                        // 预占成功：余额不变，仅把本次额度计入累计已预占
                        return redisTemplate.opsForHash()
                            .increment(reservedHashKey, "total", amount)
                            .then(redisTemplate.opsForHash()
                                .put(reservedHashKey, requestId, String.valueOf(amount)))
                            .then(redisTemplate.expire(reservedHashKey, CacheConstants.TTL_RESERVE_CACHE))
                            .then(redisTemplate.opsForValue()
                                .set(reservedKey, String.valueOf(amount), CacheConstants.TTL_RESERVE_CACHE))
                            .doOnSuccess(v -> {
                                // 预占成功日志：余额不变，仅锁定本次额度
                                LogBox.logBalanceReserve(userId, requestId, balance, reserved, amount, available, true);
                            })
                            .thenReturn(available - amount);
                    });
            });
    }

    /**
     * 实际扣减余额（纯扣减，不处理预占释放）
     * 预占的释放由调用方在扣减前/后单独调用 releaseReservedBalance 处理。
     *
     * @param userId 用户ID
     * @param requestId 请求ID（用于扣减日志，与预占日志保持一致，便于排查）
     * @param actualAmount 实际扣减额度
     * @return 扣减后的余额
     */
    public Mono<Long> deductBalance(Long userId, String requestId, long actualAmount) {
        String balanceKey = CacheConstants.userBalanceKey(userId);

        return redisTemplate.opsForValue().get(balanceKey)
            .switchIfEmpty(syncBalanceFromDb(userId, balanceKey))
            .flatMap(balanceStr -> {
                long balance = Long.parseLong(balanceStr);

                // 原子扣减：Redis increment 直接 balance = balance - actualAmount，
                // 避免"读出-计算-写回"在并发结算时互相覆盖丢更新。
                return redisTemplate.opsForValue().increment(balanceKey, -actualAmount)
                    .flatMap(newBalance -> {
                        if (newBalance < 0) {
                            // 余额不足：回滚本次扣减（把扣掉的补回去）
                            return redisTemplate.opsForValue().increment(balanceKey, actualAmount)
                                .then(Mono.error(new LlmGatewayException(
                                    LlmErrorCode.BALANCE_INSUFFICIENT, actualAmount, balance)));
                        }

                        return syncBalanceToDb(userId, requestId, balance, actualAmount)
                            .doOnSuccess(v -> {
                                // 实际扣减日志（使用真实 requestId，不再硬编码 "deduct"）
                                LogBox.logBalanceDeduct(userId, requestId, balance, actualAmount, newBalance, 0, 0);
                            })
                            .thenReturn(newBalance);
                    });
            });
    }

    /**
     * 释放预占余额（请求失败或无需扣费时）
     */
    public Mono<Void> releaseReservedBalance(Long userId, String requestId) {
        String reservedHashKey = CacheConstants.userReservedHashKey(userId);
        String reservedKey = CacheConstants.userReservedBalanceKey(userId, requestId);

        return redisTemplate.opsForHash().get(reservedHashKey, requestId)
            .flatMap(amount -> {
                long reserved = Long.parseLong(amount.toString());
                return redisTemplate.opsForHash()
                    .increment(reservedHashKey, "total", -reserved)
                    .then(redisTemplate.opsForHash().remove(reservedHashKey, requestId))
                    .then(redisTemplate.delete(reservedKey))
                    .doOnSuccess(v -> {
                        // 输出带方框的余额释放日志
                        LogBox.logBalanceRelease(userId, requestId, reserved);
                    });
            })
            .then();
    }

    /**
     * 从数据库同步余额到 Redis
     */
    private Mono<String> syncBalanceFromDb(Long userId, String balanceKey) {
        return userUsersRepository.findById(userId)
            .switchIfEmpty(Mono.error(new LlmGatewayException(
                LlmErrorCode.AUTH_USER_DISABLED, "User not found: " + userId)))
            .flatMap(user -> {
                // pt_users.status 为枚举字符串 ACTIVE/DISABLED，非旧库 Integer 0/1
                if (user.getStatus() == null || !SystemConstants.USER_STATUS_ACTIVE.equals(user.getStatus())) {
                    return Mono.error(new LlmGatewayException(
                        LlmErrorCode.AUTH_USER_DISABLED));
                }
                long balance = user.getBalance() != null ? user.getBalance() : 0;
                return redisTemplate.opsForValue()
                    .set(balanceKey, String.valueOf(balance), CacheConstants.TTL_BALANCE_CACHE)
                    .thenReturn(String.valueOf(balance));
            });
    }

    /**
     * 同步余额到数据库（异步、原子相对扣减）
     *
     * 用 `UPDATE pt_users SET balance = balance - :amount` 而非"读出-改写"，避免并发结算
     * 互相覆盖丢更新；且与 Redis 侧 atomic increment 使用同一扣减量，两侧保持一致。
     *
     * @param oldBalance   扣减前余额（Redis 中扣减前的值，仅用于日志）
     * @param deductAmount 本次扣减量
     */
    private Mono<Void> syncBalanceToDb(Long userId, String requestId, long oldBalance, long deductAmount) {
        return Mono.fromCallable(() -> {
            userUsersRepository.decrementBalance(userId, deductAmount).block();
            // 打印余额扣减量 + 相对更新 SQL（balance = balance - amount，原子防并发丢更新）
            LogBox.logBalanceDbUpdate(userId, requestId, deductAmount, oldBalance, oldBalance - deductAmount);
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /**
     * 计算额度（USD -> quota）
     * 汇率不参与计费，仅用于显示转换
     */
    public long calculateQuota(BigDecimal costInUsd) {
        BigDecimal quotaDecimal = costInUsd
            .multiply(BusinessConstants.QUOTA_PER_USD);
        return quotaDecimal.setScale(0, BusinessConstants.QUOTA_ROUNDING_MODE).longValue();
    }

    /**
     * 将 USD 转换为额度
     */
    public Mono<Long> convertUsdToQuota(BigDecimal costInUsd) {
        return Mono.just(calculateQuota(costInUsd));
    }
}
