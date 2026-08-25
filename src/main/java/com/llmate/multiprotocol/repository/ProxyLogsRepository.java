package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.ProxyLogsEntity;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

/**
 * 合并日志表 Repository（原 proxy_logs ∪ proxy_request_logs）
 */
@Repository
public interface ProxyLogsRepository extends R2dbcRepository<ProxyLogsEntity, Long> {

    /**
     * 根据请求ID查询
     */
    Mono<ProxyLogsEntity> findByRequestId(String requestId);

    /**
     * 根据用户ID查询
     */
    Flux<ProxyLogsEntity> findByUserId(Long userId);

    /**
     * 根据用户ID和时间范围查询
     */
    Flux<ProxyLogsEntity> findByUserIdAndCreatedAtBetween(Long userId, LocalDateTime start, LocalDateTime end);

    /**
     * 根据模型ID和时间范围查询
     */
    Flux<ProxyLogsEntity> findByModelAndCreatedAtBetween(String model, LocalDateTime start, LocalDateTime end);

    /**
     * 结算回填（部分更新，非流式）。
     * 只更新结算拥有的列，绝不触碰审计/响应列 —— 那些列由 recordRequestLogComplete 等单独写，
     * 两条部分 UPDATE 列集不相交，无论先后执行都不会互相覆盖（照 proxy_request_logs 时代教训）。
     */
    @Modifying
    @Query("""
            UPDATE proxy_logs SET
                token_name = :tokenName,
                channel_name = :channelName,
                prompt_tokens = :promptTokens,
                completion_tokens = :completionTokens,
                quota_consumed = :quotaConsumed,
                latency_ms = :latencyMs,
                status = :status,
                error_msg = :errorMsg,
                is_thinking = :isThinking,
                price_markup = :priceMarkup,
                discount_ride_ids = :discountRideIds,
                original_quota = :originalQuota,
                billing_detail = :billingDetail,
                aborted = :aborted
            WHERE request_id = :requestId
            """)
    Mono<Integer> updateSettlementByRequestId(
            @Param("requestId") String requestId,
            @Param("tokenName") String tokenName,
            @Param("channelName") String channelName,
            @Param("promptTokens") Integer promptTokens,
            @Param("completionTokens") Integer completionTokens,
            @Param("quotaConsumed") Long quotaConsumed,
            @Param("latencyMs") Integer latencyMs,
            @Param("status") String status,
            @Param("errorMsg") String errorMsg,
            @Param("isThinking") Integer isThinking,
            @Param("priceMarkup") java.math.BigDecimal priceMarkup,
            @Param("discountRideIds") String discountRideIds,
            @Param("originalQuota") Long originalQuota,
            @Param("billingDetail") String billingDetail,
            @Param("aborted") Integer aborted);

    /**
     * 非流式完成回填（部分更新）：响应状态码 + tokens + quota_consumed + 结算 status/error。
     * 列集与 updateSettlementByRequestId 不相交（除 status/error_msg 由结算成功时先写、失败时此处兜底），
     * 避免与结算回填并发整行覆盖。
     * 注：response_headers / response_body / response_size_bytes 已删除（请求/应答数据仅入网关日志）。
     */
    @Modifying
    @Query("""
            UPDATE proxy_logs SET
                response_status = :responseStatus,
                latency_ms = :latencyMs,
                completed_at = :completedAt,
                prompt_tokens = :promptTokens,
                completion_tokens = :completionTokens,
                total_tokens = :totalTokens,
                quota_consumed = :quotaConsumed,
                status = :status,
                error_msg = :errorMsg
            WHERE request_id = :requestId
            """)
    Mono<Integer> updateCompletionByRequestId(
            @Param("requestId") String requestId,
            @Param("responseStatus") Integer responseStatus,
            @Param("latencyMs") Integer latencyMs,
            @Param("completedAt") LocalDateTime completedAt,
            @Param("promptTokens") Integer promptTokens,
            @Param("completionTokens") Integer completionTokens,
            @Param("totalTokens") Integer totalTokens,
            @Param("quotaConsumed") Long quotaConsumed,
            @Param("status") String status,
            @Param("errorMsg") String errorMsg);

    /**
     * 流式结算回填（部分更新）。
     * 只更新结算拥有的列；response_headers / response_body / response_size_bytes 已删除
     * （请求/应答数据仅入网关日志），不再涉及。
     */
    @Modifying
    @Query("""
            UPDATE proxy_logs SET
                response_status = :responseStatus,
                status = :status,
                error_msg = :errorMsg,
                latency_ms = :latencyMs,
                first_chunk_latency_ms = :firstChunkLatencyMs,
                completed_at = :completedAt,
                stream_chunks = :streamChunks,
                prompt_tokens = :promptTokens,
                completion_tokens = :completionTokens,
                total_tokens = :totalTokens,
                quota_consumed = :quotaConsumed,
                billing_detail = :billingDetail
            WHERE request_id = :requestId
            """)
    Mono<Integer> updateStreamCompletion(
            @Param("requestId") String requestId,
            @Param("responseStatus") Integer responseStatus,
            @Param("status") String status,
            @Param("latencyMs") Integer latencyMs,
            @Param("firstChunkLatencyMs") Integer firstChunkLatencyMs,
            @Param("completedAt") LocalDateTime completedAt,
            @Param("streamChunks") Integer streamChunks,
            @Param("promptTokens") Integer promptTokens,
            @Param("completionTokens") Integer completionTokens,
            @Param("totalTokens") Integer totalTokens,
            @Param("quotaConsumed") Long quotaConsumed,
            @Param("billingDetail") String billingDetail,
            @Param("errorMsg") String errorMsg);
}
