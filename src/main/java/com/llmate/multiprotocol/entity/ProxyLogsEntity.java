package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * 合并日志表实体（结算 + 请求/响应审计，原 proxy_logs ∪ proxy_request_logs）
 * 对应数据库表: proxy_logs
 *
 * 数据库重构后 proxy_request_logs 已并入本表（DROP 原表）：
 * - 结算字段：quota_consumed / latency_ms / status / price_markup / billing_detail 等
 * - 审计字段：request_method ~ completed_at（原 proxy_request_logs 全列）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("proxy_logs")
public class ProxyLogsEntity {

    @Id
    private Long id;

    @Column("user_id")
    private Long userId;

    @Column("token_id")
    private Long tokenId;

    @Column("token_name")
    private String tokenName;

    @Column("channel_id")
    private Long channelId;

    @Column("request_id")
    private String requestId;

    @Column("channel_name")
    private String channelName;

    @Column("model")
    private String model;

    @Column("prompt_tokens")
    private Integer promptTokens;

    @Column("completion_tokens")
    private Integer completionTokens;

    @Column("quota_consumed")
    private Long quotaConsumed;

    @Column("latency_ms")
    private Integer latencyMs;

    @Column("status")
    private String status;

    @Column("error_msg")
    private String errorMsg;

    /**
     * 是否思考模式
     */
    @Column("is_thinking")
    private Integer isThinking;

    /**
     * 价格倍率
     */
    @Column("price_markup")
    private BigDecimal priceMarkup;

    /**
     * 实际命中折扣的车次ID(逗号分隔,候选全保留,§5.4)
     */
    @Column("discount_ride_ids")
    private String discountRideIds;

    /**
     * 折扣前原价额度(反推 round(quota_consumed/price_markup),§5.4)
     */
    @Column("original_quota")
    private Long originalQuota;

    @Column("created_at")
    private LocalDateTime createdAt;

    /**
     * 是否中断
     */
    @Column("aborted")
    private Integer aborted;

    /**
     * 请求方法
     */
    @Column("request_method")
    private String requestMethod;

    /**
     * 请求路径
     */
    @Column("request_path")
    private String requestPath;

    /**
     * 响应状态码
     */
    @Column("response_status")
    private Integer responseStatus;

    /**
     * 是否流式
     */
    @Column("is_stream")
    private Integer isStream;

    /**
     * 流式块数
     */
    @Column("stream_chunks")
    private Integer streamChunks;

    /**
     * 首块延迟（毫秒）
     */
    @Column("first_chunk_latency_ms")
    private Integer firstChunkLatencyMs;

    @Column("total_tokens")
    private Integer totalTokens;

    /**
     * 客户端IP
     */
    @Column("client_ip")
    private String clientIp;

    /**
     * 用户代理
     */
    @Column("user_agent")
    private String userAgent;

    /**
     * 完成时间
     */
    @Column("completed_at")
    private LocalDateTime completedAt;

    /**
     * 计费多行明细（tokens 消耗 + 各维度费用，\n 拼接）
     */
    @Column("billing_detail")
    private String billingDetail;
}
