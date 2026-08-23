package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

/**
 * 渠道上游 Token 池表实体
 * 对应数据库表: proxy_channel_tokens
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("proxy_channel_tokens")
public class ProxyChannelTokensEntity {

    @Id
    private Long id;

    @Column("channel_id")
    private Long channelId;

    /**
     * Token名称
     */
    @Column("name")
    private String name;

    /**
     * 上游渠道的 API Key（加密存储）
     */
    @Column("api_key_encrypted")
    private String apiKeyEncrypted;

    /**
     * 权重
     */
    @Column("weight")
    private Integer weight;

    /**
     * 当前使用量
     */
    @Column("current_usage")
    private Integer currentUsage;

    /**
     * 0=禁用, 1=启用
     */
    @Column("status")
    private Integer status;

    /**
     * 总请求数
     */
    @Column("total_requests")
    private Long totalRequests;

    /**
     * 成功次数
     */
    @Column("success_count")
    private Long successCount;

    /**
     * 错误次数
     */
    @Column("error_count")
    private Long errorCount;

    @Column("last_used_at")
    private LocalDateTime lastUsedAt;

    /**
     * 连续错误次数
     */
    @Column("consecutive_errors")
    private Integer consecutiveErrors;

    /**
     * 是否自动禁用
     */
    @Column("auto_disabled")
    private Integer autoDisabled;

    @Column("auto_disabled_at")
    private LocalDateTime autoDisabledAt;

    @Column("created_at")
    private LocalDateTime createdAt;

    @Column("updated_at")
    private LocalDateTime updatedAt;
}
