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
 * 渠道-模型关联表实体
 * 对应数据库表: proxy_channel_models
 *
 * 注意：定价参数（input_price, output_price, video_per_second_720p等）
 * 存储在 model_prices 表的 billing_params 字段（JSON格式）中
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("proxy_channel_models")
public class ProxyChannelModelsEntity {

    @Id
    private Long id;

    @Column("channel_id")
    private Long channelId;

    /**
     * 模型ID，如 qwen-max, claude-3-opus
     */
    @Column("model_id")
    private String modelId;

    /**
     * 该模型在此渠道绑定的 Adapter 能力快照（JSON，String 存储）
     * {"provider_alias":"openai_bearer","domain":"chat","class_name":"<Adapter全限定名>"}
     * NULL=未绑定（网关启动自动回填，见 ProviderRegistry）
     */
    @Column("provider_capability")
    private String providerCapability;

    /**
     * 优先级
     */
    @Column("priority")
    private Integer priority;

    /**
     * 价格倍率
     */
    @Column("markup")
    private BigDecimal markup;

    /**
     * 0=禁用, 1=启用
     */
    @Column("is_enabled")
    private Integer isEnabled;

    /**
     * 上游端点ID（关联 endpoint 表；NULL=用渠道/适配器默认路径）
     * 新库替代旧 model_channel_configs / model_templates 的端点拼接
     */
    @Column("use_endpoint_id")
    private Long useEndpointId;

    @Column("created_at")
    private LocalDateTime createdAt;

    @Column("updated_at")
    private LocalDateTime updatedAt;
}
