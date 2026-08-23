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
 * 上游渠道表实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("proxy_channels")
public class ProxyChannelsEntity {

    @Id
    private Long id;

    /**
     * 渠道别名，如 aliyun, deepseek
     */
    @Column("channel_code")
    private String channelCode;

    @Column("name")
    private String name;

    /**
     * openai_bearer, anthropic, vertex, openai_azure
     */
    @Column("type")
    private String type;

    @Column("base_url")
    private String baseUrl;

    /**
     * 0=禁用, 1=启用
     */
    @Column("status")
    private Integer status;

    @Column("priority")
    private Integer priority;

    @Column("weight")
    private Integer weight;

    /**
     * 负载均衡策略
     */
    @Column("token_lb_strategy")
    private String tokenLbStrategy;

    /**
     * 参数映射配置（JSON）
     */
    @Column("param_mappings")
    private String paramMappings;

    @Column("created_at")
    private LocalDateTime createdAt;
}
