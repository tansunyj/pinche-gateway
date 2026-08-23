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
 * 用户 API Key 表实体
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("proxy_tokens")
public class ProxyTokensEntity {

    @Id
    private Long id;

    @Column("user_id")
    private Long userId;

    @Column("name")
    private String tokenName;

    /**
     * sk-xxx 格式
     */
    @Column("key")
    private String apiKey;

    /**
     * 0=禁用, 1=启用
     */
    @Column("status")
    private Integer status;

    /**
     * 价格倍率
     */
    @Column("price_markup")
    private BigDecimal priceMarkup;

    /**
     * 过期时间
     */
    @Column("expired_at")
    private LocalDateTime expiredAt;

    @Column("created_at")
    private LocalDateTime createdAt;
}
