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
 * 用户主表实体
 * 对应数据库表: pt_users（拼车平台用户表，替换旧 user_users）
 *
 * 仅保留 pt_users 实际存在的列。status 为枚举字符串 ACTIVE/DISABLED（非旧库 Integer 0/1）。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("pt_users")
public class UserUsersEntity {

    @Id
    private Long id;

    @Column("phone")
    private String phone;

    @Column("password_hash")
    private String passwordHash;

    @Column("nickname")
    private String nickname;

    @Column("avatar_url")
    private String avatarUrl;

    /**
     * 余额（额度单位），计费扣减目标列
     */
    @Column("balance")
    private Long balance;

    /**
     * 累计充值
     */
    @Column("cumulative_recharge")
    private Long cumulativeRecharge;

    /**
     * 状态: ACTIVE=正常, DISABLED=禁用
     */
    @Column("status")
    private String status;

    @Column("last_login_at")
    private LocalDateTime lastLoginAt;

    @Column("created_at")
    private LocalDateTime createdAt;

    @Column("updated_at")
    private LocalDateTime updatedAt;
}
