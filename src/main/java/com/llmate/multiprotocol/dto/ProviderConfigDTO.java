package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;

/**
 * Provider 配置 DTO
 * 用于 Redis 缓存序列化
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProviderConfigDTO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 渠道名称 */
    private String name;

    /** 渠道代码 */
    private String alias;

    /** 渠道类型 */
    private String type;

    /** 基础URL */
    private String baseUrl;

    /** API Key（单 Token 模式保留兼容） */
    private String apiKey;

    /** 多 Token 模式：所有启用的 API Key 列表 */
    private List<String> apiKeys;

    /** 多 Token 模式：所有启用的 Token ID 列表（与 apiKeys 一一对应） */
    private List<Long> tokenIds;

    /** 状态: 0=禁用, 1=启用 */
    private Integer status;
}
