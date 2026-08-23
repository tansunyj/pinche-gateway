package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 模型端点配置 DTO
 * 包含完整的请求地址信息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ModelEndpointConfig {

    /** 基础URL，如 https://api.deepseek.com */
    private String baseUrl;

    /** 端点路径，如 v1/chat/completions */
    private String endpointPath;

    /** HTTP 方法: GET, POST, PUT, DELETE */
    private String httpMethod;

    /**
     * 获取完整的请求URL
     */
    public String getFullUrl() {
        if (baseUrl == null || baseUrl.isEmpty()) {
            return endpointPath != null ? endpointPath : "";
        }
        String normalizedBase = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        String normalizedPath = endpointPath != null ? endpointPath : "";
        // 移除路径开头的斜杠，避免双斜杠
        if (normalizedPath.startsWith("/")) {
            normalizedPath = normalizedPath.substring(1);
        }
        return normalizedBase + "/" + normalizedPath;
    }
}
