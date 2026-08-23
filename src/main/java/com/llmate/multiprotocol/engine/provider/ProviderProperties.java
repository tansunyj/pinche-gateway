package com.llmate.multiprotocol.engine.provider;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Provider 配置属性绑定类
 * 自动绑定 application.yml 中的 providers.enabled 列表
 *
 * 配置示例：
 * providers:
 *   enabled:
 *     - name: dashscope
 *       alias: aliyun
 *       type: openai_bearer
 *       base-url: https://dashscope.aliyuncs.com/compatible-mode/v1/
 *       api-key: sk-xxx
 *     - name: deepseek
 *       alias: deepseek
 *       type: openai_bearer
 *       base-url: https://api.deepseek.com/v1/
 *       api-key: sk-xxx
 *     - name: azure
 *       alias: azure
 *       type: openai_azure
 *       base-url: https://xxx.openai.azure.com/openai/v1/
 *       api-key: xxx
 *     - name: anthropic
 *       alias: vp
 *       type: anthropic
 *       base-url: https://api.anthropic.com/
 *       api-key: sk-ant-xxx
 *     - name: vertex
 *       alias: vp_vtx
 *       type: vertex
 *       base-url: https://api.vapeur.ai/gemini/v1beta/models/{model}:generateContent
 *       api-key: sk-xxx
 */
@Data
@Component
@ConfigurationProperties(prefix = "providers")
public class ProviderProperties {

    /**
     * 启用的 Provider 列表
     */
    private List<ProviderConfig> enabled = new ArrayList<>();

    /**
     * 单个 Provider 配置项
     */
    @Data
    public static class ProviderConfig {
        /** Provider 名称（用于日志） */
        private String name;

        /** 模型名称前缀（如 aliyun, deepseek, azure） */
        private String alias;

        /** 渠道ID（网关从 proxy_channels 加载时设置，供查询该渠道模型绑定） */
        private Long channelId;

        /** Provider 类型，决定使用哪个模板类创建实例 */
        private String type;

        /** API 基址 */
        private String baseUrl;

        /** API Key / Access Token（单 Token 模式保留兼容） */
        private String apiKey;

        /** 多 Token 模式：所有启用的 API Key 列表 */
        private List<String> apiKeys;

        /** 多 Token 模式：所有启用的 Token ID 列表（与 apiKeys 一一对应） */
        private List<Long> tokenIds;
    }
}
