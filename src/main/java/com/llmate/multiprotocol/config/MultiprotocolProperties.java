package com.llmate.multiprotocol.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 多协议中转网关全局配置映射矩阵
 * 自动绑定 YML 中以 multiprotocol 开头的配置项
 */
@Data
@Component
@ConfigurationProperties(prefix = "multiprotocol")
public class MultiprotocolProperties {

    /**
     * 存放模型影子影子别名的 Map 容器
     * Key 为外部客户端 SDK 传入的模型名 (如: "gpt-4o")
     * Value 为系统内部真实的路由映射规则与伪装掩码
     */
    private Map<String, ModelMapping> modelMappings = new HashMap<>();
}