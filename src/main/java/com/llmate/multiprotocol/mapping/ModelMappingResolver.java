package com.llmate.multiprotocol.mapping;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.config.MultiprotocolProperties;
import com.llmate.multiprotocol.config.ModelMapping;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 模型名称映射解析器
 * 负责将外部客户端传入的模型名解析为内部路由标识
 * 支持精确匹配、协议特定覆盖、通配匹配三级策略
 */
@Component
public class ModelMappingResolver {

    private final MultiprotocolProperties properties;

    public ModelMappingResolver(MultiprotocolProperties properties) {
        this.properties = properties;
    }

    /**
     * 解析外部模型名到内部路由标识（无协议维度，向后兼容）
     */
    public String resolve(String externalModel) {
        return resolve(externalModel, null);
    }

    /**
     * 解析外部模型名到内部路由标识（带协议维度）
     * 解析优先级：
     * 1. 精确匹配 + 协议特定覆盖
     * 2. 精确匹配（无协议覆盖时使用默认 internal）
     * 3. 通配映射
     * 4. 兜底直接透传
     *
     * @param externalModel 外部客户端传入的模型名
     * @param protocol      当前请求的协议类型（可为 null，null 时等价于无协议维度的旧逻辑）
     * @return 内部路由标识 (如 "anthropic/claude-3-5-sonnet")
     */
    public String resolve(String externalModel, ProtocolType protocol) {
        // 1. 精确匹配
        ModelMapping exact = properties.getModelMappings().get(externalModel);
        if (exact != null) {
            // 1a. 协议特定覆盖优先
            if (protocol != null && exact.getProtocolOverrides() != null) {
                String override = exact.getProtocolOverrides().get(protocol.getCode());
                if (override != null) {
                    return override;
                }
            }
            // 1b. 使用默认 internal
            return exact.getInternal();
        }

        // 2. 通配映射
        for (Map.Entry<String, ModelMapping> entry : properties.getModelMappings().entrySet()) {
            if (entry.getValue().isPattern()) {
                String regexPattern = entry.getKey().replace("*", "(.*)");
                if (externalModel.matches(regexPattern)) {
                    String internalTemplate = entry.getValue().getInternal();
                    String capturedGroup = externalModel.replaceAll(regexPattern, "$1");
                    return internalTemplate.replace("{0}", capturedGroup);
                }
            }
        }

        // 3. 兜底直接透传
        return externalModel;
    }

    /**
     * 获取响应中应该显示的模型名（用于伪装）
     */
    public String getResponseModelMask(String externalModel) {
        ModelMapping mapping = properties.getModelMappings().get(externalModel);
        if (mapping != null && mapping.getResponseModelMask() != null) {
            return mapping.getResponseModelMask();
        }
        return externalModel;
    }
}
