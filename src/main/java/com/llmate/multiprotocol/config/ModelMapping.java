package com.llmate.multiprotocol.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 影子模型映射规则明细
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ModelMapping {

    /**
     * 系统内部核心路由的真实目标模型路径 (如: "anthropic/claude-3-5-sonnet")
     */
    private String internal;

    /**
     * 逆向伪装掩码。大模型响应时，需要伪装欺骗回客户端 SDK 的模型名称 (如: "gpt-4o")
     * 防止客户端 SDK 校验模型名不一致时抛出前端异常。
     */
    private String responseModelMask;

    /**
     * 是否启用正则表达式通配符匹配 (如: claude-3-* 匹配所有 claude-3 系列模型)
     */
    private boolean pattern;

    /**
     * 协议特定覆盖映射
     * Key: 协议 code (如 "openai_chat", "anthropic_messages", "openai_responses")
     * Value: 该协议下的内部路由目标 (如 "aliyun/qwen-max")
     *
     * 当同一个外部模型名在不同协议入口下需要路由到不同渠道时使用。
     * 例如：
     *   gpt-4o 从 OpenAI 入口进来 → 路由到 aliyun/qwen-max
     *   gpt-4o 从 Anthropic 入口进来 → 路由到 anthropic/claude-3-5-sonnet
     */
    private Map<String, String> protocolOverrides;
}