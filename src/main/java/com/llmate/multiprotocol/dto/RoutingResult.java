package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 路由结果
 * 包含模型路由后的完整信息
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RoutingResult {

    /**
     * 原始模型ID（用户传入，保持原样）
     * 如：deepseek-v4-flash 或 aliyun/deepseek-v4-flash
     */
    private String modelId;

    /**
     * 渠道代码（如 aliyun, deepseek）
     */
    private String channelCode;

    /**
     * 上游真实模型名（映射后的模型名，发给渠道）
     * 如：qwen3.6-flash
     */
    private String upstreamModel;

    /**
     * 渠道ID
     */
    private Long channelId;

    /**
     * 纯模型ID（去掉渠道前缀后的模型名）
     * 如：deepseek-v4-flash
     */
    private String pureModelId;

    /**
     * 用户请求中是否包含渠道前缀
     */
    private boolean hasChannelPrefix;

    /**
     * 该模型在此渠道绑定的 Adapter provider_alias（来自 proxy_channel_models.provider_capability）
     * 如 openai_bearer / anthropic / openai_image / dashscope_video
     */
    private String providerAlias;

    /**
     * 获取返回给用户时应使用的模型名
     * 如果有渠道前缀则返回带前缀的，否则返回纯模型ID
     */
    public String getResponseModelId() {
        if (hasChannelPrefix && channelCode != null) {
            return channelCode + "/" + pureModelId;
        }
        return pureModelId != null ? pureModelId : modelId;
    }
}
