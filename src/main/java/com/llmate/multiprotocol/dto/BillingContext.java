package com.llmate.multiprotocol.dto;

import com.llmate.multiprotocol.entity.ModelPricesEntity;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 计费上下文
 *
 * 一次请求内，计费/结算全生命周期所需的共享数据。
 * 由 LlmGateway 在完成路由后组装，传给 BillingService 使用，
 * 避免计费方法携带过长参数列表，也避免 LlmGateway 关心计费细节。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BillingContext {

    /** 请求ID */
    private String requestId;

    /** 用户ID */
    private Long userId;

    /** API Key Token ID */
    private Long tokenId;

    /** API Key Token 实体 */
    private ProxyTokensEntity tokenEntity;

    /** 路由结果（渠道、上游模型名等） */
    private RoutingResult routing;

    /** 模型价格配置 */
    private ModelPricesEntity priceConfig;
}
