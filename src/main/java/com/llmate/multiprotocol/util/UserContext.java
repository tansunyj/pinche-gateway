package com.llmate.multiprotocol.util;

import com.llmate.multiprotocol.constant.SystemConstants;
import org.springframework.web.server.ServerWebExchange;

import java.util.UUID;

/**
 * 用户上下文工具类
 * 用于在 WebFlux 反应式链中传递和获取用户信息
 */
public class UserContext {

    /**
     * 设置用户ID到 Exchange 属性
     */
    public static void setUserId(ServerWebExchange exchange, Long userId) {
        exchange.getAttributes().put(SystemConstants.CONTEXT_USER_ID_KEY, userId);
    }

    /**
     * 获取用户ID
     */
    public static Long getUserId(ServerWebExchange exchange) {
        Object userId = exchange.getAttribute(SystemConstants.CONTEXT_USER_ID_KEY);
        return userId instanceof Long ? (Long) userId : null;
    }

    /**
     * 设置 Token ID 到 Exchange 属性
     */
    public static void setTokenId(ServerWebExchange exchange, Long tokenId) {
        exchange.getAttributes().put(SystemConstants.CONTEXT_TOKEN_ID_KEY, tokenId);
    }

    /**
     * 获取 Token ID
     */
    public static Long getTokenId(ServerWebExchange exchange) {
        Object tokenId = exchange.getAttribute(SystemConstants.CONTEXT_TOKEN_ID_KEY);
        return tokenId instanceof Long ? (Long) tokenId : null;
    }

    /**
     * 设置 API Key 实体到 Exchange 属性
     */
    public static void setTokenEntity(ServerWebExchange exchange, Object tokenEntity) {
        exchange.getAttributes().put(SystemConstants.CONTEXT_TOKEN_ENTITY_KEY, tokenEntity);
    }

    /**
     * 获取 API Key 实体
     */
    @SuppressWarnings("unchecked")
    public static <T> T getTokenEntity(ServerWebExchange exchange) {
        Object tokenEntity = exchange.getAttribute(SystemConstants.CONTEXT_TOKEN_ENTITY_KEY);
        return tokenEntity != null ? (T) tokenEntity : null;
    }

    /**
     * 设置请求ID到 Exchange 属性
     */
    public static void setRequestId(ServerWebExchange exchange, String requestId) {
        exchange.getAttributes().put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId);
    }

    /**
     * 获取请求ID
     */
    public static String getRequestId(ServerWebExchange exchange) {
        Object requestId = exchange.getAttribute(SystemConstants.CONTEXT_REQUEST_ID_KEY);
        return requestId != null ? requestId.toString() : null;
    }

    /**
     * 获取本请求【全链路唯一】 requestId（复用入口 RequestIdWebFilter 已生成的）。
     *
     * 所有 HTTP 请求必经 RequestIdWebFilter（@Order 最前），它在入口生成一次 requestId，
     * 写入 exchange 属性 + Reactor Context（后者驱动 [reqId=] 日志前缀）。这里直接复用同一个
     * 值，保证：日志前缀 [reqId=]、LogBox 框内 RequestId、计费、proxy_request_logs、视频任务行
     * 全部用同一个 ID，可按 requestId 追踪单次请求。
     *
     * 仅当属性缺失（理论上的非 HTTP / 内部入口）才兜底生成并写回，保证不返回 null。
     */
    public static String getOrGenerateRequestId(ServerWebExchange exchange) {
        String requestId = getRequestId(exchange);
        if (requestId != null) {
            return requestId;
        }
        requestId = UUID.randomUUID().toString().replace("-", "");
        setRequestId(exchange, requestId);
        return requestId;
    }

    /**
     * 清除所有用户信息
     */
    public static void clear(ServerWebExchange exchange) {
        exchange.getAttributes().remove(SystemConstants.CONTEXT_USER_ID_KEY);
        exchange.getAttributes().remove(SystemConstants.CONTEXT_TOKEN_ID_KEY);
        exchange.getAttributes().remove(SystemConstants.CONTEXT_TOKEN_ENTITY_KEY);
        exchange.getAttributes().remove(SystemConstants.CONTEXT_REQUEST_ID_KEY);
    }
}
