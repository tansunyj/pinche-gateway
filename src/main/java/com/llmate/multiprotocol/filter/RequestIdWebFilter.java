package com.llmate.multiprotocol.filter;

import com.llmate.multiprotocol.constant.SystemConstants;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * 请求 ID 生成与注入 WebFilter（最先执行，order 最高）
 *
 * 为每个请求生成 requestId（UUID），并：
 * 1. 存入 exchange 属性（key=requestId），供 RequestLoggingWebFilter / SettlementService /
 *    UserContext 等以非响应式方式读取；
 * 2. 用 contextWrite 写入 Reactor Context。配合 ReactiveMdcConfiguration 注册的
 *    ThreadContextAccessor + log4j2 %X{requestId}，本条请求【所有下游日志】（后续
 *    WebFilter / 控制器 / 网关 / Adapter / 计费）都会自动带上 requestId，无需手动传参。
 *
 * 为什么要独立成最先执行的 filter：RequestLoggingWebFilter 为了在入口日志读到 userId
 * 故意排在认证 filter 之后（HIGHEST_PRECEDENCE + 100），若由它生成 requestId，排在它
 * 之前的 ApiKeyAuth / ModelPermission 等 filter 的日志就拿不到 requestId。这里单独在最
 * 前面生成，保证认证、权限、日志、业务全链路日志一致带 requestId。
 *
 * 注意：contextWrite 应用在 chain.filter(exchange) 的返回 Mono 上（最外层算子），
 * 后续所有 filter 与 DispatcherHandler 都在它上游，因此都能读到该 Context。
 *
 * 坑：@Order 用 Ordered.HIGHEST_PRECEDENCE（=Integer.MIN_VALUE）与 ApiKeyAuthWebFilter 相同，
 * 同序 WebFilter 的相对顺序由 Bean 注册次序决定、不受控——实测 ApiKeyAuth 先执行，其内部
 * setUserContext 打日志时 requestId 尚未生成，[reqId=] 恒为空。
 *
 * 千万不要写成 HIGHEST_PRECEDENCE - 1！Integer.MIN_VALUE - 1 会整数溢出成
 * Integer.MAX_VALUE（=LOWEST_PRECEDENCE），本过滤器反而变成【最后】执行，其 contextWrite
 * 只覆盖 DispatcherHandler（控制器/网关），前面所有 WebFilter 的日志全部拿不到 requestId
 * （日志特征：setUserContext/ModelPermission/请求入口 [reqId=] 恒空，而 Controller 之后都有）。
 * 最小值就是 MIN_VALUE，没有比它更小的整数。正确做法是：本过滤器保持 @Order(HIGHEST_PRECEDENCE)
 * 严格最前，把原本也挂在 MIN_VALUE 上的 ApiKeyAuthWebFilter 下调（HIGHEST_PRECEDENCE + 5），
 * 让 requestId 在认证日志之前就已生成。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@Log4j2
public class RequestIdWebFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 若已有 requestId（理论上无，仅防重入），复用；否则生成
        String requestId = exchange.getAttribute(SystemConstants.CONTEXT_REQUEST_ID_KEY);
        if (requestId == null) {
            requestId = UUID.randomUUID().toString().replace("-", "");
            exchange.getAttributes().put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId);
        }
        // lambda 要求局部变量 effectively final，取一个 final 副本供 contextWrite 使用
        final String contextRequestId = requestId;
        return chain.filter(exchange)
                .contextWrite(ctx -> ctx.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, contextRequestId));
    }
}
