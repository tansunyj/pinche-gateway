package com.llmate.multiprotocol.filter;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * 强制每个请求使用独立的 HTTP 连接（Connection: close）。
 *
 * 背景：ReadOnlyHttpHeaders 异常（UnsupportedOperationException，发生在
 * EncoderHttpMessageWriter 设置 Content-Length 时）只在连接上「第二个及以后的请求」出现，
 * 第一个请求正常。原因是 HTTP/1.1 keep-alive 连接复用时，前一个请求的响应状态（committed /
 * 只读 headers）残留到连接上，导致后一个请求的 EncoderHttpMessageWriter 在写入响应头时
 * 发现 headers 已是只读。
 *
 * 对本网关场景（每次请求耗时 30~120 秒的上游 LLM 调用），连接按请求新建完全可接受，
 * 且能彻底消除 keep-alive 复用带来的响应状态串扰。
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class ConnectionCloseWebFilter implements WebFilter {

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 必须在响应 commit 之前设置；WebFilter 在 Controller 执行前运行，满足该条件。
        exchange.getResponse().getHeaders().set(HttpHeaders.CONNECTION, "close");
        return chain.filter(exchange);
    }
}
