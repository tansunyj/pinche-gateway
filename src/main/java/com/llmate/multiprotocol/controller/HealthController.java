package com.llmate.multiprotocol.controller;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 健康检查 / 根路径控制器
 *
 * <p>Claude Desktop、Claude Code 等 Anthropic SDK 客户端配置自定义 base_url 后，
 * 启动时会对根路径发起 GET /、HEAD / 做连通性探测。本控制器返回 200 JSON，
 * 避免请求落入默认静态资源处理器抛 NoResourceFoundException 被全局异常处理成 500，
 * 导致客户端误判网关不可用。
 *
 * <p>注意：不带 {@link com.llmate.multiprotocol.annotation.RequireApiKey}，
 * ApiKeyAuthWebFilter 对该 handler 直接放行（客户端探测不携带 API Key）。
 */
@RestController
public class HealthController {

    /** 服务名（与 application.yml spring.application.name 保持一致） */
    private static final String SERVICE_NAME = "silievo-gateway";

    /**
     * GET / 与 GET /health —— 根路径连通性 / 存活探针
     * HEAD / 由 Spring 对 GET 映射自动支持，返回 200 空 body。
     */
    @GetMapping({"/", "/health"})
    public Mono<Map<String, Object>> health() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("service", SERVICE_NAME);
        return Mono.just(body);
    }
}
