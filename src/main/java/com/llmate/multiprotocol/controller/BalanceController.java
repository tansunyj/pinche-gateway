package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.service.UserBalanceService;
import com.llmate.multiprotocol.util.UserContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 用户余额查询控制器
 *
 * <p>{@code GET /v1/user/balance} —— 返回当前 API Key 对应账户的可用余额（额度单位）。
 *
 * <p>响应格式兼容 CC-Switch 用量查询「通用模板」（{@code {{baseUrl}}/user/balance}）：
 * 其 extractor 只读取 {@code response.balance}（剩余额度）与 {@code response.is_active}（有效标志），
 * 因此这里返回 {@code {"is_active": true, "balance": <余额>}}。
 * 用户需在 CC-Switch 用量查询面板选「通用」模板，并把 Base URL 填为
 * {@code https://api.numspirit.com/v1}（该字段留空时默认用供应商端点，拼不出 /v1 前缀）。
 *
 * <p>鉴权：{@link RequireApiKey} 类注解，ApiKeyAuthWebFilter 校验通过后
 * {@link UserContext#getUserId} 必然非空。
 */
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Log4j2
@RequireApiKey
public class BalanceController {

    private final UserBalanceService userBalanceService;

    @GetMapping("/user/balance")
    public Mono<Map<String, Object>> balance(ServerWebExchange exchange) {
        Long userId = UserContext.getUserId(exchange);
        log.info("[BalanceController] 余额查询: userId={}", userId);
        // balance 返回「元」（xxx.yyy 格式），已按 1 元 = 100000 额度从库内额度换算，勿直接返回额度
        return userBalanceService.getBalanceInYuan(userId)
            .map(balance -> {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("is_active", true);
                body.put("balance", balance);
                return body;
            });
    }
}
