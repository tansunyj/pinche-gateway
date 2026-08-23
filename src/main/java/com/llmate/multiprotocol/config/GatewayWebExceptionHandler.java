package com.llmate.multiprotocol.config;

import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;
import reactor.core.publisher.Mono;

/**
 * 网关 WebFilter 层异常处理器（WebExceptionHandler）
 *
 * 为什么需要：
 * @RestControllerAdvice 只能捕获 DispatcherHandler 内部（Controller 方法执行）抛出的异常。
 * 而 ApiKeyAuthWebFilter（@Order(HIGHEST_PRECEDENCE)）在进入
 * DispatcherHandler 之前执行，它们抛出的 LlmGatewayException（如「API Key 已被禁用」、
 * 「模型无权限」）会一路冒泡到 ExceptionHandlingWebHandler，若无人处理会被默认的
 * DefaultErrorWebExceptionHandler 渲染成通用 500 错误体。
 *
 * 这里补上这一层：把 WebFilter 层抛出的 LlmGatewayException 转成协议格式错误响应
 * （OpenAI/Anthropic 格式 + 错误码→HTTP 状态映射，与 {@link GlobalExceptionHandler} 共用
 * {@link GatewayErrorResponseBuilder}，返回格式一致）。
 *
 * 必须 @Order(-2) 早于默认错误处理器（Boot 注册的 DefaultErrorWebExceptionHandler @Order(-1)），
 * 否则默认处理器先接管、又会退化成 500。
 * 非 LlmGatewayException 的异常继续向下抛，交还默认处理器，保持 404/500 等 Spring 语义不变。
 */
@Component
@RequiredArgsConstructor
@Log4j2
@Order(-2)
public class GatewayWebExceptionHandler implements WebExceptionHandler {

    private final GatewayErrorResponseBuilder errorBuilder;

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.empty();
        }

        if (ex instanceof LlmGatewayException gatewayEx) {
            LlmErrorCode errorCode = gatewayEx.getErrorCode();
            String message = gatewayEx.getMessage() != null ? gatewayEx.getMessage() : errorCode.getMessageTemplate();
            HttpStatus status = errorBuilder.mapErrorCodeToStatus(errorCode);
            // 完整异常对象进日志（含 cause 链）；对外响应走 builder 统一脱敏 + request_id
            log.warn("[WebExceptionHandler] 网关业务异常(Filter层): code={}, status={}, message={}",
                    errorCode.getCode(), status.value(), message, gatewayEx);
            return errorBuilder.writeError(exchange, status, errorCode, message);
        }

        // 其它异常（ResponseStatusException 等）交还默认处理器
        return Mono.error(ex);
    }
}
