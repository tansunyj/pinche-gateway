package com.llmate.multiprotocol.config;

import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.resource.NoResourceFoundException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.Map;

/**
 * 全局异常处理器（@RestControllerAdvice，处理 Controller 层抛出的异常）
 *
 * 注意：WebFilter 层（ApiKeyAuthWebFilter）抛出的 LlmGatewayException
 * 不会进入 @ControllerAdvice，统一由 {@link GatewayWebExceptionHandler}（WebExceptionHandler）处理。
 * 两层共用 {@link GatewayErrorResponseBuilder} 的格式化逻辑，保证返回格式一致。
 */
@RestControllerAdvice
@RequiredArgsConstructor
@Log4j2
public class GlobalExceptionHandler {

    private final GatewayErrorResponseBuilder errorBuilder;

    /**
     * 处理网关业务异常（携带错误码枚举）
     * 根据错误码映射 HTTP 状态码，按协议类型格式化错误响应
     */
    @ExceptionHandler(LlmGatewayException.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleLlmGatewayException(LlmGatewayException ex, ServerWebExchange exchange) {
        LlmErrorCode errorCode = ex.getErrorCode();
        String message = ex.getMessage() != null ? ex.getMessage() : errorCode.getMessageTemplate();
        HttpStatus status = errorBuilder.mapErrorCodeToStatus(errorCode);

        log.error("[网关业务异常] code={}, message={}", errorCode.getCode(), message, ex);

        // 响应已 committed 时无法修改，直接返回空 Mono
        if (exchange.getResponse().isCommitted()) {
            log.warn("[GlobalExceptionHandler] Response 已 committed，跳过异常处理: code={}, message={}", errorCode.getCode(), message);
            return Mono.empty();
        }

        // 传入 errorCode：上游/渠道类错误码由 builder 统一替换为通用文案（不透传上游信息）
        return errorBuilder.buildErrorResponse(status, errorCode, message, exchange);
    }

    /**
     * 处理未知路径/静态资源缺失（404）
     *
     * 请求落不到任何 Controller 映射时，WebFlux 会交给默认静态资源处理器 ResourceWebHandler，
     * 找不到资源抛 NoResourceFoundException。若不加区分被上面的 RuntimeException 兜底处理，
     * 会错误返回 500（日志里 Claude Desktop 探测 GET / 报 "No static resource ." 500 即此问题）。
     * 这里单独映射为 404，语义正确且不再污染错误日志（只打 warn 不打 ERROR）。
     */
    @ExceptionHandler(NoResourceFoundException.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleNoResourceFound(NoResourceFoundException ex, ServerWebExchange exchange) {
        String path = exchange.getRequest().getPath().value();
        log.warn("[GlobalExceptionHandler] 未知路径 404: {} {}", exchange.getRequest().getMethod(), path);
        return errorBuilder.buildErrorResponse(HttpStatus.NOT_FOUND, null, "Not Found: " + path, exchange);
    }

    /**
     * 兜底处理非业务异常的 RuntimeException，统一归为 INTERNAL_ERROR (500)
     */
    @ExceptionHandler(RuntimeException.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleRuntimeException(RuntimeException ex, ServerWebExchange exchange) {
        log.error("[未预期的运行时异常] {}", ex.getMessage(), ex);

        if (exchange.getResponse().isCommitted()) {
            log.warn("[GlobalExceptionHandler] Response 已 committed，跳过异常处理: {}", ex.getMessage());
            return Mono.empty();
        }

        // 对外统一文案，不透传 ex.getMessage()（裸上游异常自动 502 + 通用文案）
        HttpStatus status = errorBuilder.statusFor(ex);
        String message = errorBuilder.clientMessage(ex);
        return errorBuilder.buildErrorResponse(status, null, message, exchange);
    }

    /**
     * 兜底异常处理
     */
    @ExceptionHandler(Exception.class)
    public Mono<ResponseEntity<Map<String, Object>>> handleGenericException(Exception ex, ServerWebExchange exchange) {
        log.error("[未预期的异常] {}", ex.getMessage(), ex);

        if (exchange.getResponse().isCommitted()) {
            log.warn("[GlobalExceptionHandler] Response 已 committed，跳过异常处理: {}", ex.getMessage());
            return Mono.empty();
        }

        // 对外统一文案，不透传 ex.getMessage()（裸上游异常自动 502 + 通用文案）
        HttpStatus status = errorBuilder.statusFor(ex);
        String message = errorBuilder.clientMessage(ex);
        return errorBuilder.buildErrorResponse(status, null, message, exchange);
    }
}
