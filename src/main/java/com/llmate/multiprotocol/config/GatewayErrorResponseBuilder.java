package com.llmate.multiprotocol.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.util.UserContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 网关错误响应构建器（共享组件）
 *
 * 两种异常处理机制共用同一套格式化逻辑，保证返回格式一致：
 * 1. {@link GlobalExceptionHandler}（@RestControllerAdvice）—— 捕获 Controller 层抛出的异常
 * 2. {@link GatewayWebExceptionHandler}（WebExceptionHandler）—— 捕获 WebFilter 层抛出的异常
 *    （ApiKeyAuthWebFilter 在进入 DispatcherHandler 之前执行，
 *    @RestControllerAdvice 接不到，必须由 WebExceptionHandler 层处理）
 *
 * 输出协议格式：OpenAI / Responses → { "error": { message, type, code, request_id } }
 *              Anthropic           → { "type": "error", "error": { type, message, request_id } }
 *
 * 安全约定（对外消息统一脱敏，内部日志保持完整）：
 * - 上游/渠道调用失败（502/504 那批错误码）→ 客户端 message 一律固定通用文案，
 *   不透传上游 URL / 响应体 / 密钥（这些细节只进日志，见 AbstractProviderAdapter.logError 等）。
 * - 未预期异常（500）→ 固定「服务器内部错误，请稍后重试」，不透传 ex.getMessage()。
 * - 业务错误码（网关自身模板文案）→ 保留原文 + 过 {@link #sanitize} 兜底脱敏。
 * - 流式 SSE 错误事件与视频任务查询回显走同一策略，保证全出口一致。
 */
@Component
@RequiredArgsConstructor
@Log4j2
public class GatewayErrorResponseBuilder {

    private final ObjectMapper objectMapper;

    /** 上游/渠道调用失败时的统一对外文案（不透传上游任何信息） */
    public static final String UPSTREAM_FAILED_MESSAGE = "上游渠道调用失败，请稍后重试或联系管理员";

    /** 未预期异常的统一对外文案 */
    public static final String INTERNAL_ERROR_MESSAGE = "服务器内部错误，请稍后重试";

    /**
     * 需要完全通用化的错误码集合：message 可能携带上游 URL / 响应体 / 密钥（来自
     * WebClientResponseException.getMessage()、extractUpstreamErrorMessage 等），一律替换为
     * {@link #UPSTREAM_FAILED_MESSAGE}。CHANNEL_* 属网关自身路由/选渠道文案（含渠道名/模型名），
     * 不进本集合，走 {@link #sanitize} 保留文案并剥 URL/密钥。
     */
    private static final Set<LlmErrorCode> UPSTREAM_GENERIC = Set.of(
            LlmErrorCode.PROVIDER_ERROR,
            LlmErrorCode.PROVIDER_INVALID_RESPONSE,
            LlmErrorCode.PROVIDER_TIMEOUT,
            LlmErrorCode.UPSTREAM_UNAVAILABLE,
            LlmErrorCode.IMAGE_DOWNLOAD_FAILED,
            LlmErrorCode.ARK_SERVICE_ERROR
    );

    /**
     * 错误码枚举 → HTTP 状态码映射
     */
    public HttpStatus mapErrorCodeToStatus(LlmErrorCode errorCode) {
        return switch (errorCode) {
            // 401 认证错误
            case AUTH_FAILED, AUTH_INVALID_KEY, AUTH_KEY_EXPIRED, AUTH_USER_DISABLED, AUTH_MODEL_NO_PERMISSION
                -> HttpStatus.UNAUTHORIZED;

            // 402 余额错误
            case BALANCE_INSUFFICIENT, BALANCE_RESERVE_FAILED
                -> HttpStatus.PAYMENT_REQUIRED;

            // 400 请求错误
            case INVALID_REQUEST, MODEL_NOT_SUPPORTED, PRICE_NOT_CONFIGURED, FEATURE_NOT_ENABLED
                -> HttpStatus.BAD_REQUEST;

            // 404 模型/任务/素材不存在
            case MODEL_NOT_FOUND, TASK_NOT_FOUND, MATERIAL_NOT_FOUND, MATERIAL_COLLECTION_NOT_FOUND
                -> HttpStatus.NOT_FOUND;

            // 409 资源数量上限冲突
            case MATERIAL_LIMIT_REACHED
                -> HttpStatus.CONFLICT;

            // 429 限流
            case RATE_LIMITED
                -> HttpStatus.TOO_MANY_REQUESTS;

            // 502 渠道/上游错误
            case CHANNEL_NOT_FOUND, CHANNEL_UNAVAILABLE, CHANNEL_TOKEN_EXHAUSTED, CHANNEL_NO_DEFAULT,
                 UPSTREAM_UNAVAILABLE, PROVIDER_ERROR, PROVIDER_INVALID_RESPONSE, IMAGE_DOWNLOAD_FAILED,
                 ARK_SERVICE_ERROR
                -> HttpStatus.BAD_GATEWAY;

            // 503 服务未配置/不可用（向量等模型服务未配置）
            case SERVICE_UNAVAILABLE
                -> HttpStatus.SERVICE_UNAVAILABLE;

            // 504 上游超时
            case PROVIDER_TIMEOUT
                -> HttpStatus.GATEWAY_TIMEOUT;

            // 500 计费/系统错误
            case BILLING_PRICE_NOT_FOUND, BILLING_CALCULATION_ERROR, INTERNAL_ERROR
                -> HttpStatus.INTERNAL_SERVER_ERROR;
        };
    }

    /**
     * 构建错误 ResponseEntity（供 @RestControllerAdvice 返回给消息转换器）
     *
     * @param errorCode 业务错误码（可为 null：未预期异常/兜底异常）。上游类错误码会替换为通用文案。
     */
    public Mono<ResponseEntity<Map<String, Object>>> buildErrorResponse(HttpStatus status, LlmErrorCode errorCode,
                                                                        String message, ServerWebExchange exchange) {
        return Mono.just(ResponseEntity
                .status(status)
                .contentType(MediaType.APPLICATION_JSON)
                .body(buildErrorBody(status, errorCode, message, exchange)));
    }

    /**
     * 直接把错误响应写入 exchange（供 WebExceptionHandler 使用）。
     * 响应已 committed 时返回空 Mono，避免 ReadOnlyHttpHeaders / IllegalStateException。
     */
    public Mono<Void> writeError(ServerWebExchange exchange, HttpStatus status, LlmErrorCode errorCode, String message) {
        ServerHttpResponse response = exchange.getResponse();
        if (response.isCommitted()) {
            log.warn("[GatewayError] Response 已 committed，跳过写错误响应: status={}", status);
            return Mono.empty();
        }

        byte[] bytes;
        try {
            bytes = objectMapper.writeValueAsBytes(buildErrorBody(status, errorCode, message, exchange));
        } catch (Exception e) {
            log.error("[GatewayError] 序列化错误响应失败: status={}, message={}", status, message, e);
            bytes = "{\"error\":{\"message\":\"Internal Server Error\",\"type\":\"api_error\",\"code\":500}}"
                    .getBytes(StandardCharsets.UTF_8);
        }

        response.setStatusCode(status);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        DataBuffer buffer = response.bufferFactory().wrap(bytes);
        return response.writeWith(Mono.just(buffer));
    }

    /**
     * 根据协议类型构建错误响应体（统一过 {@link #clientMessage} 脱敏 + 附 request_id）
     */
    private Map<String, Object> buildErrorBody(HttpStatus status, LlmErrorCode errorCode, String message,
                                               ServerWebExchange exchange) {
        message = clientMessage(errorCode, message);
        String requestId = requestIdOf(exchange);
        ProtocolType protocol = detectProtocol(exchange);
        if (protocol == ProtocolType.ANTHROPIC_MESSAGES) {
            return buildAnthropicErrorBody(status, message, requestId);
        }
        // OpenAI Chat Completions / Responses / 其他均使用 OpenAI 格式
        return buildOpenAiErrorBody(status, message, requestId);
    }

    // ==================== 对外消息策略（统一脱敏） ====================

    /**
     * Throwable → 对客户端安全的 message（流式 SSE 错误事件 / 未预期异常兜底用）。
     * - {@link LlmGatewayException}：按错误码分类（上游码→通用文案；业务码→模板原文过脱敏）。
     * - 裸上游异常（WebClient 响应/请求/连接/超时/SSL，沿 cause 链识别）：→ 通用文案，防 URL 泄漏。
     * - 其余异常：→ 「服务器内部错误，请稍后重试」。
     */
    public String clientMessage(Throwable t) {
        if (t instanceof LlmGatewayException gex) {
            return clientMessage(gex.getErrorCode(), gex.getMessage());
        }
        if (isUpstreamThrowable(t)) {
            return UPSTREAM_FAILED_MESSAGE;
        }
        return INTERNAL_ERROR_MESSAGE;
    }

    /**
     * 已知错误码 → 对客户端安全的 message。
     * 上游/渠道类错误码 → 固定通用文案；业务错误码 → 模板原文过 {@link #sanitize} 兜底脱敏。
     */
    public String clientMessage(LlmErrorCode code, String raw) {
        if (code != null && UPSTREAM_GENERIC.contains(code)) {
            return UPSTREAM_FAILED_MESSAGE;
        }
        String sanitized = sanitize(raw);
        // 脱敏后为空 / 只剩脱敏占位符 → 回退（INTERNAL_ERROR 用通用文案，其余用错误码模板原文）
        if (sanitized == null || sanitized.isBlank() || isOnlyRedaction(sanitized)) {
            if (code == null || code == LlmErrorCode.INTERNAL_ERROR) {
                return INTERNAL_ERROR_MESSAGE;
            }
            return code.getMessageTemplate();
        }
        return sanitized;
    }

    /**
     * Throwable → HTTP 状态（未预期异常兜底用）。
     * LlmGatewayException 按错误码映射；裸上游异常 → 502；其余 → 500。
     */
    public HttpStatus statusFor(Throwable t) {
        if (t instanceof LlmGatewayException gex) {
            return mapErrorCodeToStatus(gex.getErrorCode());
        }
        if (isUpstreamThrowable(t)) {
            return HttpStatus.BAD_GATEWAY;
        }
        return HttpStatus.INTERNAL_SERVER_ERROR;
    }

    /**
     * 流式 SSE 错误事件 data：{ message, type, code, request_id }（message 统一脱敏，不透传上游信息）。
     */
    public Map<String, Object> streamErrorData(Throwable t, ServerWebExchange exchange) {
        HttpStatus status = statusFor(t);
        Map<String, Object> data = new HashMap<>();
        data.put("message", clientMessage(t));
        data.put("type", "api_error");
        data.put("code", status.value());
        String requestId = requestIdOf(exchange);
        if (requestId != null) {
            data.put("request_id", requestId);
        }
        return data;
    }

    /**
     * 流式 SSE 错误事件流。OpenAI 兼容协议在 error 事件后附 [DONE] 结束标记（保持现状），
     * Anthropic / Responses / Vertex 只发 error 事件。
     */
    public Flux<ServerSentEvent<Object>> streamErrorEvents(Throwable t, ServerWebExchange exchange, boolean appendDone) {
        ServerSentEvent<Object> errorEvent = ServerSentEvent.builder()
                .event("error")
                .data(streamErrorData(t, exchange))
                .build();
        if (appendDone) {
            return Flux.just(errorEvent, ServerSentEvent.builder().data("[DONE]").build());
        }
        return Flux.just(errorEvent);
    }

    /**
     * 对客户端消息做脱敏兜底：剥 URL、掩码 API 密钥 / Bearer / base64 长串，压缩空白。
     * 日志路径不受影响（上游完整信息仍由 logError / GlobalExceptionHandler 打印）。
     */
    public String sanitize(String raw) {
        if (raw == null) {
            return null;
        }
        String s = raw;
        // 1) URL 先剥（避免 URL 内长 token 被第 5 步误杀成多段 [REDACTED]）
        s = s.replaceAll("https?://[^\\s\"'<>]+", "[URL]");
        // 2) OpenAI 风格 sk- 密钥
        s = s.replaceAll("(?i)\\bsk-[A-Za-z0-9_-]{6,}\\b", "sk-[REDACTED]");
        // 3) Bearer token
        s = s.replaceAll("(?i)\\bbearer\\s+[A-Za-z0-9._~+/-]{8,}", "Bearer [REDACTED]");
        // 4) x-api-key / api-key header 样值
        s = s.replaceAll("(?i)(x-api-key|api-key)[\\s=:]+[A-Za-z0-9._-]{8,}", "$1=[REDACTED]");
        // 5) 超长 base64 / token（连续 64+ 个 base64 字符集）
        s = s.replaceAll("[A-Za-z0-9+/=]{64,}", "[REDACTED]");
        // 6) 空白压缩（上游错误常含换行/多空格）
        s = s.replaceAll("\\s+", " ").trim();
        // 7) 兜底：未替换 %s 占位符不应暴露给用户（原 sanitizeUnresolvedPlaceholders 逻辑）
        if (s.contains("%s")) {
            log.warn("[GatewayError] 错误消息含未替换占位符 %s，已净化后返回: {}", s);
            s = s.replaceAll("\\s*%s\\s*", "").trim();
            // 去掉净化后结尾残留的冒号（如 "API Key 无效："）
            if (s.endsWith("：") || s.endsWith(":")) {
                s = s.substring(0, s.length() - 1);
            }
        }
        return s;
    }

    /**
     * 脱敏后是否只剩脱敏占位符（[URL] / [REDACTED] 及少量分隔符）——此时内容无诊断价值，回退模板。
     */
    private boolean isOnlyRedaction(String s) {
        return s.replaceAll("[\\[\\]URLREDACTED\\s:：、,.;；-]+", "").isEmpty();
    }

    /**
     * 沿 cause 链识别裸上游异常（WebClient 响应/请求异常、连接失败、超时、TLS 异常）。
     * 非流式路径经 {@link #statusFor} → 502；流式路径 message 通用化，防 WebClientResponseException 的 URL 泄漏。
     */
    private boolean isUpstreamThrowable(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof WebClientResponseException || cur instanceof WebClientRequestException
                    || cur instanceof ConnectException || cur instanceof SocketTimeoutException
                    || cur instanceof SSLException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    /**
     * 获取当前请求的 requestId（复用入口 RequestIdWebFilter 已生成的，写入 exchange 属性；
     * 缺失时兜底生成并写回，保证同一次请求内取值一致）。
     */
    private String requestIdOf(ServerWebExchange exchange) {
        if (exchange == null) {
            return null;
        }
        return UserContext.getOrGenerateRequestId(exchange);
    }

    /**
     * 检测当前请求的协议类型
     * 优先从 WebFlux 上下文属性读取（Controller 已绑定），兜底从请求路径推断。
     * Filter 层异常时协议未绑定，路径推断即可覆盖（/v1/messages → Anthropic，其余 → OpenAI）。
     */
    private ProtocolType detectProtocol(ServerWebExchange exchange) {
        Object bound = exchange.getAttributes().get(SystemConstants.CONTEXT_PROTOCOL_KEY);
        if (bound instanceof ProtocolType) {
            return (ProtocolType) bound;
        }

        String path = exchange.getRequest().getPath().value();
        if (path.contains("/v1/messages")) {
            return ProtocolType.ANTHROPIC_MESSAGES;
        } else if (path.contains("/v1/responses")) {
            return ProtocolType.OPENAI_RESPONSES;
        } else {
            return ProtocolType.OPENAI_CHAT_COMPLETIONS; // 默认
        }
    }

    /**
     * 构建 OpenAI 格式错误响应
     * { "error": { "message": "...", "type": "...", "code": "HTTP状态int", "request_id": "..." } }
     */
    private Map<String, Object> buildOpenAiErrorBody(HttpStatus status, String message, String requestId) {
        Map<String, Object> error = new HashMap<>();
        error.put("message", message != null ? message : "Unknown error");
        error.put("type", mapToOpenAiErrorType(status));
        error.put("code", status.value());
        if (requestId != null) {
            error.put("request_id", requestId);
        }
        Map<String, Object> errorBody = new HashMap<>();
        errorBody.put("error", error);
        return errorBody;
    }

    /**
     * 构建 Anthropic 格式错误响应
     * { "type": "error", "error": { "type": "...", "message": "...", "request_id": "..." } }
     */
    private Map<String, Object> buildAnthropicErrorBody(HttpStatus status, String message, String requestId) {
        Map<String, Object> error = new HashMap<>();
        error.put("type", mapToAnthropicErrorType(status));
        error.put("message", message != null ? message : "Unknown error");
        if (requestId != null) {
            error.put("request_id", requestId);
        }
        Map<String, Object> errorBody = new HashMap<>();
        errorBody.put("type", "error");
        errorBody.put("error", error);
        return errorBody;
    }

    /**
     * HTTP 状态码 → OpenAI 错误类型映射
     */
    private String mapToOpenAiErrorType(HttpStatus status) {
        return switch (status) {
            case UNAUTHORIZED -> "invalid_request_error";
            case TOO_MANY_REQUESTS -> "rate_limit_error";
            case BAD_REQUEST -> "invalid_request_error";
            case NOT_FOUND -> "invalid_request_error";
            case SERVICE_UNAVAILABLE -> "service_unavailable";
            default -> "api_error";
        };
    }

    /**
     * HTTP 状态码 → Anthropic 错误类型映射
     */
    private String mapToAnthropicErrorType(HttpStatus status) {
        return switch (status) {
            case UNAUTHORIZED -> "authentication_error";
            case TOO_MANY_REQUESTS -> "rate_limit_error";
            case BAD_REQUEST -> "invalid_request_error";
            case NOT_FOUND -> "not_found_error";
            default -> "api_error";
        };
    }
}
