package com.llmate.multiprotocol.filter;

import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.entity.UserUsersEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.service.ApiKeyAuthService;
import com.llmate.multiprotocol.service.UserUsersService;
import com.llmate.multiprotocol.util.KeyMaskUtil;
import com.llmate.multiprotocol.util.UserContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.reactive.result.method.annotation.RequestMappingHandlerMapping;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.Optional;

/**
 * API Key 认证 WebFilter
 * 在请求进入 Controller 前进行 API Key 认证
 * 同时校验：API Key 是否禁用、用户是否禁用、API Key 是否过期
 */
@Component
@RequiredArgsConstructor
@Order(Ordered.HIGHEST_PRECEDENCE + 5) // 认证最先执行（仅次于 RequestIdWebFilter，其负责先生成 requestId），早于 ModelPermission(+10)/RequestLogging(+100)，确保后续日志（请求入口/响应）能读到 UserId
@Log4j2
public class ApiKeyAuthWebFilter implements WebFilter {

    private final ApiKeyAuthService apiKeyAuthService;
    private final UserUsersService userUsersService;
    private final RequestMappingHandlerMapping handlerMapping;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 【关键修复】如果响应已提交，直接返回，不再处理认证
        // WebFlux 在响应完成后（如连接关闭/异常处理时）可能重新触发过滤器链
        if (exchange.getResponse().isCommitted()) {
            log.debug("[API Key 认证] 响应已提交({})，跳过处理", exchange.getResponse().getStatusCode());
            return Mono.empty();
        }

        // 【防重入检查】如果已经认证过（UserId 已设置），直接放行
        if (UserContext.getUserId(exchange) != null) {
            log.debug("[API Key 认证] 已认证，跳过重复执行: userId={}", UserContext.getUserId(exchange));
            return chain.filter(exchange);
        }

        // 关键修复：禁止用 switchIfEmpty(chain.filter(exchange))。
        // chain.filter() 返回 Mono<Void>（空完成），作为 flatMap 内部完成时总会触发 switchIfEmpty，
        // 导致整条过滤器链（含 Controller、网关、上游调用、计费）被重复执行一遍。
        // 也不能用 defaultIfEmpty(null) —— Reactor 装配阶段 Objects.requireNonNull 会抛 NPE。
        // 正确做法：用 Optional 包装 Handler，defaultIfEmpty(Optional.empty()) 把"找不到 Handler"
        // 归一为一个非 null 值，让 flatMap 只执行一次。
        // 1. 获取请求对应的 Handler
        return handlerMapping.getHandler(exchange)
            .map(Optional::ofNullable)
            .defaultIfEmpty(Optional.empty())
            .flatMap(handlerOpt -> {
                // 2. 找不到 Handler 或无需 API Key 认证 → 放行
                if (handlerOpt.isEmpty() || !requiresApiKey(handlerOpt.get())) {
                    return chain.filter(exchange);
                }
                Object handler = handlerOpt.get();

                // 3. 提取 API Key
                String apiKey = extractApiKey(exchange.getRequest());
                if (apiKey == null || apiKey.isEmpty()) {
                    return Mono.error(new LlmGatewayException(
                        LlmErrorCode.AUTH_INVALID_KEY, "缺少 API Key"));
                }

                // 4. 验证 API Key。
                // 关键：switchIfEmpty 必须放在【源头】—— validateApiKey 返回 Mono<Token>，
                // 有效时发射 token（非空值），无效时为空。用它判"无效的 API Key"语义正确。
                // 绝不能放在整条链（含 chain.filter）之后：chain.filter() 返回 Mono<Void>，
                // 业务成功执行完即"空完成"，会把成功误判为空结果（见下）。
                return apiKeyAuthService.validateApiKey(apiKey)
                    .switchIfEmpty(Mono.defer(() -> {
                        // 排查"无效的 API Key"时打印被拒 key 的遮罩首尾，便于对上日志/库里 key
                        log.warn("[API Key 认证失败] 无效的 API Key: key={}", KeyMaskUtil.mask(apiKey));
                        return Mono.error(new LlmGatewayException(LlmErrorCode.AUTH_INVALID_KEY, "无效的 API Key"));
                    }))
                    .flatMap(token -> {
                        // 5. 检查 API Key 状态（同步检查，失败直接抛异常会被 catch 捕获转 error）
                        try {
                            checkTokenStatus(exchange, token);
                        } catch (LlmGatewayException e) {
                            return Mono.error(e);
                        }

                        // 6. 检查用户状态。
                        // 关键：switchIfEmpty 同样只作用在 findById 上（找到用户发射非空值，
                        // 找不到才为空），并且放在 flatMap(...chain.filter...) 之前，
                        // 这样 chain.filter 的 Mono<Void> 空完成不会被误判为"用户不存在"。
                        return userUsersService.findById(token.getUserId())
                            .switchIfEmpty(Mono.defer(() -> {
                                log.warn("[API Key 认证] 用户不存在: tokenId={}, userId={}, key={}",
                                    token.getId(), token.getUserId(), KeyMaskUtil.mask(token.getApiKey()));
                                return Mono.error(new LlmGatewayException(
                                    LlmErrorCode.AUTH_FAILED, "用户不存在"));
                            }))
                            .flatMap(user -> checkUserStatusReactive(exchange, user)
                                .then(Mono.defer(() -> {
                                    // 7. 将认证信息存入上下文
                                    setUserContext(exchange, token, user);
                                    // 放行下游。chain.filter 返回 Mono<Void>，正常完成即空完成——
                                    // 这是预期行为，不能被任何 switchIfEmpty 捕获。
                                    return chain.filter(exchange);
                                })));
                    })
                    // 关键：整条链（含 chain.filter）之后【禁止】再有任何 switchIfEmpty。
                    // chain.filter() 的"成功空完成"会被误判为空结果。上面的判空已全部在源头处理，
                    // 这里用 then() 把 Mono<Token> 收敛为 Mono<Void> 返回给 WebFilter 契约。
                    .then();
            });
    }

    /**
     * 检查 API Key 状态
     * @throws LlmGatewayException 检查不通过时抛出
     */
    private void checkTokenStatus(ServerWebExchange exchange, ProxyTokensEntity token) {
        // 检查是否禁用
        if (token.getStatus() == null || token.getStatus() != SystemConstants.STATUS_ENABLED) {
            log.warn("[API Key 认证] API Key 已禁用: tokenId={}, key={}",
                token.getId(), KeyMaskUtil.mask(token.getApiKey()));
            throw new LlmGatewayException(LlmErrorCode.AUTH_INVALID_KEY, "API Key 已被禁用");
        }

        // 检查是否过期
        if (token.getExpiredAt() != null && token.getExpiredAt().isBefore(LocalDateTime.now())) {
            log.warn("[API Key 认证] API Key 已过期: tokenId={}, key={}",
                token.getId(), KeyMaskUtil.mask(token.getApiKey()));
            throw new LlmGatewayException(LlmErrorCode.AUTH_KEY_EXPIRED, token.getExpiredAt().toString());
        }
    }

    /**
     * 检查用户状态（Reactive 安全版本）
     * 返回 Mono.error 而不是同步抛异常，避免触发过滤器链重订阅
     */
    private Mono<Void> checkUserStatusReactive(ServerWebExchange exchange, UserUsersEntity user) {
        // 检查用户是否禁用（pt_users.status 为枚举字符串 ACTIVE/DISABLED，非旧库 Integer 0/1）
        if (user.getStatus() == null || !SystemConstants.USER_STATUS_ACTIVE.equals(user.getStatus())) {
            log.warn("[API Key 认证] 用户已禁用: userId={}", user.getId());
            return Mono.error(new LlmGatewayException(LlmErrorCode.AUTH_USER_DISABLED));
        }

        return Mono.empty(); // 检查通过
    }

    /**
     * 检查是否需要 API Key 认证
     */
    private boolean requiresApiKey(Object handler) {
        if (!(handler instanceof HandlerMethod)) {
            return false;
        }

        HandlerMethod handlerMethod = (HandlerMethod) handler;

        // 检查方法上的注解
        RequireApiKey methodAnnotation = handlerMethod.getMethodAnnotation(RequireApiKey.class);
        if (methodAnnotation != null) {
            return methodAnnotation.required();
        }

        // 检查类上的注解
        RequireApiKey classAnnotation = handlerMethod.getBeanType().getAnnotation(RequireApiKey.class);
        if (classAnnotation != null) {
            return classAnnotation.required();
        }

        return false;
    }

    /**
     * 从请求头中提取 API Key
     */
    private String extractApiKey(ServerHttpRequest request) {
        HttpHeaders headers = request.getHeaders();

        // 优先从 Authorization 头提取
        String authHeader = headers.getFirst(HttpHeaders.AUTHORIZATION);
        if (authHeader != null && authHeader.toLowerCase().startsWith("bearer ")) {
            return authHeader.substring(7).trim();
        }

        // 从 x-api-key 头提取（OpenAI 兼容）
        String apiKeyHeader = headers.getFirst("x-api-key");
        if (apiKeyHeader != null) {
            return apiKeyHeader.trim();
        }

        // 从 x-goog-api-key 头提取（Gemini/Vertex AI 兼容）
        String googApiKeyHeader = headers.getFirst("x-goog-api-key");
        if (googApiKeyHeader != null) {
            return googApiKeyHeader.trim();
        }

        // 从查询参数提取（OpenAI 兼容格式）
        String apiKeyParam = request.getQueryParams().getFirst("api_key");
        if (apiKeyParam != null) {
            return apiKeyParam.trim();
        }

        // 从查询参数提取（Gemini/Vertex AI 兼容格式：?key=xxx）
        return Optional.ofNullable(request.getQueryParams().getFirst("key"))
            .orElse(null);
    }

    /**
     * 设置用户上下文
     */
    private void setUserContext(ServerWebExchange exchange, ProxyTokensEntity token, UserUsersEntity user) {
        UserContext.setUserId(exchange, token.getUserId());
        UserContext.setTokenId(exchange, token.getId());
        UserContext.setTokenEntity(exchange, token);

        // 将用户信息存入 exchange attribute，供后续使用
        exchange.getAttributes().put("user", user);

        // INFO：用户 API Key 认证成功，打印 ID + 首尾遮罩 key，便于排查每次请求用哪个用户 key
        log.info("[API Key 认证成功] userId={}, tokenId={}, userKey={}, nickname={}",
            token.getUserId(), token.getId(), KeyMaskUtil.mask(token.getApiKey()), user.getNickname());
    }
}
