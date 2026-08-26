package com.llmate.multiprotocol.engine.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.util.KeyMaskUtil;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UrlUtils;
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Provider 适配器抽象基类
 * 提取所有 Provider 的公共逻辑，减少子类重复代码
 *
 * 公共能力：
 * 1. WebClient 构建与 baseUrl 规范化
 * 2. ObjectMapper 配置（忽略未知属性）
 * 3. 非流式/流式调用骨架方法
 * 4. 统一的日志辅助方法
 * 5. HTTP 错误处理
 * 6. 动态端点支持
 * 7. 多 Token 负载均衡（方案 C）：秒级时间戳取模选择
 *
 * 子类只需关注：
 * - getProviderAlias() / getProviderName()
 * - 认证方式（覆写 selectApiKey() 后在请求方法开头调用）
 * - 可选：覆写流式解析逻辑
 */
@Log4j2
public abstract class AbstractProviderAdapter implements ProviderAdapter {

    protected final WebClient webClient;
    protected final ObjectMapper objectMapper;
    protected final String baseUrl;

    /**
     * 多 Token 支持（方案 C）：该渠道下所有启用的 API Key 列表
     */
    protected final List<String> apiKeys;

    /**
     * 多 Token 支持（方案 C）：与 apiKeys 一一对应的 Token ID 列表
     */
    protected final List<Long> tokenIds;

    /**
     * 当前请求选中的 API Key（ThreadLocal 保证线程安全）
     */
    private final ThreadLocal<String> currentApiKey = new ThreadLocal<>();

    /**
     * 当前请求选中的 Token ID（ThreadLocal 保证线程安全）
     */
    private final ThreadLocal<Long> currentTokenId = new ThreadLocal<>();

    /**
     * 上游响应最大缓冲（字节）。
     * 生图接口返回的 base64 图片可达数 MB，Spring WebClient 默认 256KB 会抛
     * DataBufferLimitException: Exceeded limit on max bytes to buffer，这里统一放大。
     */
    private static final int MAX_RESPONSE_BUFFER_SIZE = 64 * 1024 * 1024;

    /**
     * base64 字符集（用于识别纯 base64 长串，脱敏上游请求/响应日志用）
     */
    private static final Pattern BASE64_PATTERN = Pattern.compile("[A-Za-z0-9+/=]+");

    /**
     * 构造基类（单 Token 模式，兼容旧代码）
     * @param baseUrl 上游 API 基址（会自动补 /）
     * @param webClientBuilder 预配置好认证 Header 的 WebClient.Builder
     * @param objectMapper 共享的 ObjectMapper（ProviderFactory 传入 Spring 单例；Spring Boot 默认已禁用 FAIL_ON_UNKNOWN_PROPERTIES）
     */
    protected AbstractProviderAdapter(String baseUrl, WebClient.Builder webClientBuilder, ObjectMapper objectMapper) {
        this(baseUrl, webClientBuilder, objectMapper, null, null);
    }

    /**
     * 构造基类（多 Token 模式，方案 C）
     * @param baseUrl 上游 API 基址（会自动补 /）
     * @param webClientBuilder 预配置好认证 Header 的 WebClient.Builder
     * @param objectMapper 共享的 ObjectMapper
     * @param apiKeys 多 Token 列表（可为 null，则退化为单 Token 模式）
     * @param tokenIds 与 apiKeys 对应的 Token ID 列表（可为 null）
     */
    protected AbstractProviderAdapter(String baseUrl, WebClient.Builder webClientBuilder, ObjectMapper objectMapper,
                                      List<String> apiKeys, List<Long> tokenIds) {
        this.baseUrl = normalizeBaseUrl(baseUrl);
        // 使用 ConnectionProvider.newConnection() 禁用连接池，每次请求新建 TCP+TLS 连接。
        // 国内服务器出站经阿里云 NAT，NAT 空闲超时后会断开池中旧连接，Reactor Netty 默认
        // DefaultPooledConnectionProvider 不知情，拿出来复用 → Connection reset by peer。
        // 本机无 NAT 中间盒所以不触发。curl/JDK HttpClient 每次新建连接也不触发。
        // newConnection() 无连接复用开销可接受（LLM 请求本身 latency 远大于 TCP+TLS 握手）。
        // 统一由 WebClientUtils 构建 HttpClient，避免各处重复样板。
        HttpClient httpClient = WebClientUtils.newConnHttpClient(
                Duration.ofSeconds(SystemConstants.HTTP_TIMEOUT_UPSTREAM_SECONDS));
        this.webClient = webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .baseUrl(this.baseUrl)
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(MAX_RESPONSE_BUFFER_SIZE))
                .build();
        this.objectMapper = objectMapper;
        this.apiKeys = (apiKeys != null && !apiKeys.isEmpty()) ? apiKeys : new ArrayList<>();
        this.tokenIds = (tokenIds != null && !tokenIds.isEmpty()) ? tokenIds : new ArrayList<>();

        if (!this.apiKeys.isEmpty() && this.tokenIds.size() != this.apiKeys.size()) {
            log.warn("[{}] apiKeys 和 tokenIds 长度不匹配: apiKeys={}, tokenIds={}",
                getProviderName(), this.apiKeys.size(), this.tokenIds.size());
        }
    }

    // ==================== 多 Token 选择（方案 C） ====================

    /**
     * 按秒级时间戳取模选择当前请求使用的 API Key
     * 无状态公平分配：每秒自动切换，流量自然分散
     *
     * @return 选中的 API Key
     */
    protected String selectApiKey() {
        if (apiKeys.isEmpty()) {
            // 退化到单 Token 模式：从 webClient 的默认 header 取（子类需在构造时设置）
            log.debug("[{}] 无多 Token 配置，使用默认 API Key", getProviderName());
            return null;
        }

        // 秒级时间戳取模
        int index = Math.floorMod((int) (System.currentTimeMillis() / 1000), apiKeys.size());
        String selectedKey = apiKeys.get(index);
        Long selectedTokenId = tokenIds.size() > index ? tokenIds.get(index) : null;

        currentApiKey.set(selectedKey);
        currentTokenId.set(selectedTokenId);

        log.debug("[{}] 选择 Token: index={}, tokenId={}", getProviderName(), index, selectedTokenId);
        return selectedKey;
    }

    /**
     * 获取当前请求选中的 API Key（供子类构建请求头使用）
     * @return 当前 API Key，如未选择则返回 null
     */
    protected String getCurrentApiKey() {
        return currentApiKey.get();
    }

    /**
     * 获取当前请求选中的 Token ID（供 reportTokenUsage 使用）
     * 实现 ProviderAdapter 接口
     * @return 当前 Token ID，如未选择则返回 null
     */
    @Override
    public Long getCurrentTokenId() {
        return currentTokenId.get();
    }

    /**
     * 清除当前线程的 Token 选择（防止线程池复用导致污染）
     * 子类在请求处理完毕后应调用此方法
     */
    protected void clearCurrentToken() {
        currentApiKey.remove();
        currentTokenId.remove();
    }

    /**
     * 打印"调用上游接口时"使用的 用户 API Key 与 渠道 API Key 的排查日志：
     * ID + 首尾保留、中间星号遮罩（KeyMaskUtil.mask），便于对 requestId/tokenId 定位问题，绝不打印完整 key。
     *
     * OpenAI 兼容系渠道：WebClient 的认证 Header 在构造时用【首个 Token】固定
     * （ProviderFactory 取 config.getApiKey() = apiKeys.get(0)），当前不按请求轮换，
     * 因此渠道 Token ID 与 Key 取 tokenIds/apiKeys 首个。
     *
     * @param req 内部请求（携带 LlmGateway 填充的用户 key 信息；可为 null，如 count_tokens 透传）
     * @param label 请求类型，如 "非流式" / "流式"
     */
    protected void logUpstreamKeys(LlmChatRequest req, String label) {
        log.info("[{}] 调用上游 keys ({}): {}",
            getProviderName(), label,
            KeyMaskUtil.describeKeys(
                req != null ? req.getUserTokenId() : null,
                req != null ? req.getUserApiKey() : null,
                tokenIds.isEmpty() ? null : tokenIds.get(0),
                apiKeys.isEmpty() ? null : apiKeys.get(0)));
    }

    // ==================== 公共骨架方法 ====================

    /**
     * 执行非流式 HTTP POST 调用
     * @param uri 请求路径（相对于 baseUrl）
     * @param request 上游请求体
     * @param respClass 上游响应类型
     * @param converter 上游响应 → 内部标准响应的转换函数
     */
    protected <REQ, RESP> Mono<LlmChatResponse> doPostBlocking(
            String uri, REQ request, Class<RESP> respClass,
            java.util.function.Function<RESP, LlmChatResponse> converter) {

        // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入），供上游请求/响应日志使用
        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            // 关键修复：uri 去掉前导斜杠，作为相对路径传给 WebClient。
            // 若 uri 以 / 开头，WebClient 会用绝对路径替换 baseUrl 的路径部分（丢掉 /claude 这类路径段）。
            // baseUrl 已归一化为单斜杠结尾，相对路径追加后拼接正确。
            String relativePath = UrlUtils.stripLeadingSlash(uri);

            // 输出带方框的上游请求日志（请求体经 maskForLog 脱敏，避免 base64 大段入日志）
            LogBox.logUpstreamRequest(getProviderName(), UrlUtils.join(baseUrl, uri), maskForLog(request), requestId, userId);

            return webClient.post()
                    .uri(relativePath)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(respClass)
                    // 同步打印上游响应日志：响应体经 maskForLog 脱敏后体积很小（base64 变占位符），
                    // 不会阻塞 Netty EventLoop；同步保证日志顺序确定（上游渠道响应 → 用量提取 → 计费明细…），
                    // 与文本聊天、生图、生视频等所有请求类型的日志标准一致。
                    .doOnNext(resp -> LogBox.logUpstreamResponse(getProviderName(), maskForLog(resp), requestId, userId))
                    .map(converter::apply)
                    .doOnError(e -> logError("非流式", e));
        });
    }

    /**
     * 执行非流式 HTTP POST 调用（带动态端点）
     * @param uri 请求路径（相对于 baseUrl）
     * @param request 上游请求体
     * @param respClass 上游响应类型
     * @param converter 上游响应 → 内部标准响应的转换函数
     * @param endpointConfig 端点配置（可为null）
     */
    protected <REQ, RESP> Mono<LlmChatResponse> doPostBlocking(
            String uri, REQ request, Class<RESP> respClass,
            java.util.function.Function<RESP, LlmChatResponse> converter,
            ModelEndpointConfig endpointConfig) {

        // 如果有自定义端点配置，构建新的 WebClient
        if (endpointConfig != null && endpointConfig.getFullUrl() != null && !endpointConfig.getFullUrl().isEmpty()) {
            return doPostBlockingWithFullUrl(endpointConfig.getFullUrl(), request, respClass, converter);
        }

        // 否则使用默认配置
        return doPostBlocking(uri, request, respClass, converter);
    }

    /**
     * 使用完整 URL 执行非流式 HTTP POST 调用
     */
    protected <REQ, RESP> Mono<LlmChatResponse> doPostBlockingWithFullUrl(
            String fullUrl, REQ request, Class<RESP> respClass,
            java.util.function.Function<RESP, LlmChatResponse> converter) {

        // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入），供上游请求/响应日志使用
        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            // 输出带方框的上游请求日志（请求体经 maskForLog 脱敏）
            LogBox.logUpstreamRequest(getProviderName(), fullUrl, maskForLog(request), requestId, userId);

            return webClient.mutate()
                    .build()
                    .post()
                    .uri(fullUrl)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(respClass)
                    // 同步打印上游响应日志（maskForLog 脱敏后体积很小，不阻塞 Netty；保证日志顺序确定）
                    .doOnNext(resp -> LogBox.logUpstreamResponse(getProviderName(), maskForLog(resp), requestId, userId))
                    .map(converter::apply)
                    .doOnError(e -> logError("非流式", e));
        });
    }

    /**
     * 执行流式 HTTP POST 调用，返回原始 SSE 文本流
     * 子类可在此基础上进行协议特定的 SSE 解析
     * @param uri 请求路径
     * @param request 上游请求体
     */
    protected <REQ> Flux<String> doPostStreamRaw(String uri, REQ request) {
        // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入），供上游请求日志使用
        return Flux.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            // 关键修复：uri 去掉前导斜杠，作为相对路径传给 WebClient（同非流式，见 doPostBlocking 注释）
            String relativePath = UrlUtils.stripLeadingSlash(uri);

            // 输出带方框的上游请求日志（流式，请求体经 maskForLog 脱敏）
            LogBox.logUpstreamRequest(getProviderName(), UrlUtils.join(baseUrl, uri), maskForLog(request), requestId, userId);

            return webClient.post()
                    .uri(relativePath)
                    .header(HttpHeaders.ACCEPT, MediaType.TEXT_EVENT_STREAM_VALUE)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToFlux(String.class)
                    .doOnError(e -> logError("流式", e));
        });
    }

    /**
     * 执行流式 HTTP POST 调用（带动态端点）
     */
    protected <REQ> Flux<String> doPostStreamRaw(String uri, REQ request, ModelEndpointConfig endpointConfig) {
        // 如果有自定义端点配置，使用完整 URL
        if (endpointConfig != null && endpointConfig.getFullUrl() != null && !endpointConfig.getFullUrl().isEmpty()) {
            return doPostStreamRawWithFullUrl(endpointConfig.getFullUrl(), request);
        }

        // 否则使用默认配置
        return doPostStreamRaw(uri, request);
    }

    /**
     * 使用完整 URL 执行流式 HTTP POST 调用
     */
    protected <REQ> Flux<String> doPostStreamRawWithFullUrl(String fullUrl, REQ request) {
        // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入），供上游请求日志使用
        return Flux.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            // 输出带方框的上游请求日志（流式，请求体经 maskForLog 脱敏）
            LogBox.logUpstreamRequest(getProviderName(), fullUrl, maskForLog(request), requestId, userId);

            return webClient.mutate()
                    .build()
                    .post()
                    .uri(fullUrl)
                    .header(HttpHeaders.ACCEPT, MediaType.TEXT_EVENT_STREAM_VALUE)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToFlux(String.class)
                    .doOnError(e -> logError("流式", e));
        });
    }

    // ==================== 公共 SSE 解析工具 ====================

    /**
     * 标准 OpenAI 兼容 SSE 解析：过滤 [DONE]、提取 data: 后的 JSON
     * 适用于 DashScope、DeepSeek 等标准 OpenAI 兼容渠道
     */
    protected Flux<String> parseOpenAiCompatibleSse(Flux<String> rawSseLines) {
        return rawSseLines
                .filter(line -> line != null && !line.isBlank()
                        && !"[DONE]".equals(line.trim())
                        && !"data: [DONE]".equals(line.trim()))
                .map(line -> {
                    String json = line.trim();
                    if (json.startsWith("data:")) {
                        json = json.substring(5).trim();
                    }
                    return json;
                })
                .filter(json -> !json.isEmpty() && json.startsWith("{"));
    }

    /**
     * 将 JSON 字符串反序列化为指定类型，失败时返回 null（不中断流）
     */
    protected <T> T safeReadValue(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json, clazz);
        } catch (Exception e) {
            log.warn("[{}] JSON 反序列化失败: {} | 原文: {}", getProviderName(), e.getMessage(), json);
            return null;
        }
    }

    // ==================== 公共日志方法 ====================

    /**
     * 记录请求日志（自动序列化请求体，经 maskForLog 脱敏避免 base64 大段入日志）
     * 注意：使用 fullUrl 参数而不是 baseUrl + uri 拼接，避免双斜杠问题
     */
    protected void logRequest(String mode, String fullUrl, Object request) {
        try {
            log.info("[{}] 请求地址: POST {}", getProviderName(), fullUrl);
            log.info("[{}] 请求体: {}", getProviderName(), objectMapper.writeValueAsString(maskForLog(request)));
        } catch (Exception ex) {
            log.warn("[{}] 无法序列化请求体日志", getProviderName(), ex);
        }
    }

    /**
     * 上游请求/响应日志脱敏：把大体积 base64（dataUri 或纯 base64 长串）替换为占位符，
     * 避免生图/生视频等接口把整段图片 base64 打进日志，淹没后续的计费/结算日志块。
     *
     * 只作用于日志展示（Map/List 构建副本、JsonNode 先 deepCopy），不修改实际数据。
     * 支持 Map/List/String/JsonNode，其它 POJO 经 Jackson 转成 JsonNode 树再脱敏。
     */
    protected Object maskForLog(Object obj) {
        if (obj instanceof JsonNode node) {
            JsonNode copy = node.deepCopy();
            maskJsonNode(copy);
            return copy;
        }
        if (obj instanceof String s) {
            String masked = maskBase64String(s);
            return masked != null ? masked : s;
        }
        if (obj instanceof Map<?, ?> map) {
            Map<String, Object> masked = new LinkedHashMap<>();
            map.forEach((k, v) -> masked.put(String.valueOf(k), maskForLog(v)));
            return masked;
        }
        if (obj instanceof List<?> list) {
            List<Object> masked = new ArrayList<>();
            list.forEach(v -> masked.add(maskForLog(v)));
            return masked;
        }
        // POJO 等其它对象：转成 JsonNode 树脱敏（只影响日志副本）
        if (obj != null) {
            try {
                JsonNode tree = objectMapper.valueToTree(obj);
                maskJsonNode(tree);
                return tree;
            } catch (Exception e) {
                // 序列化失败则原样返回
            }
        }
        return obj;
    }

    /**
     * 递归脱敏 JsonNode（就地修改，调用方需先 deepCopy）
     */
    private void maskJsonNode(JsonNode node) {
        if (node.isObject()) {
            List<String> keys = new ArrayList<>();
            node.fieldNames().forEachRemaining(keys::add);
            for (String key : keys) {
                JsonNode child = node.get(key);
                if (child.isTextual()) {
                    String masked = maskBase64String(child.asText());
                    if (masked != null) {
                        ((ObjectNode) node).put(key, masked);
                    }
                } else {
                    maskJsonNode(child);
                }
            }
        } else if (node.isArray()) {
            for (JsonNode child : node) {
                maskJsonNode(child);
            }
        }
    }

    /**
     * 判断字符串是否含大段 base64 并返回脱敏后的值；无需脱敏返回 null。
     * 覆盖两种情况：dataUri（...;base64,XXXX）与纯 base64 长串（>10KB 且仅含 base64 字符集）。
     */
    private String maskBase64String(String s) {
        int idx = s.indexOf(";base64,");
        if (idx >= 0) {
            String prefix = s.substring(0, Math.min(idx + 8, 64));
            return prefix + "[omitted, " + s.length() + " chars]";
        }
        // 纯 base64 长串：>10KB、长度是 4 的倍数（base64 编码恒为 4 的倍数）、仅含 base64 字符集
        if (s.length() > 10_000 && s.length() % 4 == 0 && BASE64_PATTERN.matcher(s).matches()) {
            return "[base64 omitted, " + s.length() + " chars]";
        }
        return null;
    }

    /**
     * 记录错误日志（自动提取上游响应体）
     */
    protected void logError(String mode, Throwable e) {
        log.error("[{}] {}调用失败", getProviderName(), mode, e);
        if (e instanceof WebClientResponseException wcre) {
            log.error("[{}] 上游响应体: {}", getProviderName(), wcre.getResponseBodyAsString());
        }
    }

    /**
     * 把上游 HTTP 错误（WebClientResponseException）翻译成带真实原因的 LlmGatewayException，
     * 并向上传播（禁止吞成笼统的 "400 Bad Request from POST ..."）。
     *
     * 取错误优先级：响应体 JSON 的 error.message > error.Message > 响应体原文 > 异常自带 message。
     * 这样客户端能直接看到方舟/百炼返回的具体原因（如 "the specified asset is not a video"），
     * 而不是只有 URL 的 400/500 笼统消息。
     */
    protected Throwable translateUpstreamError(Throwable e) {
        if (!(e instanceof WebClientResponseException wcre)) {
            return e;
        }
        String upstreamMsg = extractUpstreamErrorMessage(wcre.getResponseBodyAsString());
        if (upstreamMsg == null || upstreamMsg.isBlank()) {
            upstreamMsg = e.getMessage();
        }
        // 保 cause 链：原 varargs 构造器丢弃 cause，内部日志必须能看到上游 WebClientResponseException
        // （含 URL/状态/响应体/堆栈）。对外文案由响应构建器统一脱敏（PROVIDER_ERROR → 通用文案）。
        return new LlmGatewayException(LlmErrorCode.PROVIDER_ERROR,
                "Provider '" + getProviderName() + "' 调用失败：" + upstreamMsg, wcre);
    }

    /**
     * 从上游错误响应体提取 error.message（兼容 OpenAI 风格 {error:{message}} / {Error:{Message}} /
     * {message} / 纯文本）；提取不到返回 null。
     */
    private String extractUpstreamErrorMessage(String body) {
        if (body == null || body.isBlank()) {
            return null;
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            JsonNode err = root.path("error");
            if (err.isObject()) {
                String msg = firstText(err, "message", "Message");
                if (msg != null) {
                    return msg;
                }
                String code = firstText(err, "code", "Code", "type", "Type");
                if (code != null) {
                    return code;
                }
            }
            String top = firstText(root, "message", "Message", "error");
            if (top != null && !top.equalsIgnoreCase(body)) {
                return top;
            }
        } catch (Exception ignore) {
            // 响应体非 JSON，按纯文本处理
        }
        return body.trim();
    }

    private String firstText(JsonNode node, String... keys) {
        for (String k : keys) {
            JsonNode v = node.get(k);
            if (v != null && v.isValueNode() && !v.asText().isEmpty()) {
                return v.asText();
            }
        }
        return null;
    }

    // ==================== 工具方法 ====================

    private String normalizeBaseUrl(String url) {
        if (url == null || url.isEmpty()) {
            return "";
        }
        return url.endsWith("/") ? url : url + "/";
    }
}
