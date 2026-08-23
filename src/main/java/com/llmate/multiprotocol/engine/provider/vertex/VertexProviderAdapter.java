package com.llmate.multiprotocol.engine.provider.vertex;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.converter.upstream.VertexFormatConverter;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentRequest;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentResponse;
import com.llmate.multiprotocol.engine.provider.ProviderAdapter;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UrlUtils;
import com.llmate.multiprotocol.util.WebClientUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Google Gemini API Provider 适配器（原 Vertex AI，现改为 Gemini 原生 API）
 * 支持模型前缀: "vertex/"
 * 例如：vertex/gemini-2.0-flash, vertex/gemini-2.0-pro
 *
 * 上游通信协议：Google Gemini 原生 API 格式
 * 端点格式：
 *   非流式: POST {baseUrl}/models/{model}:generateContent
 *   流式:   POST {baseUrl}/models/{model}:streamGenerateContent
 *
 * 认证方式：API Key（通过 x-goog-api-key header 或 query param）
 * 格式转换委派给 VertexFormatConverter，本类负责 HTTP 调用和 SSE 事件解析
 *
 * 注意：本类不由 Spring 自动扫描创建，而是由 ProviderFactory 根据配置手动实例化
 *
 * 方案 C：支持多 Token 负载均衡（秒级时间戳取模）
 */
@Log4j2
public class VertexProviderAdapter implements ProviderAdapter {

    private final WebClient webClient;
    private final HttpClient httpClient;
    private final VertexFormatConverter formatConverter;
    private final ObjectMapper objectMapper;
    private final String baseUrl;
    private final String providerName;
    private final String providerAlias;

    // 方案 C：多 Token 支持
    private final List<String> apiKeys;
    private final List<Long> tokenIds;
    private final ThreadLocal<String> currentApiKey = new ThreadLocal<>();
    private final ThreadLocal<Long> currentTokenId = new ThreadLocal<>();

    public VertexProviderAdapter(String baseUrl, String apiKey,
            VertexFormatConverter formatConverter, String providerName, String providerAlias,
            ObjectMapper objectMapper, List<String> apiKeys, List<Long> tokenIds) {

        // 规范化 baseUrl（确保以 / 结尾），保证相对路径追加拼接正确
        this.baseUrl = UrlUtils.withTrailingSlash(baseUrl);
        this.providerName = providerName;
        this.providerAlias = providerAlias;

        // 方案 C：多 Token 支持
        this.apiKeys = (apiKeys != null && !apiKeys.isEmpty()) ? apiKeys : List.of(apiKey);
        this.tokenIds = (tokenIds != null && !tokenIds.isEmpty()) ? tokenIds : new ArrayList<>();

        // 共享 HttpClient（含自定义 TLS 握手超时），必须先于 WebClient 初始化。
        // 所有 WebClient 复用一个 HttpClient → 连接池按同一 SslContext 复用，避免每次请求新建连接。
        this.httpClient = buildHttpClient();
        // 初始 WebClient（使用第一个 key）
        this.webClient = buildWebClient(apiKey);
        this.formatConverter = formatConverter;
        this.objectMapper = objectMapper;
    }

    /**
     * 构建共享 HttpClient（单例，进程内复用，保证 TLS 连接池跨请求复用）
     * 上游 api.vapeur.ai TLS 握手偶发超过 Netty 默认 10s，实测会触发 SslHandshakeTimeoutException
     * 把正常请求误判为失败，故放宽握手超时到 30s（默认信任库，不影响标准 HTTPS 校验）
     */
    private HttpClient buildHttpClient() {
        // ConnectionProvider.newConnection() 禁用连接池，每次请求新建 TCP+TLS 连接，
        // 避免国内服务器 NAT 空闲超时后池中旧连接被远端 RST（同 AbstractProviderAdapter）。
        // 统一由 WebClientUtils 构建 HttpClient（含放宽的 SSL 握手超时 30s）。
        return WebClientUtils.newConnHttpClient(
                Duration.ofSeconds(SystemConstants.HTTP_TIMEOUT_UPSTREAM_SECONDS),
                Duration.ofSeconds(SystemConstants.HTTP_TIMEOUT_SSL_HANDSHAKE_SECONDS));
    }

    /**
     * 构建 WebClient（复用共享 httpClient，共享连接池）
     */
    private WebClient buildWebClient(String apiKey) {
        WebClient.Builder builder = WebClient.builder()
                .baseUrl(this.baseUrl)
                .clientConnector(new ReactorClientHttpConnector(this.httpClient));

        // 认证：Gemini 原生 API 使用 Bearer token
        if (apiKey != null && !apiKey.isEmpty()) {
            builder.defaultHeader("Authorization", "Bearer " + apiKey);
        }

        builder.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);
        return builder.build();
    }

    /**
     * 方案 C：按秒级时间戳取模选择当前请求使用的 API Key
     */
    private String selectApiKey() {
        if (apiKeys.isEmpty()) {
            return null;
        }
        int index = Math.floorMod((int) (System.currentTimeMillis() / 1000), apiKeys.size());
        String selectedKey = apiKeys.get(index);
        Long selectedTokenId = tokenIds.size() > index ? tokenIds.get(index) : null;

        currentApiKey.set(selectedKey);
        currentTokenId.set(selectedTokenId);

        log.debug("[{}] 选择 Token: index={}, tokenId={}", getProviderName(), index, selectedTokenId);
        return selectedKey;
    }

    @Override
    public Long getCurrentTokenId() {
        return currentTokenId.get();
    }

    /**
     * 获取当前 WebClient（根据选择的 Token）
     */
    private WebClient getCurrentWebClient() {
        String selectedKey = selectApiKey();
        if (selectedKey != null) {
            return buildWebClient(selectedKey);
        }
        return webClient;
    }

    @Override
    public String getProviderAlias() {
        return providerAlias;
    }

    @Override
    public ProtocolType getNativeProtocol() {
        return ProtocolType.GOOGLE_GEMINI; // Vertex 使用与 Gemini 相同的协议格式
    }

    @Override
    public boolean supports(String modelPath) {
        return modelPath != null && modelPath.startsWith(providerAlias + "/");
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    /**
     * 构建 Gemini API 请求路径
     * - 有端点配置且 endpointPath 含 {model} 占位符：使用配置的完整路径（替换 {model}），
     *   并把端点动作对齐本次调用类型（非流式→generateContent，流式→streamGenerateContent）
     * - 否则（无端点配置）：动态拼接 models/{model}:generateContent 或 :streamGenerateContent
     */
    private String buildUri(String modelName, ModelEndpointConfig endpointConfig, boolean stream) {
        if (endpointConfig != null && endpointConfig.getEndpointPath() != null
                && endpointConfig.getEndpointPath().contains("{model}")) {
            String path = endpointConfig.getEndpointPath().replace("{model}", modelName);
            return alignAction(path, stream);
        }
        String action = stream ? "streamGenerateContent" : "generateContent";
        return String.format("models/%s:%s", modelName, action);
    }

    /**
     * 对齐 Gemini 端点动作后缀：非流式 → generateContent，流式 → streamGenerateContent。
     *
     * 若不对齐，配置了 stream 模板（:streamGenerateContent）的模型在非流式请求时也会命中
     * stream 端点，上游返回流式 NDJSON，与非流式解析（bodyToMono 期望单个 JSON）不匹配，
     * 导致"请求 stream=false 却得到流式响应"。
     * 兼容冒号与斜杠两种写法（models/{model}:generateContent 与 models/{model}/generateContent）。
     */
    private String alignAction(String path, boolean stream) {
        if (stream) {
            if (path.endsWith(":generateContent")) {
                return path.substring(0, path.length() - ":generateContent".length()) + ":streamGenerateContent";
            }
            if (path.endsWith("/generateContent")) {
                return path.substring(0, path.length() - "/generateContent".length()) + "/streamGenerateContent";
            }
        } else {
            if (path.endsWith(":streamGenerateContent")) {
                return path.substring(0, path.length() - ":streamGenerateContent".length()) + ":generateContent";
            }
            if (path.endsWith("/streamGenerateContent")) {
                return path.substring(0, path.length() - "/streamGenerateContent".length()) + "/generateContent";
            }
        }
        return path;
    }

    /**
     * 计算最终请求地址：base_url + 请求路径
     * baseUrl 直接取端点配置（实时读库，避免内存缓存旧 baseUrl），仅在配置缺失时用渠道 baseUrl
     */
    private String buildRequestUrl(ModelEndpointConfig endpointConfig, String path) {
        String effectiveBase = endpointConfig != null && endpointConfig.getBaseUrl() != null
                && !endpointConfig.getBaseUrl().isEmpty() ? endpointConfig.getBaseUrl() : baseUrl;
        return UrlUtils.join(effectiveBase, path);
    }

    /**
     * 非流式调用（默认端点）
     * POST models/{model}:generateContent
     */
    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest internalReq) {
        return chat(internalReq, null);
    }

    /**
     * 非流式调用（带自定义端点配置）
     * 最终请求地址 = 端点配置 baseUrl + endpointPath（含 {model} 占位符替换），
     * 保证渠道 baseUrl 的路径段不被丢弃
     */
    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest internalReq, ModelEndpointConfig endpointConfig) {
        return Mono.deferContextual(ctxView -> {
            log.info("[Vertex] 开始非流式调用");
            VertexGenerateContentRequest request = formatConverter.toVertexRequest(internalReq);

            // 提取模型名用于构建 URL
            String modelName = extractModelName(internalReq.getModel());
            String path = buildUri(modelName, endpointConfig, false);
            String requestUrl = buildRequestUrl(endpointConfig, path);

            // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入）
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            logRequest("非流式", requestUrl, request, requestId, userId);

            return getCurrentWebClient().post()
                    .uri(requestUrl)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(VertexGenerateContentResponse.class)
                    .map(formatConverter::toInternalResponse)
                    .doOnNext(resp -> {
                        log.info("[Vertex] 非流式响应完成: id={}", resp.getId());
                        // 上游响应体走 LogBox 方框日志；异步执行避免大响应体序列化阻塞 Netty 事件循环线程
                        Mono.fromRunnable(() -> LogBox.logUpstreamResponse(providerName, resp, requestId, userId))
                                .subscribeOn(Schedulers.boundedElastic())
                                .subscribe();
                    })
                    .doOnError(e -> logError("非流式", e));
        });
    }

    /**
     * 流式调用（默认端点）
     * POST models/{model}:streamGenerateContent
     *
     * Gemini API 流式响应格式（不使用 ?alt=sse 参数）：
     *   - 上游可能返回 compact JSON（单行）或 pretty-printed JSON（多行缩进）
     *   - 不再逐行解析（NDJSON），而是收集所有行拼接后作为完整 JSON 解析
     *
     * 解析策略: bodyToFlux(String) → 过滤空行/注释 → collectList → join → JSON 反序列化
     */
    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalReq) {
        return chatStream(internalReq, null);
    }

    /**
     * 流式调用（带自定义端点配置）
     * 与 {@link #chat(LlmChatRequest, ModelEndpointConfig)} 相同，请求地址按端点配置拼接
     */
    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalReq, ModelEndpointConfig endpointConfig) {
        return Flux.deferContextual(ctxView -> {
            log.info("[Vertex] 开始流式调用");
            VertexGenerateContentRequest request = formatConverter.toVertexRequest(internalReq);

            // 提取模型名用于构建 URL
            String modelName = extractModelName(internalReq.getModel());
            // 不添加 alt=sse 参数，直接调用 streamGenerateContent 端点
            String path = buildUri(modelName, endpointConfig, true);
            String requestUrl = buildRequestUrl(endpointConfig, path);

            // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入）
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            logRequest("流式", requestUrl, request, requestId, userId);

            return getCurrentWebClient().post()
                .uri(requestUrl)
                .bodyValue(request)
                .accept(MediaType.TEXT_EVENT_STREAM) // SSE 格式：上游返回 data: {...} 事件流
                .retrieve()
                .bodyToMono(String.class)
                .doOnSubscribe(sub -> log.info("[Vertex] 开始订阅响应流"))
                .flatMapMany(body -> {
                    List<String> sseEvents = parseSseEvents(body);
                    log.debug("[Vertex] SSE解析出{}个事件", sseEvents.size());
                    if (sseEvents.isEmpty()) {
                        log.warn("[Vertex] SSE解析后无有效事件");
                        return Flux.empty();
                    }
                    return Flux.fromIterable(sseEvents)
                        .map(json -> {
                            // 上游 SSE 事件原始内容完整打印（调试流式问题最关键的原始数据）。
                            // 经 maskSseForLog 仅把超长 base64（≥100 字符连续 base64 字符集，如图片二进制）置为占位符，
                            // 文本 / 工具调用 / 用量元数据等内容原样完整输出，不截断。
                            log.debug("[Vertex] SSE事件JSON: {}", maskSseForLog(json));
                            try {
                                // 上游错误事件（{"error": {...}}）必须向上传播，禁止吞成空 chunk：
                                // 否则客户端收到空流（表现为"工具结果没上报/AI 无输出"），
                                // 且结算被记成成功 0 tokens，真实原因被静默隐藏。
                                JsonNode node = objectMapper.readTree(json);
                                JsonNode err = node.get("error");
                                if (err != null && !err.isNull()) {
                                    String upstreamMsg = err.path("message").asText(json);
                                    log.error("[Vertex] 上游返回错误: code={}, message={}",
                                            err.path("code").asText(""), upstreamMsg);
                                    throw new LlmGatewayException(
                                            LlmErrorCode.PROVIDER_ERROR, "vertex", upstreamMsg);
                                }
                                VertexGenerateContentResponse vertexResp =
                                        objectMapper.treeToValue(node, VertexGenerateContentResponse.class);
                                return formatConverter.toInternalStreamChunk(vertexResp);
                            } catch (JsonProcessingException e) {
                                log.warn("[Vertex] JSON解析失败: {} | 数据前200字符: {}",
                                        e.getMessage(),
                                        json.length() > 200 ? json.substring(0, 200) : json);
                                LlmStreamChunk empty = new LlmStreamChunk();
                                empty.setDeltaContent("");
                                return empty;
                            }
                        });
                })
                .doOnNext(chunk -> log.debug("[Vertex] 输出内部chunk: deltaContent={}, toolCallId={}, toolCallName={}, toolCallArgs={}, finishReason={}, finished={}",
                        chunk.getDeltaContent(), chunk.getToolCallId(), chunk.getToolCallName(),
                        chunk.getToolCallArgumentsDelta(), chunk.getFinishReason(), chunk.isFinished()))
                .doOnComplete(() -> log.info("[Vertex] 流式调用完成"))
                .doOnError(e -> logError("流式", e));
                // 注意：不再用 onErrorResume 把上游异常吞成空 chunk。
                // 之前吞掉后 LlmGateway 的 doOnError→abortStream（记录失败）和控制器 SSE error 事件
                // 都不会触发，流式失败被 settleStream 记为"成功 0 tokens"，客户端收到空流。
                // 现在让错误正常传播：网关 abortStream 记真实失败，SSE error 事件发给客户端。
        });
    }

    // ==================== 流式响应解析 ====================

    /**
     * 从 SSE 行中提取 data: 后的内容（保留用于 SSE 格式兼容）
     * SSE 格式: data: {...} 或 data:{...}
     */
    private String extractSseData(String line) {
        if (line.startsWith("data: ")) {
            return line.substring(6);  // 跳过 "data: "
        } else if (line.startsWith("data:")) {
            return line.substring(5);  // 跳过 "data:"
        }
        // 如果没有 data: 前缀，直接返回原行（兼容模式）
        return line;
    }

    /**
     * SSE 事件日志脱敏：仅把疑似超长 base64（连续 ≥100 字符的 base64 字符集，如图片二进制）替换为占位符，
     * 防止生图类模型（native_image）把大段 base64 灌进日志。
     * 聊天流式 SSE 的文本 / 工具调用 / 用量元数据不含此类超长 base64 片段，会原样完整打印。
     * 判定保守：正常文本因含空格/标点（不在 base64 字符集内）会被打断，不会误伤。
     */
    private static String maskSseForLog(String json) {
        return json.replaceAll("[A-Za-z0-9+/=]{100,}", "[base64]");
    }

    /**
     * 解析 SSE 格式的流式响应体，提取每个 data: 事件为独立 JSON 字符串。
     *
     * Gemini streamGenerateContent 返回标准 SSE 流：每个 {@code data:} 行是一个完整的
     * GenerateContentResponse JSON，事件之间以空行分隔。每个事件对应一次增量生成。
     *
     * <p>重要：不能把多行 data 直接拼接成一个字符串再用 Jackson 解析——
     * Jackson 的 readValue 只读第一个完整 JSON 对象，后续事件（含剩余内容和 token 统计）
     * 会被静默丢弃，导致响应截断 + usage 归零。
     *
     * @param body 原始 SSE 响应体
     * @return 每个事件的完整 JSON 字符串列表，顺序保持上游返回顺序
     */
    private List<String> parseSseEvents(String body) {
        List<String> events = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String line : body.split("\n")) {
            String trimmed = line.strip();
            if (trimmed.isEmpty()) {
                // SSE 事件边界：空行表示当前事件结束
                if (current.length() > 0) {
                    events.add(current.toString());
                    current.setLength(0);
                }
                continue;
            }
            // SSE 注释行（如 :ok、:ping），直接跳过
            if (trimmed.startsWith(":") && !trimmed.startsWith("data:")) continue;
            // 提取 data: 后的 JSON 内容
            String data = extractSseData(trimmed);
            current.append(data);
        }
        // 最后一个事件（上游可能不以空行结尾）
        if (current.length() > 0) {
            events.add(current.toString());
        }
        return events;
    }

    /**
     * @deprecated 已替换为 {@link #parseSseEvents(String)}，保留用于兼容。
     *             直接拼接多事件会导致 Jackson 只解析第一个 JSON 对象，
     *             后续内容（含 token 统计）被丢弃。
     */
    @Deprecated
    private String parseSseBody(String body) {
        List<String> events = parseSseEvents(body);
        return events.isEmpty() ? "" : events.get(0);
    }

    // ==================== 日志辅助方法 ====================

    private void logRequest(String mode, String requestUrl, VertexGenerateContentRequest request, String requestId, Long userId) {
        // URL/RequestId 独立于 body 序列化打印，保证即使请求体序列化异常也不吞掉关键信息
        log.info("[Vertex] RequestId: {}, UserId: {}", requestId, userId);
        log.info("[Vertex] 请求地址: POST {}", requestUrl);
        // 上游请求统一走 LogBox 方框日志（与 OpenAI 渠道一致）：RequestId/UserId/Provider/URI/请求体完整打印，
        // 请求体序列化失败时 LogBox 内部兜底为 toString
        LogBox.logUpstreamRequest(providerName, requestUrl, request, requestId, userId);
    }

    private void logError(String mode, Throwable e) {
        log.error("[Vertex] {}调用失败", mode, e);
        if (e instanceof WebClientResponseException wcre) {
            log.error("[Vertex] 上游响应体: {}", wcre.getResponseBodyAsString());
        }
    }
}
