package com.llmate.multiprotocol.engine.provider.anthropic;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.constant.BusinessConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.converter.upstream.AnthropicFormatConverter;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesRequest;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesResponse;
import com.llmate.multiprotocol.dto.anthropic.AnthropicStreamEvent;
import com.llmate.multiprotocol.dto.anthropic.CountTokensResponse;
import com.llmate.multiprotocol.engine.provider.AbstractProviderAdapter;
import com.llmate.multiprotocol.engine.provider.ProviderAdapter;
import com.llmate.multiprotocol.util.KeyMaskUtil;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UrlUtils;
import com.llmate.multiprotocol.util.WebClientUtils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
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
 * Anthropic Claude Provider适配器
 * 支持模型前缀: "vp/"
 * 例如：vp/claude-3-5-sonnet, vp/claude-3-opus
 *
 * 上游通信协议：Anthropic /v1/messages 原生格式
 * 格式转换委派给 AnthropicFormatConverter，本类只负责 HTTP 调用和 SSE 事件解析
 * 使用公共 dto/anthropic 包下的 DTO，不再维护私有 DTO
 *
 * 注意：本类不由 Spring 自动扫描创建，而是由 ProviderFactory 根据配置手动实例化
 *
 * 方案 C：支持多 Token 负载均衡（秒级时间戳取模）
 */
@Log4j2
public class AnthropicProviderAdapter implements ProviderAdapter {

    private static final String UPSTREAM_PATH = BusinessConstants.UPSTREAM_PATH_MESSAGES;
    // 流式 SSE data 行解析用 ObjectMapper（只读线程安全，独立实例避免与主 objectMapper 混淆）。
    // 必须关闭 FAIL_ON_UNKNOWN_PROPERTIES：上游事件带 DTO 没有的扩展字段（顶层 model、
    // message.stop_details / delta.stop_details / usage.output_tokens_details 等），Jackson 默认
    // 对未知字段抛 UnrecognizedPropertyException，会把所有合法事件当成垃圾跳过，导致流式正文与
    // tokens 全丢。关闭后仅真正无法解析的尾部数组（START_ARRAY → MismatchedInputException）会被跳过。
    private static final ObjectMapper SSE_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final WebClient webClient;
    private final HttpClient httpClient;
    private final AnthropicFormatConverter formatConverter;
    private final String baseUrl;
    private final String providerName;
    private final String providerAlias;

    // 方案 C：多 Token 支持
    private final List<String> apiKeys;
    private final List<Long> tokenIds;
    private final ThreadLocal<String> currentApiKey = new ThreadLocal<>();
    private final ThreadLocal<Long> currentTokenId = new ThreadLocal<>();

    public AnthropicProviderAdapter(String baseUrl, String apiKey,
                                    AnthropicFormatConverter formatConverter,
                                    String providerName, String providerAlias,
                                    List<String> apiKeys, List<Long> tokenIds) {
        // 关键修复：baseUrl 必须归一化为单斜杠结尾（如 https://api.vapeur.ai/claude/），
        // 否则 WebClient 相对路径追加时会把 /claude 和 v1/messages 粘成 claudev1/messages。
        this.baseUrl = UrlUtils.withTrailingSlash(baseUrl);
        this.providerName = providerName;
        this.providerAlias = providerAlias;
        this.formatConverter = formatConverter;

        // 方案 C：多 Token 支持
        this.apiKeys = (apiKeys != null && !apiKeys.isEmpty()) ? apiKeys : List.of(apiKey);
        this.tokenIds = (tokenIds != null && !tokenIds.isEmpty()) ? tokenIds : new ArrayList<>();

        // 共享 HttpClient（含自定义 TLS 握手超时），必须先于 WebClient 初始化。
        // 所有 WebClient 复用一个 HttpClient → 连接池按同一 SslContext 复用，避免每次请求新建连接。
        this.httpClient = buildHttpClient();
        // 初始 WebClient（使用第一个 key，后续请求会动态选择）
        this.webClient = buildWebClient(apiKey);
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
        return WebClient.builder()
                .baseUrl(this.baseUrl)
                .clientConnector(new ReactorClientHttpConnector(this.httpClient))
                .defaultHeader("x-api-key", apiKey)
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .defaultHeader("anthropic-version", "2023-06-01")
                .build();
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
     * 在【调用上游接口前】打印本次使用的 用户 API Key 与 渠道 API Key 的 ID + 首尾遮罩（KeyMaskUtil.mask），
     * 便于按 requestId/tokenId 排查问题，绝不打印完整 key。
     *
     * @param internalReq 内部请求（携带 LlmGateway 填充的用户 key 信息；count_tokens 透传场景为 null）
     */
    private WebClient getCurrentWebClient(LlmChatRequest internalReq) {
        String selectedKey = selectApiKey();
        if (selectedKey != null) {
            // 多 Token 负载均衡：打印本轮实际选中的渠道 Token
            log.info("[{}] 调用上游 keys: {}",
                getProviderName(),
                KeyMaskUtil.describeKeys(
                    internalReq != null ? internalReq.getUserTokenId() : null,
                    internalReq != null ? internalReq.getUserApiKey() : null,
                    currentTokenId.get(), selectedKey));
            return buildWebClient(selectedKey);
        }
        // 单 Token 模式：用构造时配置的默认 key（apiKeys.get(0)，与 webClient 认证 Header 一致）
        log.info("[{}] 调用上游 keys: {}",
            getProviderName(),
            KeyMaskUtil.describeKeys(
                internalReq != null ? internalReq.getUserTokenId() : null,
                internalReq != null ? internalReq.getUserApiKey() : null,
                tokenIds.isEmpty() ? null : tokenIds.get(0),
                apiKeys.isEmpty() ? null : apiKeys.get(0)));
        return webClient;
    }

    @Override
    public String getProviderAlias() {
        return providerAlias;
    }

    /**
     * 声明本 Provider 与上游通信使用的原生协议为 Anthropic Messages
     */
    @Override
    public ProtocolType getNativeProtocol() {
        return ProtocolType.ANTHROPIC_MESSAGES;
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
     * 非流式调用（默认端点）
     */
    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest internalReq) {
        return chat(internalReq, null);
    }

    /**
     * 非流式调用（带自定义端点配置）
     *
     * 关键修复：最终请求地址优先使用端点配置（ModelEndpointResolver 每次实时读库，
     * baseUrl + endpointPath 拼接），而不是内存缓存的渠道 baseUrl + 固定 v1/messages。
     * 否则渠道 baseUrl 更新后（如加上 /claude 路径段），内存里的旧 Provider 不会刷新，
     * 导致请求地址丢失 baseUrl 的路径段。
     */
    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest internalReq, ModelEndpointConfig endpointConfig) {
        // 计算最终请求地址（统一走 UrlUtils.join 处理斜杠，避免缺斜杠/双斜杠）
        String targetUrl = resolveTargetUrl(endpointConfig);

        return Mono.deferContextual(ctxView -> {
            log.info("[Anthropic] 开始非流式调用");
            AnthropicMessagesRequest request = formatConverter.toAnthropicRequest(internalReq);
            request.setStream(false);

            // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入）
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            logRequest("非流式", targetUrl, request, requestId, userId);

            return getCurrentWebClient(internalReq).post()
                    .uri(targetUrl)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(AnthropicMessagesResponse.class)
                    .doOnNext(resp -> {
                        log.info("[Anthropic] 非流式响应完成: id={}", resp.getId());
                        // 上游响应体走 LogBox 方框日志（与 OpenAI/Vertex 渠道一致）；异步执行避免大响应体序列化阻塞 Netty 事件循环线程
                        Mono.fromRunnable(() -> LogBox.logUpstreamResponse(providerName, resp, requestId, userId))
                                .subscribeOn(Schedulers.boundedElastic())
                                .subscribe();
                    })
                    .map(formatConverter::toInternalResponse)
                    .doOnError(e -> logError("非流式", e));
        });
    }

    /**
     * 流式调用（默认端点）
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
        // 计算最终请求地址（统一走 UrlUtils.join 处理斜杠）
        String targetUrl = resolveTargetUrl(endpointConfig);

        return Flux.deferContextual(ctxView -> {
            log.info("[Anthropic] 开始流式调用");
            AnthropicMessagesRequest request = formatConverter.toAnthropicRequest(internalReq);
            request.setStream(true);

            // 从 Reactor Context 读取 requestId/userId（由 LlmGateway 写入）
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            logRequest("流式", targetUrl, request, requestId, userId);

            return getCurrentWebClient(internalReq).post()
                    .uri(targetUrl)
                    .header(HttpHeaders.ACCEPT, MediaType.TEXT_EVENT_STREAM_VALUE)
                    .bodyValue(request)
                    .retrieve()
                    // 容错 SSE 解析：见 parseAnthropicSseEvent。中继类上游会在
                    // message_stop 之后追加一个 data 为 JSON 数组的尾部事件，严格
                    // bodyToFlux(AnthropicStreamEvent.class) 会抛 DecodingException 把整条流打成 error，
                    // 导致网关不结算、客户端收不到 message_delta/message_stop、proxy_logs 无记录。
                    // 改为按行解析，坏 data 直接跳过不中断流，让已消费 tokens 正常走结算。
                    .bodyToFlux(String.class)
                    .mapNotNull(this::parseAnthropicSseEvent)
                    .doOnNext(event -> log.debug("[Anthropic] 收到流式事件: type={}", event.getType()))
                    // 状态化累积 tool_use 参数：Anthropic 把 tool_use 的 input 以 input_json_delta 分段下发
                    //（partial_json 片段）。toInternalStreamChunk 是无状态逐事件转换，无法跨事件拼接；
                    // 若直接透传片段，下游 Vertex（要求完整 functionCall 对象）只能收到缺参数的 {name}。
                    // 这里按 index 累积完整 JSON，在 content_block_stop 补发携带 name+完整参数 的块，
                    // 中间片段块不直接携带参数输出（否则 OpenAI 客户端会重复拼接翻倍）。
                    .map(new ToolCallArgsAccumulator()::process)
                    .doOnNext(chunk -> log.debug("[Anthropic] 输出内部chunk: deltaContent={}, finished={}", chunk.getDeltaContent(), chunk.isFinished()))
                    .doOnComplete(() -> log.info("[Anthropic] 流式调用完成"))
                    .doOnError(e -> logError("流式", e));
                    // 注意：不再用 onErrorResume 把上游异常吞成空 chunk。
                    // 之前吞掉后 LlmGateway 的 doOnError→abortStream（记录失败）和控制器 SSE error 事件
                    // 都不会触发，流式失败被 settleStream 记为"成功 0 tokens"，客户端收到空流。
                    // 现在让错误正常传播：网关 abortStream 记真实失败，SSE error 事件发给客户端。
        });
    }

    /**
     * Anthropic 流式 tool_use 参数状态化累积器（每次 chatStream 调用 new 一个，天然按请求隔离）。
     *
     * Anthropic 上游把 tool_use 的 input 以多个 input_json_delta 事件分段下发（partial_json 片段），
     * 且片段事件不携带 name/id。toInternalStreamChunk 无状态逐事件转换，若把片段直接透传，
     * 下游 Vertex（要求完整 functionCall 对象）只能输出缺参数的 {name}（客户端报
     * "The required parameter pattern is missing"）。本累积器：
     * - content_block_start：登记该 index 的 id/name，并读取初始 input（部分实现会直接下发完整 input）
     * - content_block_delta：把 partial_json 片段按 index 累积进 StringBuilder
     * - content_block_stop：补发携带 name+完整参数 的块；片段块本身不携带参数（清空），
     *   避免 OpenAI 客户端按 index 拼接时把片段与完整参数重复叠加
     */
    private class ToolCallArgsAccumulator {
        private static final ObjectMapper INIT_MAPPER = new ObjectMapper();
        private final java.util.Map<Integer, MutableToolCall> toolCalls = new java.util.HashMap<>();

        LlmStreamChunk process(AnthropicStreamEvent event) {
            LlmStreamChunk chunk = formatConverter.toInternalStreamChunk(event);
            switch (event.getType()) {
                case "content_block_start" -> {
                    Object cb = event.getContentBlock();
                    if (cb instanceof java.util.Map<?, ?> map && "tool_use".equals(map.get("type"))) {
                        Integer idx = event.getIndex() != null ? event.getIndex() : 0;
                        MutableToolCall tc = toolCalls.computeIfAbsent(idx, k -> new MutableToolCall());
                        tc.id = chunk.getToolCallId();
                        tc.name = chunk.getToolCallName();
                        // 部分实现（含中继）直接在 content_block_start 下发完整 input，而非 input_json_delta
                        Object input = map.get("input");
                        if (input instanceof java.util.Map<?, ?> im && !im.isEmpty()) {
                            try {
                                tc.args.append(INIT_MAPPER.writeValueAsString(input));
                            } catch (Exception e) {
                                log.warn("[Anthropic] content_block_start 初始 input 序列化失败: {}", e.getMessage());
                            }
                        }
                    }
                }
                case "content_block_delta" -> {
                    if (chunk.getToolCallArgumentsDelta() != null) {
                        Integer idx = chunk.getToolCallIndex() != null ? chunk.getToolCallIndex() : 0;
                        MutableToolCall tc = toolCalls.computeIfAbsent(idx, k -> new MutableToolCall());
                        tc.args.append(chunk.getToolCallArgumentsDelta());
                        // 片段块不直接携带参数输出
                        chunk.setToolCallArgumentsDelta(null);
                    }
                }
                case "content_block_stop" -> {
                    Integer idx = event.getIndex() != null ? event.getIndex() : 0;
                    MutableToolCall tc = toolCalls.get(idx);
                    if (tc != null && tc.name != null && tc.args.length() > 0) {
                        chunk.setToolCallName(tc.name);
                        chunk.setToolCallId(tc.id);
                        chunk.setToolCallArgumentsDelta(tc.args.toString());
                    }
                    toolCalls.remove(idx);
                }
                default -> {
                }
            }
            return chunk;
        }
    }

    /** 累积器内部可变状态：一次工具调用的 id/name + 参数片段拼接 */
    private static class MutableToolCall {
        private String id;
        private String name;
        private final StringBuilder args = new StringBuilder();
    }

    // ==================== 日志辅助方法 ====================

    /**
     * 计算最终请求地址
     * 唯一拼接规则：base_url + endpoint_path（ModelEndpointResolver 已按渠道协议类型
     * 给出正确的默认 endpoint_path，适配器无需对 base url 做任何额外处理）。
     * 仅当端点配置缺失时回退到渠道 baseUrl + v1/messages。
     */
    private String resolveTargetUrl(ModelEndpointConfig endpointConfig) {
        if (endpointConfig != null && endpointConfig.getBaseUrl() != null
                && !endpointConfig.getBaseUrl().isEmpty()
                && endpointConfig.getEndpointPath() != null
                && !endpointConfig.getEndpointPath().isEmpty()) {
            return UrlUtils.join(endpointConfig.getBaseUrl(), endpointConfig.getEndpointPath());
        }
        return UrlUtils.join(baseUrl, UPSTREAM_PATH);
    }

    /**
     * Claude Code 的 POST /v1/messages/count_tokens：估算输入 tokens（不产生模型调用）。
     *
     * 网关此前未实现该接口 → 404 → Claude Code 视为失败高频重试，产生大量 429。
     * 这里把原始请求体原样透传给上游（Anthropic 标准接口 /v1/messages/count_tokens），
     * 取 input_tokens 返回。不参与计费/预占/日志（纯估算，不产生真实用量）。
     */
    public Mono<CountTokensResponse> countTokens(JsonNode rawBody, ModelEndpointConfig endpointConfig) {
        String basePath = endpointConfig != null && endpointConfig.getBaseUrl() != null
                && !endpointConfig.getBaseUrl().isEmpty()
                ? endpointConfig.getBaseUrl() : baseUrl;
        String countUrl = UrlUtils.join(basePath, "v1/messages/count_tokens");

        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            String model = rawBody != null && rawBody.path("model").isTextual()
                    ? rawBody.path("model").asText() : null;
            log.info("[Anthropic] count_tokens 请求: url={}, model={}", countUrl, model);
            LogBox.logUpstreamRequest(providerName, countUrl, rawBody, requestId, userId);

            return getCurrentWebClient(null).post()
                    .uri(countUrl)
                    .bodyValue(rawBody)
                    .retrieve()
                    .bodyToMono(CountTokensResponse.class)
                    .doOnNext(resp -> log.info("[Anthropic] count_tokens 响应: input_tokens={}", resp.getInputTokens()))
                    .doOnError(e -> logError("count_tokens", e));
        });
    }

    /**
     * 容错解析单条 SSE data 行（形如 data:{"type":"message_start",...}）。
     *
     * 与 OpenAI 渠道的 parseOpenAiCompatibleSse/safeReadValue 同一模式：
     * - 兼容「data: 」前缀与裸 JSON 两种形态（Spring SSE 读取器按 data 内容给 String，不带前缀）；
     * - 空行/注释/非对象（如 [DONE]）返回 null，由 mapNotNull 静默跳过；
     * - Jackson 解析失败（典型：中继在 message_stop 后追加 data 为 JSON 数组的尾部事件）
     *   返回 null 跳过，而不是把整条流以 error 结束——保证已消费 tokens 正常结算、客户端
     *   能收到 message_delta/message_stop 收尾事件。
     */
    private AnthropicStreamEvent parseAnthropicSseEvent(String line) {
        if (line == null) {
            return null;
        }
        String json = line.trim();
        if (json.startsWith("data:")) {
            json = json.substring(5).trim();
        }
        if (json.isEmpty() || !json.startsWith("{")) {
            return null;
        }
        try {
            return SSE_MAPPER.readValue(json, AnthropicStreamEvent.class);
        } catch (Exception e) {
            log.warn("[Anthropic] 跳过无法解析的SSE事件(非标准尾部数据): {}", json);
            return null;
        }
    }

    private void logRequest(String mode, String requestUrl, AnthropicMessagesRequest request, String requestId, Long userId) {
        // URL/RequestId 独立于 body 序列化打印，保证即使请求体序列化异常也不吞掉关键信息
        log.info("[Anthropic] RequestId: {}, UserId: {}", requestId, userId);
        log.info("[Anthropic] 请求地址: POST {}", requestUrl);
        log.info("[Anthropic] 请求头: Content-Type=application/json, anthropic-version=2023-06-01, x-api-key=***");
        // 上游请求统一走 LogBox 方框日志（与 OpenAI/Vertex 渠道一致）：RequestId/UserId/Provider/URI/请求体完整打印，
        // 请求体序列化失败时 LogBox 内部兜底为 toString
        LogBox.logUpstreamRequest(providerName, requestUrl, request, requestId, userId);
    }

    private void logError(String mode, Throwable e) {
        log.error("[Anthropic] {}调用失败", mode, e);
        if (e instanceof WebClientResponseException wcre) {
            log.error("[Anthropic] 上游响应体: {}", wcre.getResponseBodyAsString());
        }
    }
}
