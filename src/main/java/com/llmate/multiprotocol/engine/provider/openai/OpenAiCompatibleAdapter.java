package com.llmate.multiprotocol.engine.provider.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.converter.upstream.OpenAiFormatConverter;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.openai.OpenAiChatRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiChatResponse;
import com.llmate.multiprotocol.dto.openai.OpenAiStreamChunk;
import com.llmate.multiprotocol.engine.provider.AbstractProviderAdapter;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UrlUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * OpenAI 兼容协议 Provider 抽象基类
 *
 * 适用于所有上游协议兼容 OpenAI /v1/chat/completions 的 Provider：
 * - DashScope（阿里云百炼）
 * - DeepSeek
 * - Azure OpenAI
 *
 * 子类只需提供：
 * 1. 认证方式（通过 WebClient.Builder 配置）
 * 2. Provider 别名和名称
 * 3. 可选：覆写流式解析逻辑（如 Azure 的特殊处理）
 *
 * 公共逻辑：
 * - 使用 OpenAiFormatConverter 进行请求/响应转换
 * - 统一走 chat/completions 端点
 * - 标准 OpenAI 兼容 SSE 解析（过滤 [DONE]、提取 data: JSON）
 */
@Log4j2
public abstract class OpenAiCompatibleAdapter extends AbstractProviderAdapter {

    protected static final String DEFAULT_PATH = "chat/completions";
    protected final OpenAiFormatConverter formatConverter;

    /**
     * 构造 OpenAI 兼容 Provider（单 Token 模式，兼容旧代码）
     * @param baseUrl 上游 API 基址
     * @param formatConverter OpenAI 格式转换器
     * @param authConfigurer 认证配置（设置 Header）
     */
    protected OpenAiCompatibleAdapter(
            String baseUrl,
            OpenAiFormatConverter formatConverter,
            java.util.function.Consumer<WebClient.Builder> authConfigurer,
            ObjectMapper objectMapper) {
        this(baseUrl, formatConverter, authConfigurer, objectMapper, null, null);
    }

    /**
     * 构造 OpenAI 兼容 Provider（多 Token 模式，方案 C）
     * @param baseUrl 上游 API 基址
     * @param formatConverter OpenAI 格式转换器
     * @param authConfigurer 认证配置（设置 Header）
     * @param apiKeys 多 Token 列表（可为 null）
     * @param tokenIds 与 apiKeys 对应的 Token ID 列表（可为 null）
     */
    protected OpenAiCompatibleAdapter(
            String baseUrl,
            OpenAiFormatConverter formatConverter,
            java.util.function.Consumer<WebClient.Builder> authConfigurer,
            ObjectMapper objectMapper,
            List<String> apiKeys,
            List<Long> tokenIds) {
        super(baseUrl, configureAuth(WebClient.builder(), authConfigurer), objectMapper, apiKeys, tokenIds);
        this.formatConverter = formatConverter;
    }

    private static WebClient.Builder configureAuth(
            WebClient.Builder builder,
            java.util.function.Consumer<WebClient.Builder> authConfigurer) {
        authConfigurer.accept(builder);
        return builder;
    }

    // ==================== 非流式调用（默认路径） ====================

    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest internalReq) {
        return chat(internalReq, null);
    }

    // ==================== 非流式调用（带自定义端点） ====================

    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest internalReq, ModelEndpointConfig endpointConfig) {
        log.info("[{}] 开始非流式调用", getProviderName());
        OpenAiChatRequest openAiReq = formatConverter.toOpenAiRequest(internalReq);
        openAiReq.setStream(false);

        String uri = endpointConfig != null && endpointConfig.getEndpointPath() != null
                ? endpointConfig.getEndpointPath()
                : DEFAULT_PATH;

        // 构建完整的 URL 用于日志记录（统一走 UrlUtils.join 处理斜杠，避免双斜杠/缺斜杠）
        String fullUrl = endpointConfig != null && endpointConfig.getFullUrl() != null
                ? endpointConfig.getFullUrl()
                : UrlUtils.join(baseUrl, uri);

        logRequest("非流式", fullUrl, openAiReq);
        // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
        logUpstreamKeys(internalReq, "非流式");

        // 部分上游(如兴鼎 xdwl)即使收到 stream:false 也可能返回 text/event-stream；
        // doPostBlocking 的 bodyToMono 读 SSE 会抛 UnsupportedOperationException(SSE 只能以 Flux 读)，
        // 故改用 doPostBlockingTolerant 按响应 Content-Type 分流，SSE 聚合回单个非流式响应。
        return doPostBlockingTolerant(openAiReq, uri, endpointConfig)
                .doOnNext(resp -> log.info("[{}] 非流式响应完成: id={}", getProviderName(), resp.getId()))
                .doOnError(e -> logError("非流式", e));
    }

    // ==================== 流式调用（默认路径） ====================

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalReq) {
        return chatStream(internalReq, null);
    }

    // ==================== 流式调用（带自定义端点） ====================

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalReq, ModelEndpointConfig endpointConfig) {
        log.info("[{}] 开始流式调用", getProviderName());
        OpenAiChatRequest openAiReq = formatConverter.toOpenAiRequest(internalReq);
        openAiReq.setStream(true);
        // 客户端已传 stream_options 就沿用客户端的，没传才补上 include_usage。
        // 流式对话缺少 usage chunk 会导致 token 计费归零，所以 include_usage 是刚需。
        // 用 extraParams 判断而非 streamOptions 属性，是因为 OpenAiFormatConverter 把
        // stream_options 归入白名单透传字段，客户端的值只存在于 extraParams 中。
        boolean clientHasStreamOptions = openAiReq.getExtraParams() != null
                && openAiReq.getExtraParams().containsKey("stream_options");
        if (!clientHasStreamOptions) {
            openAiReq.setStreamOptions(OpenAiChatRequest.StreamOptions.builder().includeUsage(true).build());
        }

        String uri = endpointConfig != null && endpointConfig.getEndpointPath() != null
                ? endpointConfig.getEndpointPath()
                : DEFAULT_PATH;

        // 构建完整的 URL 用于日志记录（统一走 UrlUtils.join 处理斜杠，避免双斜杠/缺斜杠）
        String fullUrl = endpointConfig != null && endpointConfig.getFullUrl() != null
                ? endpointConfig.getFullUrl()
                : UrlUtils.join(baseUrl, uri);

        logRequest("流式", fullUrl, openAiReq);
        // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
        logUpstreamKeys(internalReq, "流式");

        // 状态化累积 tool_call 参数：OpenAI 把 arguments 以 delta.tool_calls[].function.arguments
        // 增量片段下发（首块带 name/id、后续只有参数片段）。toInternalStreamChunk 无状态逐事件转换，
        // 若直接透传片段，下游 Vertex（要求完整 functionCall 对象）每个块都不满足"name+完整参数"
        // → functionCall 被整体丢弃，客户端空白。这里按 index 累积完整 JSON，在终止块补发
        // name+完整参数，中间片段块不携带参数输出（避免 OpenAI 客户端按 index 拼接时重复叠加）。
        return doPostStreamRaw(uri, openAiReq, endpointConfig)
                .doOnNext(line -> log.debug("[{}] 收到原始SSE行: {}", getProviderName(), line))
                .transform(this::parseOpenAiCompatibleSse)
                .mapNotNull(json -> safeReadValue(json, OpenAiStreamChunk.class))
                .mapNotNull(formatConverter::toInternalStreamChunk)
                .map(new ToolCallArgsAccumulator()::process)
                .doOnNext(chunk -> log.debug("[{}] 输出内部chunk: deltaContent={}",
                        getProviderName(), chunk.getDeltaContent()))
                .doOnComplete(() -> log.info("[{}] 流式调用完成", getProviderName()))
                .doOnError(e -> logError("流式", e))
                // 上游流错误禁止吞成空 chunk（会静默丢失错误 + 客户端永远收不到提示），
                // 必须 Flux.error(e) 传播到 Controller 统一转 SSE error 事件（GatewayErrorResponseBuilder.streamErrorEvents）
                ;
    }

    // ==================== 非流式容错调用 ====================

    /**
     * 非流式容错调用：部分上游（如兴鼎 xdwl）即使收到 stream:false 也照常返回 text/event-stream，
     * 而 WebClient 的 bodyToMono 读 SSE 会抛 UnsupportedOperationException（SSE 只能以 Flux 读）。
     * 这里用 exchangeToMono 按响应 Content-Type 分流：
     * - text/event-stream → 复用已验证的流式解析管线，把整条流聚合回单个非流式响应（tool_call 参数
     *   累积、usage 提取与流式路径一致）；
     * - 普通 JSON → 与 doPostBlocking 一致解析；
     * - 非 2xx → 照常抛错（exchangeToMono 不会像 retrieve() 那样自动抛 4xx/5xx，必须手动转异常，
     *   否则错误会被吞成成功空响应）。
     */
    private Mono<LlmChatResponse> doPostBlockingTolerant(OpenAiChatRequest request, String uri, ModelEndpointConfig endpointConfig) {
        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            String fullUrl = endpointConfig != null && endpointConfig.getFullUrl() != null
                    ? endpointConfig.getFullUrl()
                    : UrlUtils.join(baseUrl, uri);
            LogBox.logUpstreamRequest(getProviderName(), fullUrl, maskForLog(request), requestId, userId);

            // 自定义端点用完整 URL（与 doPostBlockingWithFullUrl 一致，mutate 保留超时/codecs 配置）；
            // 否则 uri 剥离前导斜杠作为相对路径（避免 / 开头替换掉 baseUrl 的路径段，见 doPostBlocking 注释）。
            WebClient.RequestBodySpec uriSpec = (endpointConfig != null && endpointConfig.getFullUrl() != null && !endpointConfig.getFullUrl().isEmpty())
                    ? webClient.mutate().build().post().uri(endpointConfig.getFullUrl())
                    : webClient.post().uri(UrlUtils.stripLeadingSlash(uri));

            return uriSpec.bodyValue(request)
                    .exchangeToMono(response -> {
                        if (!response.statusCode().is2xxSuccessful()) {
                            return response.createException().flatMap(e -> Mono.error(e));
                        }
                        MediaType contentType = response.headers().contentType().orElse(null);
                        if (contentType != null && contentType.isCompatibleWith(MediaType.TEXT_EVENT_STREAM)) {
                            log.warn("[{}] 上游忽略 stream:false 返回 text/event-stream，改用流式解析并聚合", getProviderName());
                            return response.bodyToFlux(String.class)
                                    .transform(this::parseOpenAiCompatibleSse)
                                    .mapNotNull(json -> safeReadValue(json, OpenAiStreamChunk.class))
                                    .mapNotNull(formatConverter::toInternalStreamChunk)
                                    .map(new ToolCallArgsAccumulator()::process)
                                    .collectList()
                                    .map(this::assembleChatResponse);
                        }
                        return response.bodyToMono(OpenAiChatResponse.class)
                                .map(formatConverter::toInternalResponse);
                    })
                    .doOnNext(resp -> LogBox.logUpstreamResponse(getProviderName(), maskForLog(resp), requestId, userId))
                    .doOnError(e -> logError("非流式", e));
        });
    }

    /**
     * 把聚合后的流式块列表重组为单个非流式响应：
     * - 内容 = 各块 deltaContent 直接拼接（不插分隔符，流式切块无空白边界，插符会改变原文）；
     * - 工具调用 = 终止块补发的 name+完整参数（ToolCallArgsAccumulator 已在终止块注入）；
     * - usage / finishReason / model / id 取首个非 null。
     */
    private LlmChatResponse assembleChatResponse(List<LlmStreamChunk> chunks) {
        StringBuilder content = new StringBuilder();
        List<LlmToolCall> toolCalls = new ArrayList<>();
        LlmUsage usage = null;
        String finishReason = null;
        String model = null;
        String id = null;

        for (LlmStreamChunk chunk : chunks) {
            if (chunk.getDeltaContent() != null) {
                content.append(chunk.getDeltaContent());
            }
            // 仅终止块携带完整工具调用（name+完整参数），其余块参数片段已被累积器清空
            if (chunk.getToolCallName() != null && !chunk.getToolCallName().isEmpty()) {
                toolCalls.add(LlmToolCall.builder()
                        .id(chunk.getToolCallId())
                        .name(chunk.getToolCallName())
                        .arguments(chunk.getToolCallArgumentsDelta() != null
                                ? chunk.getToolCallArgumentsDelta() : "{}")
                        .build());
            }
            if (usage == null) {
                usage = chunk.getUsage();
            }
            if (finishReason == null) {
                finishReason = chunk.getFinishReason();
            }
            if (model == null) {
                model = chunk.getModel();
            }
            if (id == null) {
                id = chunk.getId();
            }
        }

        LlmChatResponse response = new LlmChatResponse();
        response.setId(id);
        response.setModel(model);
        LlmChatResponse.Choice choice = new LlmChatResponse.Choice();
        choice.setIndex(0);
        LlmChatResponse.Message message = new LlmChatResponse.Message();
        message.setRole("assistant");
        message.setContent(content.toString());
        if (!toolCalls.isEmpty()) {
            message.setToolCalls(toolCalls);
        }
        choice.setMessage(message);
        choice.setFinishReason(finishReason);
        response.setChoices(List.of(choice));
        response.setUsage(toResponseUsage(usage));
        return response;
    }

    /** 内部流式 LlmUsage → 非流式响应 Usage（全维度映射，未提供维度保持默认 0） */
    private LlmChatResponse.Usage toResponseUsage(LlmUsage usage) {
        if (usage == null) {
            return null;
        }
        LlmChatResponse.Usage u = new LlmChatResponse.Usage();
        u.setPromptTokens(usage.getPromptTokens());
        u.setCompletionTokens(usage.getCompletionTokens());
        u.setTotalTokens(usage.getTotalTokens());
        u.setReasoningTokens(usage.getReasoningTokens());
        u.setCacheHitTokens(usage.getCacheHitTokens());
        u.setCacheMissTokens(usage.getCacheMissTokens());
        u.setCacheCreationTokens(usage.getCacheCreationTokens());
        u.setCacheReadTokens(usage.getCacheReadTokens());
        u.setCachedTokens(usage.getCachedTokens());
        return u;
    }

    /**
     * OpenAI 流式 tool_call 参数状态化累积器（每次 chatStream 调用 new 一个，天然按请求隔离）。
     *
     * OpenAI 兼容上游（OpenAI/Azure/DashScope/DeepSeek 等）把 tool_call 的 arguments 以
     * delta.tool_calls[].function.arguments 增量片段下发：首个 chunk 携带 name/id + 空 arguments，
     * 后续 chunk 只有参数片段、无 name（与 Anthropic input_json_delta 同一模式）。若把片段直接透传，
     * 下游 Vertex（要求完整 functionCall 对象）name 与完整参数不在同一个 chunk，functionCall 被整体
     * 丢弃 → 客户端空白。本累积器（与 AnthropicProviderAdapter.ToolCallArgsAccumulator 同构）：
     * - 公告块：登记该 index 的 id/name，片段参数置 null（不直接输出，避免 OpenAI 客户端按 index 拼接重复）
     * - 增量块：把 arguments 片段按 index 累积进 StringBuilder
     * - 终止块（finish_reason=tool_calls）：补发携带 name+完整参数 的块
     *
     * 注意：并行多工具调用只补发首个（index 最小）的完整参数，与当前 Vertex 输出端
     * toVertexStreamChunk 单 functionCall part 的限制一致；如需支持并行调用再扩展。
     */
    protected class ToolCallArgsAccumulator {
        private final Map<Integer, MutableToolCall> toolCalls = new HashMap<>();

        LlmStreamChunk process(LlmStreamChunk chunk) {
            boolean hasName = chunk.getToolCallName() != null && !chunk.getToolCallName().isEmpty();
            if (hasName) {
                // 公告块：登记 id/name，可能带初始参数（部分实现直接下发完整 arguments）
                Integer idx = chunk.getToolCallIndex() != null ? chunk.getToolCallIndex() : 0;
                MutableToolCall tc = toolCalls.computeIfAbsent(idx, k -> new MutableToolCall());
                tc.id = chunk.getToolCallId();
                tc.name = chunk.getToolCallName();
                if (chunk.getToolCallArgumentsDelta() != null) {
                    tc.args.append(chunk.getToolCallArgumentsDelta());
                }
                // 公告块不直接携带参数输出（避免下游按 index 拼接时重复叠加）
                chunk.setToolCallArgumentsDelta(null);
            } else if (chunk.getToolCallArgumentsDelta() != null) {
                // 增量块：累积参数片段
                Integer idx = chunk.getToolCallIndex() != null ? chunk.getToolCallIndex() : 0;
                MutableToolCall tc = toolCalls.computeIfAbsent(idx, k -> new MutableToolCall());
                tc.args.append(chunk.getToolCallArgumentsDelta());
                // 片段块不直接携带参数输出（下游需要完整参数）
                chunk.setToolCallArgumentsDelta(null);
            }

            // 终止块（finish_reason=tool_calls）：补发首个累积完成的工具调用（name+完整参数）
            if (chunk.isFinished() && !toolCalls.isEmpty()) {
                MutableToolCall tc = toolCalls.values().iterator().next();
                chunk.setToolCallId(tc.id);
                chunk.setToolCallName(tc.name);
                chunk.setToolCallArgumentsDelta(tc.args.length() == 0 ? "{}" : tc.args.toString());
                toolCalls.clear();
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
}
