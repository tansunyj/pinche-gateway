package com.llmate.multiprotocol.converter;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmContent;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.LlmToolDefinition;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.anthropic.*;
import com.llmate.multiprotocol.converter.support.PollutionCleaner;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.mapping.ModelMappingResolver;
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Anthropic 协议双向转换器
 * 负责 Anthropic /v1/messages 协议与内部标准模型的双向转换
 */
@Component
@Log4j2
public class AnthropicProtocolConverter
        implements ProtocolConverter<AnthropicMessagesRequest, AnthropicMessagesResponse> {

    private static final com.fasterxml.jackson.databind.ObjectMapper OBJECT_MAPPER =
            new com.fasterxml.jackson.databind.ObjectMapper();

    private final ModelMappingResolver mappingResolver;

    public AnthropicProtocolConverter(ModelMappingResolver mappingResolver) {
        this.mappingResolver = mappingResolver;
    }

    @Override
    public ProtocolType getProtocolType() {
        return ProtocolType.ANTHROPIC_MESSAGES;
    }

    @Override
    public Mono<LlmChatRequest> toInternalRequest(AnthropicMessagesRequest externalRequest) {
        if (externalRequest == null) {
            log.warn("[AnthropicConverter] 外部请求为空");
            return Mono.empty();
        }

        // 解析模型映射
        String externalModel = externalRequest.getModel();
        String internalModel = mappingResolver.resolve(externalModel, ProtocolType.ANTHROPIC_MESSAGES);
        log.info("[AnthropicConverter] 模型映射: {} -> {}", externalModel, internalModel);

        List<LlmMessage> internalMessages = new ArrayList<>();

        // 收集待下载的 URL 图片（source.type=url）：Anthropic 图片块的 url 必须由网关下载转 base64，
        // 但 convertContentBlocks 是同步方法，无法在此下载 → 先建空 base64 引用，返回前响应式下载填充。
        // 用可变 LlmContent 引用（@Data）实现「先占位、后填充」，无需重建消息结构。
        List<java.util.Map.Entry<LlmContent, String>> pendingImageDownloads = new ArrayList<>();

        // Anthropic 的 system 是顶层字段，转换为内部 messages[0]
        // 支持两种格式: String 或 List<{type, text}> 结构化内容块
        String systemText = extractSystemText(externalRequest.getSystem());
        if (systemText != null && !systemText.isEmpty()) {
            internalMessages.add(LlmMessage.system(systemText));
            log.debug("[AnthropicConverter] 提取system消息: {}", systemText.substring(0, Math.min(50, systemText.length())));
        }

        // 转换 messages
        if (externalRequest.getMessages() != null) {
            log.info("[AnthropicConverter] 转换 {} 条消息", externalRequest.getMessages().size());
            // tool_use id → name 映射：tool_result 块只携带 tool_use_id，
            // 但 Gemini functionResponse 必须带函数名，需从历史 tool_use 块回查
            java.util.Map<String, String> toolNameById = new java.util.HashMap<>();
            for (AnthropicMessagesRequest.AnthropicMessage msg : externalRequest.getMessages()) {
                // 处理 content（可能是 String 或 List）
                if (msg.getContent() instanceof String) {
                    LlmMessage internalMsg = new LlmMessage();
                    internalMsg.setRole(msg.getRole());
                    internalMsg.setTextContent((String) msg.getContent());
                    internalMessages.add(internalMsg);
                    log.debug("[AnthropicConverter] 转换文本消息: role={}, content={}", msg.getRole(),
                            ((String) msg.getContent()).substring(0, Math.min(30, ((String) msg.getContent()).length())));
                } else if (msg.getContent() instanceof List) {
                    // Anthropic 内容块列表：text / tool_use / tool_result / image 等
                    convertContentBlocks(msg.getRole(), (List<?>) msg.getContent(), internalMessages, toolNameById, pendingImageDownloads);
                } else {
                    LlmMessage internalMsg = new LlmMessage();
                    internalMsg.setRole(msg.getRole());
                    internalMessages.add(internalMsg);
                }
            }
        }

        // 客户端中断污染清洗：剥离 Claude Desktop/Code 注入的 "[Tool use interrupted]" / "(no content)"，
        // 防止上游模型把中断标记当对话内容回显（"触发 function call 但没有返回结果"的退化循环）。
        PollutionCleaner.clean(internalMessages);

        // 兜底透传：Anthropic 显式字段（metadata / output_config）与未建模字段合并进 extraParams，
        // 保证跨协议零遗漏（Gemini 无对应显式字段，仍随 extraParams 原样带过去）
        java.util.Map<String, Object> extraParams = new java.util.LinkedHashMap<>();
        if (externalRequest.getExtraParams() != null) {
            extraParams.putAll(externalRequest.getExtraParams());
        }
        if (externalRequest.getMetadata() != null) {
            extraParams.put("metadata", externalRequest.getMetadata());
        }
        if (externalRequest.getOutputConfig() != null) {
            extraParams.put("output_config", externalRequest.getOutputConfig());
        }

        LlmChatRequest internalReq = LlmChatRequest.builder()
                .model(internalModel)
                .messages(internalMessages)
                .temperature(externalRequest.getTemperature())
                .maxTokens(externalRequest.getMaxTokens())
                .stream(externalRequest.getStream()) // 透传原始 stream 标志，避免上游误判
                // ===== 透传字段：跨协议零遗漏 =====
                .tools(toInternalTools(externalRequest.getTools()))
                .toolChoice(externalRequest.getToolChoice())
                .thinking(externalRequest.getThinking())
                .topP(externalRequest.getTopP())
                .topK(externalRequest.getTopK())
                .stopSequences(externalRequest.getStopSequences())
                .extraParams(extraParams.isEmpty() ? null : extraParams)
                .build();

        // 日志确认透传字段是否被入口携带
        log.info("[AnthropicConverter] 内部请求构建完成: model={}, messages={}, tools={}, thinking={}, topP={}, topK={}, stopSequences={}, extraParams={}",
                internalModel, internalMessages.size(),
                externalRequest.getTools() != null ? externalRequest.getTools().size() : 0,
                externalRequest.getThinking(),
                externalRequest.getTopP(), externalRequest.getTopK(),
                externalRequest.getStopSequences(),
                externalRequest.getExtraParams() != null ? externalRequest.getExtraParams().keySet() : null);

        // URL 图片延迟下载：convertContentBlocks 同步阶段已为每个 url source 建了空 base64 引用，
        // 这里统一响应式下载（WebClientUtils 共享客户端，跟随重定向 + 5 分钟超时），下载完成后
        // setBase64Data 原地填充，消息结构无需重建。全部下载成功或失败后才返回请求。
        if (pendingImageDownloads.isEmpty()) {
            return Mono.just(internalReq);
        }
        return Flux.fromIterable(pendingImageDownloads)
                .flatMapSequential(entry -> downloadImageToBase64(entry.getKey(), entry.getValue()))
                .then(Mono.just(internalReq));
    }

    /**
     * 下载 URL 图片转 base64 并填充到 LlmContent（可变引用原地更新）。
     * 失败时抛 LlmGatewayException(IMAGE_DOWNLOAD_FAILED)，整条请求拒绝——避免把坏图送到上游。
     * 下载前 percent-decode 规避二次编码坑（见 WebClientUtils.decodeImageUrl）。
     */
    private Mono<Void> downloadImageToBase64(LlmContent content, String url) {
        String decodedUrl = WebClientUtils.decodeImageUrl(url);
        return WebClientUtils.imageDownloadClient().get().uri(decodedUrl)
                .retrieve()
                .bodyToMono(byte[].class)
                .map(bytes -> java.util.Base64.getEncoder().encodeToString(bytes))
                .doOnNext(base64 -> {
                    content.setBase64Data(base64);
                    // source 未带 media_type 时按 URL 扩展名兜底
                    if (content.getMimeType() == null || content.getMimeType().isEmpty()) {
                        content.setMimeType(WebClientUtils.detectImageMimeType(decodedUrl));
                    }
                })
                .onErrorMap(e -> new LlmGatewayException(LlmErrorCode.IMAGE_DOWNLOAD_FAILED, url, e))
                .then();
    }

    @Override
    public AnthropicMessagesResponse toExternalResponse(LlmChatResponse internalResponse) {
        if (internalResponse == null) {
            log.warn("[AnthropicConverter] 内部响应为空");
            return null;
        }

        List<AnthropicMessagesResponse.AnthropicContent> content = new ArrayList<>();

        // 处理文本内容
        if (internalResponse.getChoices() != null && !internalResponse.getChoices().isEmpty()) {
            LlmChatResponse.Choice firstChoice = internalResponse.getChoices().get(0);
            LlmChatResponse.Message msg = firstChoice.getMessage();

            // 文本内容
            if (msg != null && msg.getContent() != null) {
                content.add(AnthropicMessagesResponse.AnthropicContent.builder()
                        .type("text")
                        .text(msg.getContent())
                        .build());
            }

            // 工具调用 → Anthropic tool_use 内容块
            if (msg != null && msg.getToolCalls() != null) {
                for (var tc : msg.getToolCalls()) {
                    content.add(AnthropicMessagesResponse.AnthropicContent.builder()
                            .type("tool_use")
                            .id(tc.getId())
                            .name(tc.getName())
                            .input(tc.getArguments()) // Anthropic 期望 JSON 对象，这里传 String 会被 Jackson 序列化
                            .build());
                }
            }
        }

        // 构建 Anthropic 响应
        AnthropicMessagesResponse response = new AnthropicMessagesResponse();
        response.setId("msg_" + UUID.randomUUID().toString().replace("-", ""));
        response.setType("message");
        response.setRole("assistant");
        response.setModel(internalResponse.getModel());
        response.setContent(content);

        // 根据是否有工具调用决定 stop_reason
        boolean hasToolCalls = content.stream().anyMatch(c -> "tool_use".equals(c.getType()));
        response.setStopReason(hasToolCalls ? "tool_use" : "end_turn");

        // Usage（非流式同样回填缓存字段，与流式 message_delta 一致；缺失时按 0 处理）
        // input_tokens 须为纯新输入（扣除缓存），否则与 cache 字段双算
        if (internalResponse.getUsage() != null) {
            LlmChatResponse.Usage u = internalResponse.getUsage();
            int cacheCreation = u.getCacheCreationTokens();
            int cacheRead = u.getCacheReadTokens() > 0 ? u.getCacheReadTokens() : u.getCachedTokens();
            response.setUsage(AnthropicUsage.builder()
                    .inputTokens(Math.max(0, u.getPromptTokens() - cacheRead - cacheCreation))
                    .outputTokens(u.getCompletionTokens())
                    .cacheCreationInputTokens(cacheCreation > 0 ? cacheCreation : null)
                    .cacheReadInputTokens(cacheRead > 0 ? cacheRead : null)
                    .build());
        }

        log.info("[AnthropicConverter] 外部响应构建完成: id={}, model={}, contentSize={}",
                response.getId(), response.getModel(), content.size());
        return response;
    }

    /**
     * 内部 LlmUsage → Anthropic usage（口径对齐 Anthropic 官方：input_tokens 为纯新输入，
     * 缓存创建/读取单列。内部 promptTokens 是含缓存的总输入（OpenAI 口径），
     * 出站给 Anthropic 客户端时须扣除缓存部分，否则 input_tokens 与缓存字段双算）
     */
    private AnthropicUsage toAnthropicUsage(LlmUsage u) {
        int cacheRead = u.getCacheReadTokens() > 0 ? u.getCacheReadTokens() : u.getCachedTokens();
        int cacheCreation = u.getCacheCreationTokens();
        return AnthropicUsage.builder()
                .inputTokens(Math.max(0, u.getPromptTokens() - cacheRead - cacheCreation))
                .outputTokens(u.getCompletionTokens())
                .cacheCreationInputTokens(cacheCreation > 0 ? cacheCreation : null)
                .cacheReadInputTokens(cacheRead > 0 ? cacheRead : null)
                .build();
    }

    @Override
    public Flux<ServerSentEvent<Object>> toExternalStream(Flux<LlmStreamChunk> internalStream,
            AnthropicMessagesRequest originalReq, String maskedModelName) {
        String messageId = "msg_" + UUID.randomUUID().toString().replace("-", "");
        log.info("[AnthropicConverter] 开始流式转换: messageId={}, maskedModel={}", messageId, maskedModelName);

        // 记录流中最后一个 chunk 的 finishReason，用于 message_delta 的 stop_reason
        java.util.concurrent.atomic.AtomicReference<String> lastFinishReason =
                new java.util.concurrent.atomic.AtomicReference<>("end_turn");

        // 捕获流内用量：firstUsageRef=首个带用量的 chunk（message_start 的输入 tokens，用于外部 message_start 回填）；
        // finalUsageRef=最后一个带用量的 chunk（message_delta 的累计输入/输出 tokens，用于外部 message_delta 回填）。
        java.util.concurrent.atomic.AtomicReference<LlmUsage> firstUsageRef =
                new java.util.concurrent.atomic.AtomicReference<>(null);
        java.util.concurrent.atomic.AtomicReference<LlmUsage> finalUsageRef =
                new java.util.concurrent.atomic.AtomicReference<>(null);

        // 内容块状态追踪。
        // Anthropic 官方协议要求 content_block_delta 前必须先有 content_block_start，
        // 且同一时刻只能有一个打开的块；换块（text↔tool_use、不同 tool call）前必须先发
        // content_block_stop，否则严格客户端（Claude Code SDK）解析失败并中止流。
        // currentBlockIndex=-1 表示尚无打开的块；currentBlockType 取值 text / tool_use。
        java.util.concurrent.atomic.AtomicInteger currentBlockIndex =
                new java.util.concurrent.atomic.AtomicInteger(-1);
        java.util.concurrent.atomic.AtomicReference<String> currentBlockType =
                new java.util.concurrent.atomic.AtomicReference<>(null);
        // 当前 tool_use 块对应的 tool call 唯一标识（id 优先，否则用 name），
        // 用于把只携带 argumentsDelta 的后续 chunk 归属到已打开的块
        java.util.concurrent.atomic.AtomicReference<String> currentToolKey =
                new java.util.concurrent.atomic.AtomicReference<>(null);
        // 流内是否出现过工具调用：有则 message_delta 的 stop_reason 必须是 tool_use，
        // 否则 Claude 客户端不会进入工具执行回传循环
        java.util.concurrent.atomic.AtomicBoolean hadToolUse =
                new java.util.concurrent.atomic.AtomicBoolean(false);

        // 1. 构建数据流（首块触发 message_start；文本 text_delta / 工具调用 tool_use 混合）
        //    工具调用：Gemini functionCall / OpenAI tool_calls 已被 Provider 层转成 chunk 的
        //    toolCallId / toolCallName / toolCallArgumentsDelta 增量字段。
        //    说明：message_start 由首块（upstream message_start → firstChunk=true）触发发射，
        //    doOnNext 先于 concatMap 执行，故此时 firstUsageRef 已含该 chunk 的输入用量。
        Flux<ServerSentEvent<Object>> dataStream = internalStream
                .doOnNext(chunk -> {
                    log.info("[AnthropicConverter] 收到内部chunk: deltaContent={}, toolCallName={}, finished={}",
                            chunk.getDeltaContent(), chunk.getToolCallName(), chunk.isFinished());
                    if (chunk.getFinishReason() != null) {
                        lastFinishReason.set(chunk.getFinishReason());
                    }
                    if (chunk.getUsage() != null) {
                        if (firstUsageRef.get() == null) {
                            firstUsageRef.set(chunk.getUsage());
                        }
                        finalUsageRef.set(chunk.getUsage());
                    }
                })
                .concatMap(chunk -> {
                    List<ServerSentEvent<Object>> events = new ArrayList<>();

                    // 首块：发 message_start（带输入 tokens）。Anthropic 要求 message_start 必须最先发出
                    if (chunk.isFirstChunk()) {
                        LlmUsage startUsage = firstUsageRef.get();
                        AnthropicStreamEvent.AnthropicMessage msgStart = AnthropicStreamEvent.AnthropicMessage.builder()
                                .id(messageId)
                                .type("message")
                                .role("assistant")
                                .model(maskedModelName)
                                .stopReason(null)
                                .stopSequence(null)
                                .usage(startUsage != null ? toAnthropicUsage(startUsage)
                                        : AnthropicUsage.builder().inputTokens(0).outputTokens(0).build())
                                .build();
                        events.add(ServerSentEvent.builder()
                                .event("message_start")
                                .data(AnthropicStreamEvent.builder()
                                        .type("message_start")
                                        .message(msgStart)
                                        .build())
                                .build());
                    }

                    // 结束块（message_delta/message_stop）：不发内容增量，usage 由 messageDeltaFlux 统一发出
                    if (chunk.isFinished()) {
                        return Flux.fromIterable(events);
                    }

                    // 工具调用块：发出 content_block_start(tool_use) + input_json_delta 增量。
                    // 注意：Anthropic 上游的 name/id 只在 content_block_start 出现一次，
                    // 后续 chunk 只有 toolCallArgumentsDelta，不能要求 name/id 同时非空，
                    // 否则 arguments 增量会被整体丢弃（客户端拿到空 input 的 tool_use）。
                    if (chunk.getToolCallName() != null || chunk.getToolCallArgumentsDelta() != null) {
                        log.info("[AnthropicConverter] 转换工具调用增量: name={}, argsDelta={}", chunk.getToolCallName(), chunk.getToolCallArgumentsDelta());
                        String tcKey = chunk.getToolCallName() != null
                                ? (chunk.getToolCallId() != null ? chunk.getToolCallId() : chunk.getToolCallName())
                                : currentToolKey.get();
                        if (tcKey == null) {
                            log.warn("[AnthropicConverter] 收到无法关联到任何 tool call 的 arguments 增量，已丢弃: {}",
                                    chunk.getToolCallArgumentsDelta());
                            return Flux.fromIterable(events);
                        }
                        // 新的 tool call（或当前打开的不是该 tool call 的块）→ 先关旧块再开新块
                        if (!"tool_use".equals(currentBlockType.get()) || !tcKey.equals(currentToolKey.get())) {
                            if (currentBlockIndex.get() >= 0) {
                                events.add(ServerSentEvent.builder()
                                        .event("content_block_stop")
                                        .data(AnthropicStreamEvent.builder()
                                                .type("content_block_stop")
                                                .index(currentBlockIndex.get())
                                                .build())
                                        .build());
                            }
                            int idx = currentBlockIndex.incrementAndGet();
                            currentBlockType.set("tool_use");
                            currentToolKey.set(tcKey);
                            hadToolUse.set(true);
                            // Gemini functionCall 可能无独立 id，兜底生成符合 Anthropic 风格的 toolu_ id
                            String blockId = chunk.getToolCallId() != null
                                    ? chunk.getToolCallId()
                                    : "toolu_" + UUID.randomUUID().toString().replace("-", "").substring(0, 24);
                            events.add(ServerSentEvent.builder()
                                    .event("content_block_start")
                                    .data(AnthropicStreamEvent.builder()
                                            .type("content_block_start")
                                            .index(idx)
                                            .contentBlock(java.util.Map.of(
                                                    "type", "tool_use",
                                                    "id", blockId,
                                                    "name", chunk.getToolCallName() != null ? chunk.getToolCallName() : tcKey,
                                                    "input", java.util.Map.of()
                                            ))
                                            .build())
                                    .build());
                        }
                        if (chunk.getToolCallArgumentsDelta() != null && !chunk.getToolCallArgumentsDelta().isEmpty()) {
                            // 关键：input_json_delta 的增量必须放 partial_json 字段，
                            // 放 text 字段客户端读不到（Claude Desktop 报 "You sent: undefined"）
                            events.add(ServerSentEvent.builder()
                                    .event("content_block_delta")
                                    .data(AnthropicStreamEvent.builder()
                                            .type("content_block_delta")
                                            .index(currentBlockIndex.get())
                                            .delta(AnthropicStreamEvent.AnthropicDelta.builder()
                                                    .type("input_json_delta")
                                                    .partialJson(chunk.getToolCallArgumentsDelta())
                                                    .build())
                                            .build())
                                    .build());
                        }
                        return Flux.fromIterable(events);
                    }

                    // 文本块：首个 text_delta 前必须先发 content_block_start(type=text)，否则严格客户端解析中止
                    String deltaText = chunk.getDeltaContent() != null ? chunk.getDeltaContent() : "";
                    if (!"text".equals(currentBlockType.get())) {
                        // 空文本增量不值得为它新开一个块（message_start 等空 chunk 会走到这里）
                        if (deltaText.isEmpty()) {
                            return Flux.fromIterable(events);
                        }
                        if (currentBlockIndex.get() >= 0) {
                            events.add(ServerSentEvent.builder()
                                    .event("content_block_stop")
                                    .data(AnthropicStreamEvent.builder()
                                            .type("content_block_stop")
                                            .index(currentBlockIndex.get())
                                            .build())
                                    .build());
                        }
                        int idx = currentBlockIndex.incrementAndGet();
                        currentBlockType.set("text");
                        currentToolKey.set(null);
                        events.add(ServerSentEvent.builder()
                                .event("content_block_start")
                                .data(AnthropicStreamEvent.builder()
                                        .type("content_block_start")
                                        .index(idx)
                                        .contentBlock(java.util.Map.of(
                                                "type", "text",
                                                "text", ""))
                                        .build())
                                .build());
                    }
                    events.add(ServerSentEvent.builder()
                            .event("content_block_delta")
                            .data(AnthropicStreamEvent.builder()
                                    .type("content_block_delta")
                                    .index(currentBlockIndex.get())
                                    .delta(AnthropicStreamEvent.AnthropicDelta.builder()
                                            .type("text_delta")
                                            .text(deltaText)
                                            .build())
                                    .build())
                            .build());
                    return Flux.fromIterable(events);
                });

        // 2. content_block_stop 事件：关闭流末尾仍打开的块（若有）。
        //    用 Flux.defer 延迟到 dataStream 完成后取值，拿到真实的当前块 index。
        Flux<ServerSentEvent<Object>> blockStopFlux = Flux.defer(() -> {
            if (currentBlockIndex.get() < 0) {
                return Flux.empty();
            }
            return Flux.just(ServerSentEvent.builder()
                    .event("content_block_stop")
                    .data((Object) AnthropicStreamEvent.builder()
                            .type("content_block_stop")
                            .index(currentBlockIndex.get())
                            .build())
                    .build());
        });

        // 3. message_delta 事件（对齐 Anthropic 官方 SSE 格式：type + delta + usage 顶层平铺）
        //    stop_reason 由最后一个 chunk 的 finishReason 决定（tool_calls / end_turn / max_tokens）。
        //    usage 取流内最后一个带用量的 chunk（message_delta 的累计输入/输出 tokens），
        //    用 Flux.defer 延迟到订阅时取值，保证能读到 dataStream 已写入的 lastFinishReason / finalUsageRef。
        Flux<ServerSentEvent<Object>> messageDeltaFlux = Flux.defer(() -> {
            LlmUsage u = finalUsageRef.get();
            // 统一走 toAnthropicUsage：input_tokens 扣除缓存（否则与缓存字段双算），
            // 缓存读取双源（cache_read_input_tokens → prompt_tokens_details.cached_tokens）
            AnthropicUsage msgDeltaUsage = u != null
                    ? toAnthropicUsage(u)
                    : AnthropicUsage.builder().inputTokens(0).outputTokens(0).build();
            // 上游（如 Gemini）在函数调用时 finishReason 可能仍是 STOP/stop，
            // 但 Anthropic 协议要求含 tool_use 块时 stop_reason 必须是 tool_use，
            // 否则 Claude 客户端不回传工具结果（报 Tool use interrupted）
            String stopReason = mapStopReasonToAnthropic(lastFinishReason.get());
            if (hadToolUse.get() && "end_turn".equals(stopReason)) {
                stopReason = "tool_use";
            }
            AnthropicStreamEvent event = AnthropicStreamEvent.builder()
                    .type("message_delta")
                    .delta(AnthropicStreamEvent.AnthropicDelta.builder()
                            .stopReason(stopReason)
                            .build())
                    .usage(msgDeltaUsage)
                    .build();
            return Flux.just(ServerSentEvent.builder().event("message_delta").data(event).build());
        });

        // 4. message_stop 事件
        AnthropicStreamEvent stopEvent = AnthropicStreamEvent.builder()
                .type("message_stop")
                .build();

        // 组合所有事件
        return Flux.concat(
                dataStream,
                blockStopFlux,
                messageDeltaFlux,
                Flux.just(ServerSentEvent.builder().event("message_stop").data(stopEvent).build()))
                .doOnNext(sse -> log.debug("[AnthropicConverter] 输出SSE: event={}", sse.event()))
                .doOnComplete(() -> log.info("[AnthropicConverter] 流式转换完成"))
                // 上游流错误禁止在此吞成 SSE error（会丢失统一脱敏 + request_id），必须 Flux.error 传播到
                // Controller 的 onErrorResume 统一处理（GatewayErrorResponseBuilder.streamErrorEvents）
                .doOnError(e -> log.error("[AnthropicConverter] 流式转换错误", e));
    }

    /**
     * 内部 finish_reason → Anthropic stop_reason
     * internal: stop / tool_calls / length / content_filter
     * Anthropic: end_turn / tool_use / max_tokens / stop_sequence
     */
    private String mapStopReasonToAnthropic(String finishReason) {
        if (finishReason == null) return "end_turn";
        return switch (finishReason) {
            case "tool_calls" -> "tool_use";
            case "length" -> "max_tokens";
            case "stop_sequence" -> "stop_sequence";
            case "stop" -> "end_turn";
            default -> "end_turn";
        };
    }

    @Override
    public String extractModelName(AnthropicMessagesRequest externalRequest) {
        return externalRequest != null ? externalRequest.getModel() : null;
    }

    /**
     * Anthropic 工具定义 → 内部标准工具定义
     * Anthropic: {name, description, input_schema} → LlmToolDefinition{name, description, parameters}
     */
    private List<LlmToolDefinition> toInternalTools(List<AnthropicTool> tools) {
        if (tools == null || tools.isEmpty()) {
            return null;
        }
        List<LlmToolDefinition> result = new ArrayList<>();
        for (AnthropicTool tool : tools) {
            result.add(LlmToolDefinition.builder()
                    .name(tool.getName())
                    .description(tool.getDescription())
                    .parameters(tool.getInputSchema())
                    .build());
        }
        return result;
    }

    /**
     * 从 Anthropic system 字段中提取纯文本
     * 支持两种格式:
     * 1. String: "You are helpful" → 直接返回
     * 2. List<LinkedHashMap<String, Object>>: [{type: "text", text: "You are
     * helpful", cache_control: {...}}] → 拼接所有 text
     */
    @SuppressWarnings("unchecked")
    private String extractSystemText(Object system) {
        if (system == null) {
            return null;
        }
        if (system instanceof String) {
            return (String) system;
        }
        if (system instanceof List<?> blocks) {
            StringBuilder sb = new StringBuilder();
            for (Object block : blocks) {
                if (block instanceof java.util.Map<?, ?> map) {
                    Object text = map.get("text");
                    if (text instanceof String) {
                        if (!sb.isEmpty()) {
                            sb.append("\n");
                        }
                        sb.append(text);
                    }
                }
            }
            return sb.toString();
        }
        // 兜底: 未知格式直接 toString
        return system.toString();
    }

    /**
     * 转换 Anthropic 内容块列表为内部消息（可能拆出多条）。
     * 除 text 块外必须保留工具调用历史块，否则多轮 function call 会话中
     * 上游模型看不到工具结果，会反复重新发起同一调用（客户端死循环）：
     * - assistant 消息的 tool_use 块 → 内部 assistant 消息的 toolCalls
     * - user 消息的 tool_result 块 → 独立的 tool 角色消息（带 toolCallId / name）
     * tool 消息先于同条 user 消息的剩余文本输出，保证上游 functionCall→functionResponse 相邻。
     */
    private void convertContentBlocks(String role, List<?> blocks, List<LlmMessage> out,
            java.util.Map<String, String> toolNameById,
            List<java.util.Map.Entry<LlmContent, String>> pendingImageDownloads) {
        StringBuilder text = new StringBuilder();
        List<LlmToolCall> toolCalls = new ArrayList<>();
        List<LlmMessage> toolResults = new ArrayList<>();
        // 多模态：text 与 image 保序累积，最后统一组装进 LlmMessage.contents
        List<LlmContent> contents = new ArrayList<>();

        for (Object block : blocks) {
            if (!(block instanceof java.util.Map<?, ?> blockMap)) {
                continue;
            }
            Object type = blockMap.get("type");
            if ("text".equals(type)) {
                Object t = blockMap.get("text");
                if (t instanceof String s && !s.isEmpty()) {
                    if (!text.isEmpty()) {
                        text.append("\n");
                    }
                    text.append(s);
                    contents.add(LlmContent.text(s));
                }
            } else if ("image".equals(type)) {
                // Anthropic image 块：source.type ∈ base64 / url / file
                // 用户范围：支持 base64 与 url 两种；file（Files API 引用）暂不支持，明确 400
                Object sourceObj = blockMap.get("source");
                if (sourceObj instanceof java.util.Map<?, ?> sourceMap) {
                    String sourceType = sourceMap.get("type") instanceof String s ? s : null;
                    if ("base64".equals(sourceType)) {
                        String data = sourceMap.get("data") instanceof String s ? s : null;
                        String mediaType = sourceMap.get("media_type") instanceof String s ? s : null;
                        if (data != null && !data.isEmpty()) {
                            contents.add(LlmContent.image(data, mediaType));
                        }
                    } else if ("url".equals(sourceType)) {
                        String url = sourceMap.get("url") instanceof String s ? s : null;
                        String mediaType = sourceMap.get("media_type") instanceof String s ? s : null;
                        if (url != null && !url.isEmpty()) {
                            // 先建空 base64 引用占位，下载成功后原地填充（可变 @Data 对象）
                            LlmContent pending = LlmContent.image(null, mediaType);
                            contents.add(pending);
                            pendingImageDownloads.add(java.util.Map.entry(pending, url));
                        }
                    } else {
                        // file 或未知 source 类型：明确拒绝，避免静默丢图
                        throw new LlmGatewayException(LlmErrorCode.INVALID_REQUEST,
                                "Anthropic image source.type 暂不支持: " + sourceType);
                    }
                }
            } else if ("tool_use".equals(type)) {
                String id = blockMap.get("id") instanceof String s ? s : null;
                String name = blockMap.get("name") instanceof String s ? s : null;
                if (name != null) {
                    if (id != null) {
                        toolNameById.put(id, name);
                    }
                    toolCalls.add(LlmToolCall.builder()
                            .id(id)
                            .name(name)
                            .arguments(writeJson(blockMap.get("input")))
                            .build());
                }
            } else if ("tool_result".equals(type)) {
                String toolUseId = blockMap.get("tool_use_id") instanceof String s ? s : null;
                LlmMessage toolMsg = new LlmMessage();
                toolMsg.setRole("tool");
                toolMsg.setToolCallId(toolUseId);
                toolMsg.setName(toolUseId != null ? toolNameById.get(toolUseId) : null);
                toolMsg.setTextContent(extractToolResultText(blockMap.get("content")));
                toolResults.add(toolMsg);
            }
            // image 等其它块类型暂不支持，跳过
        }

        out.addAll(toolResults);
        // 仅当有实际内容（或整条消息为空需占位）时输出 role 本体消息
        if (!text.isEmpty() || !toolCalls.isEmpty() || toolResults.isEmpty()) {
            LlmMessage m = new LlmMessage();
            m.setRole(role);
            // 多模态统一约定：contents 含 image 时，text 块全部在 contents 里（与 image 保序），
            // textContent 置 null，避免出站转换器「先 add textContent 再遍历 contents」导致文本重复。
            // 纯文本消息 contents 为 null，走原 textContent 逻辑，零回归。
            boolean hasImage = contents.stream().anyMatch(c -> "image".equals(c.getType()));
            if (hasImage) {
                m.setContents(contents);
            } else {
                m.setTextContent(text.toString());
            }
            if (!toolCalls.isEmpty()) {
                m.setToolCalls(toolCalls);
            }
            out.add(m);
        }
        log.debug("[AnthropicConverter] 转换内容块消息: role={}, 文本长度={}, toolCalls={}, toolResults={}, images={}",
                role, text.length(), toolCalls.size(), toolResults.size(),
                contents.stream().filter(c -> "image".equals(c.getType())).count());
    }

    /**
     * 提取 tool_result 块的 content 文本。
     * Anthropic 允许 String 或 [{type:"text", text:"..."}] 两种格式
     */
    private String extractToolResultText(Object content) {
        if (content == null) {
            return "";
        }
        if (content instanceof String s) {
            return s;
        }
        if (content instanceof List<?> blocks) {
            StringBuilder sb = new StringBuilder();
            for (Object block : blocks) {
                if (block instanceof java.util.Map<?, ?> map && map.get("text") instanceof String s && !s.isEmpty()) {
                    if (!sb.isEmpty()) {
                        sb.append("\n");
                    }
                    sb.append(s);
                }
            }
            return sb.toString();
        }
        return content.toString();
    }

    /** tool_use 的 input 对象 → JSON 字符串（内部 LlmToolCall.arguments 统一为字符串） */
    private String writeJson(Object obj) {
        if (obj == null) {
            return "{}";
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("[AnthropicConverter] tool_use input 序列化失败: {}", e.getMessage());
            return "{}";
        }
    }
}
