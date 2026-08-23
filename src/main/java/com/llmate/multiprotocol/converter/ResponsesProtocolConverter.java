package com.llmate.multiprotocol.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmContent;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.LlmToolDefinition;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.openai.*;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * OpenAI Responses API 协议转换器
 * 负责 /v1/responses 协议与内部标准模型的双向转换
 */
@Component
@Log4j2
public class ResponsesProtocolConverter implements ProtocolConverter<OpenAiResponsesRequest, OpenAiResponsesResponse> {

    private final ModelMappingResolver mappingResolver;
    private final ObjectMapper objectMapper;

    public ResponsesProtocolConverter(ModelMappingResolver mappingResolver, ObjectMapper objectMapper) {
        this.mappingResolver = mappingResolver;
        // 注入 Spring 单例 ObjectMapper，不各自 new 一份
        this.objectMapper = objectMapper;
    }

    @Override
    public ProtocolType getProtocolType() {
        return ProtocolType.OPENAI_RESPONSES;
    }

    @Override
    public Mono<LlmChatRequest> toInternalRequest(OpenAiResponsesRequest externalRequest) {
        if (externalRequest == null) {
            log.warn("[ResponsesConverter] 外部请求为空");
            return Mono.empty();
        }

        // 解析模型映射
        String externalModel = externalRequest.getModel();
        String internalModel = mappingResolver.resolve(externalModel, ProtocolType.OPENAI_RESPONSES);
        log.info("[ResponsesConverter] 模型映射: {} -> {}", externalModel, internalModel);

        // 从 input[] 中提取 Codex Desktop "additional_tools" 条目里的工具定义。
        // Codex Desktop 把工具放在 input 数组中（type="additional_tools"），而非顶层 tools 字段，
        // 直接读 externalRequest.getTools() 为空 → toolChoice 还在 → 上游报 400。
        // 这里先把 input 里的工具抽出、与顶层 tools 合并，再转为内部工具列表。
        List<Map<String, Object>> combinedTools = extractToolsFromInput(externalRequest.getInput());
        if (externalRequest.getTools() != null && !externalRequest.getTools().isEmpty()) {
            if (combinedTools == null) {
                combinedTools = new ArrayList<>();
            }
            combinedTools.addAll(externalRequest.getTools());
        }

        // 转换 input 为内部消息格式（会跳过 additional_tools 等非消息条目）
        // input_image 的 http URL 图片：先建空 base64 引用收集待下载，返回前响应式下载填充
        List<java.util.Map.Entry<LlmContent, String>> pendingImageDownloads = new ArrayList<>();
        List<LlmMessage> messages = convertInputToMessages(externalRequest.getInput(), pendingImageDownloads);

        // 兜底透传：Responses 显式字段（metadata / stream_options / include_reasoning）合并进 extraParams，保证零遗漏
        java.util.Map<String, Object> extraParams = new java.util.LinkedHashMap<>();
        if (externalRequest.getExtraParams() != null) {
            extraParams.putAll(externalRequest.getExtraParams());
        }

        // OpenAI Responses 的 instructions 字段（系统提示）未在 OpenAiResponsesRequest 中显式建模，
        // 经 @JsonAnySetter 落入 extraParams。这里提取出来转为 system 消息，避免泄漏到非 Responses
        // 上游协议（如 Anthropic）导致 400（"instructions: Extra inputs are not permitted"）。
        Object instructions = extraParams.remove("instructions");
        if (instructions instanceof String ins && !ins.isBlank()) {
            messages.add(0, LlmMessage.builder().role("system").textContent(ins).build());
        }

        // 客户端中断污染清洗：剥离 "[Tool use interrupted]" / "(no content)"，避免上游模型回显污染
        PollutionCleaner.clean(messages);

        log.info("[ResponsesConverter] 转换了 {} 条消息", messages.size());
        if (externalRequest.getMetadata() != null) {
            extraParams.put("metadata", externalRequest.getMetadata());
        }
        if (externalRequest.getStreamOptions() != null) {
            extraParams.put("stream_options", externalRequest.getStreamOptions());
        }
        if (externalRequest.getIncludeReasoning() != null) {
            extraParams.put("include_reasoning", externalRequest.getIncludeReasoning());
        }

        LlmChatRequest internalReq = LlmChatRequest.builder()
                .model(internalModel)
                .messages(messages)
                .maxTokens(externalRequest.getMaxOutputTokens())
                .stream(externalRequest.getStream())
                // ===== 透传字段：跨协议零遗漏 =====
                .temperature(externalRequest.getTemperature())
                .tools(toInternalTools(combinedTools))
                .toolChoice(externalRequest.getToolChoice())
                .thinking(externalRequest.getReasoning() != null
                        ? java.util.Map.of("effort", externalRequest.getReasoning().getEffort() != null
                                ? externalRequest.getReasoning().getEffort() : "medium")
                        : null)
                .topP(externalRequest.getTopP())
                .topK(externalRequest.getTopK())
                .extraParams(extraParams.isEmpty() ? null : extraParams)
                .build();

        log.info("[ResponsesConverter] 内部请求构建完成: model={}, stream={}, tools={}, reasoning={}, topP={}, topK={}",
                internalModel, externalRequest.getStream(),
                combinedTools != null ? combinedTools.size() : 0,
                externalRequest.getReasoning(),
                externalRequest.getTopP(), externalRequest.getTopK());

        // URL 图片延迟下载：convertInputToMessages 同步阶段已为每个 input_image 的 http URL 建了空 base64 引用，
        // 这里统一响应式下载（WebClientUtils 共享客户端，跟随重定向 + 5 分钟超时），setBase64Data 原地填充。
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
                    // image_url 只给 url 未带 mime → 按 URL 扩展名兜底
                    if (content.getMimeType() == null || content.getMimeType().isEmpty()) {
                        content.setMimeType(WebClientUtils.detectImageMimeType(decodedUrl));
                    }
                })
                .onErrorMap(e -> new LlmGatewayException(LlmErrorCode.IMAGE_DOWNLOAD_FAILED, url, e))
                .then();
    }

    @Override
    public OpenAiResponsesResponse toExternalResponse(LlmChatResponse internalResponse) {
        if (internalResponse == null) {
            log.warn("[ResponsesConverter] 内部响应为空");
            return null;
        }

        String responseId = "resp_" + UUID.randomUUID().toString().replace("-", "");
        log.info("[ResponsesConverter] 构建外部响应: id={}", responseId);

        // 构建输出项列表
        List<OpenAiResponsesResponse.OutputItem> outputItems = new ArrayList<>();

        if (internalResponse.getChoices() != null && !internalResponse.getChoices().isEmpty()) {
            LlmChatResponse.Choice firstChoice = internalResponse.getChoices().get(0);
            LlmChatResponse.Message msg = firstChoice.getMessage();

            // 文本消息输出项
            if (msg != null && msg.getContent() != null) {
                List<OpenAiResponsesResponse.ContentItem> contentItems = new ArrayList<>();
                contentItems.add(OpenAiResponsesResponse.ContentItem.builder()
                        .type("output_text")
                        .text(msg.getContent())
                        .build());

                outputItems.add(OpenAiResponsesResponse.OutputItem.builder()
                        .type("message")
                        .id("msg_" + UUID.randomUUID().toString().replace("-", ""))
                        .status("completed")
                        .role("assistant")
                        .content(contentItems)
                        .build());
            }

            // 工具调用输出项 (function_call 类型)
            // 关键：SDK ResponseFunctionToolCall.arguments 是 String 类型——
            // 必须传 JSON 字符串，不能是 Map/对象，否则 Kotlin/Jackson 反序列化失败
            if (msg != null && msg.getToolCalls() != null) {
                for (var tc : msg.getToolCalls()) {
                    outputItems.add(OpenAiResponsesResponse.OutputItem.builder()
                            .type("function_call")
                            .id(tc.getId())
                            .status("completed")
                            .name(tc.getName())
                            .callId(tc.getId()) // call_id 与 id 相同（非流式无独立 call_id）
                            .arguments(tc.getArguments() != null
                                    ? (Object) tc.getArguments()
                                    : (Object) "{}")
                            .build());
                }
            }
        }

        // Usage（对齐 OpenAI Responses 规范：input/output tokens + 各自的 details 子对象）
        OpenAiResponsesResponse.Usage usage = null;
        if (internalResponse.getUsage() != null) {
            LlmChatResponse.Usage u = internalResponse.getUsage();
            usage = OpenAiResponsesResponse.Usage.builder()
                    .inputTokens(u.getPromptTokens())
                    .outputTokens(u.getCompletionTokens())
                    .totalTokens(u.getTotalTokens())
                    .inputTokensDetails(OpenAiResponsesResponse.Usage.InputTokensDetails.builder()
                            .cachedTokens(u.getCachedTokens())
                            .build())
                    .outputTokensDetails(OpenAiResponsesResponse.Usage.OutputTokensDetails.builder()
                            .reasoningTokens(u.getReasoningTokens())
                            .build())
                    .build();
        }

        return OpenAiResponsesResponse.builder()
                .id(responseId)
                .object("response")
                .createdAt(Instant.now().getEpochSecond())
                .model(internalResponse.getModel())
                .output(outputItems)
                .usage(usage)
                .status("completed")
                .build();
    }

    @Override
    public Flux<ServerSentEvent<Object>> toExternalStream(Flux<LlmStreamChunk> internalStream, OpenAiResponsesRequest originalReq, String maskedModelName) {
        String responseId = "resp_" + UUID.randomUUID().toString().replace("-", "");
        String msgId = "msg_" + UUID.randomUUID().toString().replace("-", "");
        long createdAt = Instant.now().getEpochSecond();
        log.info("[ResponsesConverter] 开始流式转换: responseId={}", responseId);

        // 累积完整文本，用于 output_text.done / output_item.done / response.completed
        StringBuilder fullText = new StringBuilder();
        AtomicReference<LlmUsage> finalUsageRef = new AtomicReference<>(null);

        // sequence_number：OpenAI SDK 要求每个 SSE 事件都携带单调递增序号
        final java.util.concurrent.atomic.AtomicLong seqNum = new java.util.concurrent.atomic.AtomicLong(0);

        // 工具调用追踪：Vertex/Gemini 流式 functionCall → Responses function_call 事件
        // output_index: 0=message, 1+=function_call（按出现顺序递增）
        final java.util.concurrent.atomic.AtomicInteger toolCallOutputSeq = new java.util.concurrent.atomic.AtomicInteger(1);
        final List<java.util.Map<String, Object>> toolCallItems = java.util.Collections.synchronizedList(new ArrayList<>());
        final java.util.Set<String> startedToolCallIds = java.util.concurrent.ConcurrentHashMap.newKeySet();
        // 累积每个 tool call 的完整 arguments（delta 拼接，兼容增量流和全量流）
        final java.util.Map<String, StringBuilder> toolCallArgsBuf = new java.util.concurrent.ConcurrentHashMap<>();
        // toolCallId → output_index 映射
        final java.util.Map<String, Integer> toolCallOutputIdx = new java.util.concurrent.ConcurrentHashMap<>();
        // toolCallId → 唯一 item_id（"fc_N_resp_xxx" 格式，对齐 OpenAI 规范）
        final java.util.Map<String, String> toolCallItemId = new java.util.concurrent.ConcurrentHashMap<>();
        // 块 index → tcKey 映射：Anthropic 的 input_json_delta 事件不携带 name/id，
        // 只有块 index，需要用 content_block_start 时登记的映射回查归属
        final java.util.Map<Integer, String> toolCallIndexKey = new java.util.concurrent.ConcurrentHashMap<>();
        // 最近一次激活的 tool call key（index 缺失时的兜底关联）
        final AtomicReference<String> lastToolCallKey = new AtomicReference<>(null);

        // ---- 构建 response 对象（created / in_progress 共用骨架，对齐 OpenAI 官方格式）----
        java.util.Map<String, Object> responseBase = new java.util.LinkedHashMap<>();
        responseBase.put("id", responseId);
        responseBase.put("object", "response");
        responseBase.put("created_at", createdAt);
        responseBase.put("model", maskedModelName);
        responseBase.put("status", "in_progress");
        responseBase.put("completed_at", null);
        responseBase.put("error", null);
        responseBase.put("incomplete_details", null);
        responseBase.put("output", List.of());
        responseBase.put("usage", null);
        responseBase.put("metadata", java.util.Map.of());

        // 1) 前导事件：response.created + response.in_progress + output_item.added + content_part.added
        java.util.Map<String, Object> createdEvent = new java.util.LinkedHashMap<>();
        createdEvent.put("type", "response.created");
        createdEvent.put("sequence_number", seqNum.getAndIncrement());
        createdEvent.put("response", responseBase);

        java.util.Map<String, Object> inProgressEvent = new java.util.LinkedHashMap<>();
        inProgressEvent.put("type", "response.in_progress");
        inProgressEvent.put("sequence_number", seqNum.getAndIncrement());
        inProgressEvent.put("response", responseBase);

        java.util.Map<String, Object> itemAddedEvent = new java.util.LinkedHashMap<>();
        itemAddedEvent.put("type", "response.output_item.added");
        itemAddedEvent.put("sequence_number", seqNum.getAndIncrement());
        itemAddedEvent.put("output_index", 0);
        itemAddedEvent.put("item", java.util.Map.of(
                "type", "message",
                "id", msgId,
                "status", "in_progress",
                "role", "assistant",
                "content", List.of()
        ));

        java.util.Map<String, Object> partAddedEvent = new java.util.LinkedHashMap<>();
        partAddedEvent.put("type", "response.content_part.added");
        partAddedEvent.put("sequence_number", seqNum.getAndIncrement());
        partAddedEvent.put("output_index", 0);
        partAddedEvent.put("content_index", 0);
        partAddedEvent.put("item_id", msgId);
        partAddedEvent.put("part", java.util.Map.of(
                "type", "output_text",
                "text", ""
        ));

        Flux<ServerSentEvent<Object>> preamble = Flux.just(
                buildSSE("response.created", createdEvent),
                buildSSE("response.in_progress", inProgressEvent),
                buildSSE("response.output_item.added", itemAddedEvent),
                buildSSE("response.content_part.added", partAddedEvent)
        );

        // 2) 增量事件流（flatMapIterable：单个 chunk 可能产出 0~2 个 SSE 事件）
        Flux<ServerSentEvent<Object>> dataStream = internalStream.flatMapIterable(chunk -> {
            // 收集 usage
            if (chunk.getUsage() != null) {
                finalUsageRef.set(chunk.getUsage());
            }

            List<ServerSentEvent<Object>> events = new ArrayList<>();

            // --- 文本增量 ---
            String deltaText = chunk.getDeltaContent() != null ? chunk.getDeltaContent() : "";
            if (!deltaText.isEmpty()) {
                fullText.append(deltaText);
                java.util.Map<String, Object> deltaEvent = new java.util.LinkedHashMap<>();
                deltaEvent.put("type", "response.output_text.delta");
                deltaEvent.put("sequence_number", seqNum.getAndIncrement());
                deltaEvent.put("item_id", msgId);
                deltaEvent.put("output_index", 0);
                deltaEvent.put("content_index", 0);
                deltaEvent.put("delta", deltaText);
                events.add(buildSSE("response.output_text.delta", deltaEvent));
            }

            // --- 工具调用增量 ---
            // 注意：Anthropic 流式下 name/id 只在 content_block_start 出现一次，
            // 后续 input_json_delta 的 chunk 只有 toolCallArgumentsDelta（+index），
            // 若仅以 toolCallName != null 为入口会把所有 arguments 增量丢弃，
            // 客户端拿到空参数的 function_call（Codex 报 missing field）。
            if (chunk.getToolCallName() != null || chunk.getToolCallArgumentsDelta() != null) {
                String tcKey;
                if (chunk.getToolCallName() != null) {
                    // 携带 name 的 chunk（tool call 开始或全量）：登记 key 与 index 映射
                    // Gemini 流式 functionCall 没有独立 call_id，使用函数名作为唯一标识
                    tcKey = chunk.getToolCallId() != null ? chunk.getToolCallId() : chunk.getToolCallName();
                    if (chunk.getToolCallIndex() != null) {
                        toolCallIndexKey.put(chunk.getToolCallIndex(), tcKey);
                    }
                    lastToolCallKey.set(tcKey);
                } else {
                    // 只有 arguments 增量：优先按块 index 关联，缺失时用最近激活的 tool call 兜底
                    tcKey = chunk.getToolCallIndex() != null ? toolCallIndexKey.get(chunk.getToolCallIndex()) : null;
                    if (tcKey == null) {
                        tcKey = lastToolCallKey.get();
                    }
                }

                if (tcKey == null) {
                    log.warn("[ResponsesConverter] 收到无法关联到任何 tool call 的 arguments 增量，已丢弃: {}",
                            chunk.getToolCallArgumentsDelta());
                } else {
                    String tcArgsDelta = chunk.getToolCallArgumentsDelta() != null ? chunk.getToolCallArgumentsDelta() : "";

                    // 累积 arguments（兼容 Gemini 一次性全量 + OpenAI/Anthropic 流式增量两种模式）
                    toolCallArgsBuf.computeIfAbsent(tcKey, k -> new StringBuilder()).append(tcArgsDelta);

                    if (chunk.getToolCallName() != null && startedToolCallIds.add(tcKey)) {
                        String tcName = chunk.getToolCallName();
                        // 首次出现 → 发送 response.output_item.added
                        int outputIdx = toolCallOutputSeq.getAndIncrement();
                        toolCallOutputIdx.put(tcKey, outputIdx);
                        // 生成唯一 item_id：对齐 OpenAI "fc_N_resp_xxxxxxxx" 格式
                        String itemId = "fc_" + outputIdx + "_" + responseId.substring(5, 13);
                        toolCallItemId.put(tcKey, itemId);
                        // 生成唯一 call_id：用于将 function_call_output 与 function_call 关联
                        String callId = "call_" + outputIdx + "_" + responseId.substring(5, 13);

                        java.util.Map<String, Object> tcItemMeta = new java.util.LinkedHashMap<>();
                        tcItemMeta.put("type", "function_call");
                        tcItemMeta.put("id", itemId);
                        tcItemMeta.put("call_id", callId);
                        tcItemMeta.put("status", "in_progress");
                        tcItemMeta.put("name", tcName);
                        tcItemMeta.put("arguments", "");
                        tcItemMeta.put("_outputIdx", outputIdx);
                        toolCallItems.add(tcItemMeta);

                        java.util.LinkedHashMap<String, Object> itemObj = new java.util.LinkedHashMap<>();
                        itemObj.put("type", "function_call");
                        itemObj.put("id", itemId);
                        itemObj.put("call_id", callId);
                        itemObj.put("status", "in_progress");
                        itemObj.put("name", tcName);
                        itemObj.put("arguments", "");
                        java.util.Map<String, Object> itemAdded = new java.util.LinkedHashMap<>();
                        itemAdded.put("type", "response.output_item.added");
                        itemAdded.put("sequence_number", seqNum.getAndIncrement());
                        itemAdded.put("output_index", outputIdx);
                        itemAdded.put("item", java.util.Collections.unmodifiableMap(itemObj));
                        events.add(buildSSE("response.output_item.added", itemAdded));
                        log.debug("[ResponsesConverter] 新增工具调用项: id={}, call_id={}, name={}, outputIdx={}", itemId, callId, tcName, outputIdx);
                    }

                    // 发送 function_call_arguments.delta（空增量不发，避免 content_block_start 产生空 delta 事件）
                    if (!tcArgsDelta.isEmpty()) {
                        int outputIdx = toolCallOutputIdx.getOrDefault(tcKey, -1);
                        String itemId = toolCallItemId.get(tcKey);
                        java.util.Map<String, Object> argsDelta = new java.util.LinkedHashMap<>();
                        argsDelta.put("type", "response.function_call_arguments.delta");
                        argsDelta.put("sequence_number", seqNum.getAndIncrement());
                        argsDelta.put("item_id", itemId);
                        argsDelta.put("output_index", outputIdx);
                        argsDelta.put("delta", tcArgsDelta);
                        events.add(buildSSE("response.function_call_arguments.delta", argsDelta));
                    }
                }
            }

            // 注意：不再因为 isFinished 而丢弃 events。
            // 文本 delta 有 !isEmpty() 守卫，工具调用有 != null 守卫，
            // 空 chunk（finished 且无数据）本身就不会产事件。
            // 若 finished chunk 同时携带 tool call，事件正常发出，尾部 epilogue 负责 done。

            return events;
        });

        // 3) 尾部事件：output_text.done → content_part.done → output_item.done
        //    → [每个工具调用: function_call_arguments.done → output_item.done]
        //    → response.completed
        Flux<ServerSentEvent<Object>> epilogue = Flux.defer(() -> {
            String completedText = fullText.toString();
            int toolCallCount = toolCallItems.size();
            log.info("[ResponsesConverter] 流结束，发送尾部事件: 文本长度={}, 工具调用数={}", completedText.length(), toolCallCount);

            List<ServerSentEvent<Object>> tailEvents = new ArrayList<>();

            // ---- 文本消息的 done 事件（output_index=0）----
            java.util.Map<String, Object> completedContentPart = java.util.Map.of(
                    "type", "output_text",
                    "text", completedText
            );

            java.util.Map<String, Object> completedMessage = new java.util.LinkedHashMap<>();
            completedMessage.put("type", "message");
            completedMessage.put("id", msgId);
            completedMessage.put("status", "completed");
            completedMessage.put("role", "assistant");
            completedMessage.put("content", List.of(completedContentPart));

            tailEvents.add(buildSSE("response.output_text.done", java.util.Map.of(
                    "type", "response.output_text.done",
                    "sequence_number", seqNum.getAndIncrement(),
                    "item_id", msgId,
                    "output_index", 0,
                    "content_index", 0,
                    "text", completedText
            )));
            tailEvents.add(buildSSE("response.content_part.done", java.util.Map.of(
                    "type", "response.content_part.done",
                    "sequence_number", seqNum.getAndIncrement(),
                    "item_id", msgId,
                    "output_index", 0,
                    "content_index", 0,
                    "part", completedContentPart
            )));
            tailEvents.add(buildSSE("response.output_item.done", java.util.Map.of(
                    "type", "response.output_item.done",
                    "sequence_number", seqNum.getAndIncrement(),
                    "output_index", 0,
                    "item", completedMessage
            )));

            // ---- 构建完整 output 列表（message + function_call 项）----
            List<java.util.Map<String, Object>> completedOutputItems = new ArrayList<>();
            completedOutputItems.add(completedMessage);

            // ---- 每个工具调用的 done 事件 ----
            for (java.util.Map<String, Object> tcMeta : toolCallItems) {
                String itemId = (String) tcMeta.get("id");
                String tcName = (String) tcMeta.get("name");
                String callId = (String) tcMeta.get("call_id");
                int outputIdx = (int) tcMeta.get("_outputIdx");

                // 累积的完整 arguments（delta 可能为空时兜底为 "{}"）
                // 通过 tcMeta 中的 id/itemId 无法反向映射到 tcKey，但 toolCallArgsBuf
                // 的 key 是原始 tcKey（函数名）。遍历找匹配的(argsBuf 仅1个entry)。
                String fullArgsStr = "{}";
                for (var entry : toolCallArgsBuf.entrySet()) {
                    if (itemId.equals(toolCallItemId.get(entry.getKey()))) {
                        StringBuilder buf = entry.getValue();
                        fullArgsStr = (buf != null && buf.length() > 0) ? buf.toString() : "{}";
                        break;
                    }
                }

                // function_call_arguments.done —— 必须包含 name 字段（SDK 强制校验）
                tailEvents.add(buildSSE("response.function_call_arguments.done", java.util.Map.of(
                        "type", "response.function_call_arguments.done",
                        "sequence_number", seqNum.getAndIncrement(),
                        "item_id", itemId,
                        "output_index", outputIdx,
                        "name", tcName,
                        "arguments", fullArgsStr
                )));

                // 构建 completed function_call item。
                // 关键：SDK ResponseFunctionToolCall.arguments 是 String 类型——
                // 必须传 JSON 字符串（如 "{\"command\":\"...\"}"），
                // 不能传 JSON 对象（如 {"command":"..."}）。
                // 传入 Map/对象会导致 Kotlin/Jackson 反序列化时类型不匹配而丢弃该 item。
                java.util.Map<String, Object> completedTcItem = new java.util.LinkedHashMap<>();
                completedTcItem.put("type", "function_call");
                completedTcItem.put("id", itemId);
                completedTcItem.put("call_id", callId);
                completedTcItem.put("status", "completed");
                completedTcItem.put("name", tcName);
                completedTcItem.put("arguments", fullArgsStr);
                completedOutputItems.add(completedTcItem);

                // output_item.done
                tailEvents.add(buildSSE("response.output_item.done", java.util.Map.of(
                        "type", "response.output_item.done",
                        "sequence_number", seqNum.getAndIncrement(),
                        "output_index", outputIdx,
                        "item", completedTcItem
                )));
                log.debug("[ResponsesConverter] 工具调用完成: id={}, name={}, argsLen={}", itemId, tcName, fullArgsStr.length());
            }

            // ---- 构建 usage ----
            java.util.Map<String, Object> usageMap;
            LlmUsage usage = finalUsageRef.get();
            if (usage != null) {
                usageMap = new java.util.LinkedHashMap<>();
                usageMap.put("input_tokens", usage.getPromptTokens());
                usageMap.put("output_tokens", usage.getCompletionTokens());
                usageMap.put("total_tokens", usage.getTotalTokens());
                usageMap.put("input_tokens_details", java.util.Map.of("cached_tokens", usage.getCachedTokens()));
                usageMap.put("output_tokens_details", java.util.Map.of("reasoning_tokens", usage.getReasoningTokens()));
            } else {
                log.info("[ResponsesConverter] 上游未返回usage，使用默认零值");
                usageMap = new java.util.LinkedHashMap<>();
                usageMap.put("input_tokens", 0);
                usageMap.put("output_tokens", 0);
                usageMap.put("total_tokens", 0);
                usageMap.put("input_tokens_details", java.util.Map.of("cached_tokens", 0));
                usageMap.put("output_tokens_details", java.util.Map.of("reasoning_tokens", 0));
            }

            // ---- response.completed ----
            java.util.Map<String, Object> completedResponseObj = new java.util.LinkedHashMap<>();
            completedResponseObj.put("id", responseId);
            completedResponseObj.put("object", "response");
            completedResponseObj.put("created_at", createdAt);
            completedResponseObj.put("completed_at", Instant.now().getEpochSecond());
            completedResponseObj.put("status", "completed");
            completedResponseObj.put("model", maskedModelName);
            completedResponseObj.put("output", completedOutputItems);
            completedResponseObj.put("incomplete_details", null);
            completedResponseObj.put("error", null);
            completedResponseObj.put("usage", usageMap);
            completedResponseObj.put("metadata", java.util.Map.of());

            tailEvents.add(buildSSE("response.completed", java.util.Map.of(
                    "type", "response.completed",
                    "sequence_number", seqNum.getAndIncrement(),
                    "response", completedResponseObj
            )));

            return Flux.fromIterable(tailEvents);
        });

        // 4) 拼接：前导 + 增量 + 尾部 + [DONE]
        return preamble
                .concatWith(dataStream)
                .concatWith(epilogue)
                .concatWith(Flux.just(ServerSentEvent.builder().data("[DONE]").build()))
                .doOnComplete(() -> log.info("[ResponsesConverter] 流式转换完成"))
                // 上游流错误禁止在此吞成 SSE error（会丢失统一脱敏 + request_id），必须 Flux.error 传播到
                // Controller 的 onErrorResume 统一处理（GatewayErrorResponseBuilder.streamErrorEvents）
                .doOnError(e -> log.error("[ResponsesConverter] 流式转换错误", e));
    }

    /**
     * 构建 SSE 事件：同时设置 event 类型和 data 内容
     * OpenAI Responses API 要求每个事件同时有 SSE event 行和 JSON data 中的 type 字段
     * AI SDK 通过 SSE event 行区分事件类型，JSON type 字段供客户端二次校验
     */
    private ServerSentEvent<Object> buildSSE(String eventType, Object data) {
        return ServerSentEvent.builder()
                .event(eventType)
                .data(data)
                .build();
    }

    @Override
    public String extractModelName(OpenAiResponsesRequest externalRequest) {
        return externalRequest != null ? externalRequest.getModel() : null;
    }

    /**
     * 从 input[] 数组中提取工具定义。
     *
     * Codex Desktop 把工具放在 input 数组里的 "additional_tools" 条目中
     * （而非 Responses API 顶层 "tools" 字段），格式为：
     * <pre>{@code
     *   { "type": "additional_tools", "role": "developer",
     *     "tools": [{"type": "custom", "name": "...", "description": "...", "parameters": {...}}] }
     * }</pre>
     * 这里遍历 input 数组，收集所有 additional_tools.tools 并扁平化合并。
     *
     * @param input 原始 input 字段（可能为 String / List / null）
     * @return 扁平化的工具定义列表，或 null（没有找到时）
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractToolsFromInput(Object input) {
        if (!(input instanceof List<?> inputList)) {
            return null;
        }
        List<Map<String, Object>> result = null;
        for (Object item : inputList) {
            if (!(item instanceof java.util.Map<?, ?> itemMap)) {
                continue;
            }
            if (!"additional_tools".equals(itemMap.get("type"))) {
                continue;
            }
            Object toolsField = itemMap.get("tools");
            if (toolsField instanceof List<?> toolsList) {
                if (result == null) {
                    result = new ArrayList<>();
                }
                for (Object toolObj : toolsList) {
                    if (toolObj instanceof java.util.Map) {
                        result.add((Map<String, Object>) toolObj);
                    }
                }
            }
        }
        return result;
    }

    /**
     * Responses API 工具定义 → 内部标准工具定义
     * Responses 的 tools 元素是扁平结构：{type: function, name, description, parameters}
     * 也兼容 Chat 风格的 {type: function, function: {name, description, parameters}}
     */
    @SuppressWarnings("unchecked")
    private List<LlmToolDefinition> toInternalTools(List<Map<String, Object>> tools) {
        if (tools == null || tools.isEmpty()) {
            return null;
        }
        List<LlmToolDefinition> result = new ArrayList<>();
        for (Map<String, Object> toolMap : tools) {
            String name = null;
            String description = null;
            Object parameters = null;

            Object function = toolMap.get("function");
            if (function instanceof Map<?, ?> func) {
                name = func.get("name") instanceof String ? (String) func.get("name") : null;
                description = func.get("description") instanceof String ? (String) func.get("description") : null;
                parameters = func.get("parameters");
                // Codex Desktop 等客户端可能用 parametersJsonSchema 替代 parameters
                if (parameters == null) {
                    parameters = func.get("parametersJsonSchema");
                }
            } else {
                name = toolMap.get("name") instanceof String ? (String) toolMap.get("name") : null;
                description = toolMap.get("description") instanceof String ? (String) toolMap.get("description") : null;
                parameters = toolMap.get("parameters");
                if (parameters == null) {
                    parameters = toolMap.get("parametersJsonSchema");
                }
            }

            // 跳过没有 name 的工具（Codex Desktop 的 additional_tools 数组
            // 中可能混入非工具条目如分组标记等，Gemini 上游要求 name 必填）
            if (name == null || name.isBlank()) {
                log.debug("[ResponsesConverter] 跳过无名称的工具定义: description={}", description);
                continue;
            }

            result.add(LlmToolDefinition.builder()
                    .name(name)
                    .description(description)
                    .parameters(parameters instanceof Map ? (Map<String, Object>) parameters : null)
                    .build());
        }
        return result;
    }

    /**
     * 将 Responses API 的 input 字段转换为内部消息列表
     * 支持多种格式:
     * 1. String: "hello" → 单条 user 消息
     * 2. List<Map>: [{role, content}] 其中 content 可以是 String 或 List<{type, text}>
     * 3. List 中的元素也可能是纯文本 String
     */
    @SuppressWarnings("unchecked")
    private List<LlmMessage> convertInputToMessages(Object input,
            List<java.util.Map.Entry<LlmContent, String>> pendingImageDownloads) {
        List<LlmMessage> messages = new ArrayList<>();

        if (input instanceof String) {
            // 简单字符串输入
            messages.add(LlmMessage.builder()
                    .role("user")
                    .textContent((String) input)
                    .build());
            log.debug("[ResponsesConverter] 转换字符串输入: {}", ((String) input).substring(0, Math.min(30, ((String) input).length())));
        } else if (input instanceof List) {
            List<?> inputList = (List<?>) input;
            // call_id → function name 映射：function_call_output 不含 name 字段，
            // 需从前面的 function_call 条目提取，供 tool 消息的 .name() 使用
            java.util.Map<String, String> callIdToName = new java.util.LinkedHashMap<>();
            for (Object item : inputList) {
                if (item instanceof String) {
                    messages.add(LlmMessage.builder()
                            .role("user")
                            .textContent((String) item)
                            .build());
                } else if (item instanceof java.util.Map) {
                    java.util.Map<String, Object> map = (java.util.Map<String, Object>) item;

                    // 跳过 Codex Desktop 的 additional_tools 条目——这是工具声明不是对话消息，
                    // 工具已在 extractToolsFromInput() 中提前提取并喂给 toInternalTools()。
                    if ("additional_tools".equals(map.get("type"))) {
                        continue;
                    }

                    // ---- 处理 function_call 条目（assistant 发起的工具调用）----
                    if ("function_call".equals(map.get("type"))) {
                        String fcName = (String) map.get("name");
                        String fcCallId = (String) map.get("call_id");
                        Object fcArgs = map.get("arguments");
                        String fcArgsStr = fcArgs instanceof String ? (String) fcArgs : "";
                        // 记录 call_id → name 映射，供后续 function_call_output 使用
                        if (fcCallId != null && fcName != null) {
                            callIdToName.put(fcCallId, fcName);
                        }
                        messages.add(LlmMessage.builder()
                                .role("assistant")
                                .toolCalls(List.of(LlmToolCall.builder()
                                        .id(fcCallId != null ? fcCallId : "call_unknown")
                                        .type("function")
                                        .name(fcName)
                                        .arguments(fcArgsStr)
                                        .build()))
                                .build());
                        log.debug("[ResponsesConverter] 转换 function_call: name={}, call_id={}", fcName, fcCallId);
                        continue;
                    }

                    // ---- 处理 function_call_output 条目（工具执行结果）----
                    if ("function_call_output".equals(map.get("type"))) {
                        String fcoCallId = (String) map.get("call_id");
                        Object fcoOutput = map.get("output");
                        String fcoOutputStr;
                        if (fcoOutput instanceof String) {
                            fcoOutputStr = (String) fcoOutput;
                        } else if (fcoOutput != null) {
                            fcoOutputStr = fcoOutput.toString();
                        } else {
                            fcoOutputStr = "";
                        }
                        // function_call_output 不含 name → 从前面 function_call 的 call_id 映射取
                        String fcoName = (String) map.get("name");
                        if (fcoName == null && fcoCallId != null) {
                            fcoName = callIdToName.get(fcoCallId);
                        }
                        messages.add(LlmMessage.builder()
                                .role("tool")
                                .toolCallId(fcoCallId)
                                .name(fcoName)
                                .textContent(fcoOutputStr)
                                .build());
                        log.debug("[ResponsesConverter] 转换 function_call_output: call_id={}, name={}, outputLen={}", fcoCallId, fcoName, fcoOutputStr.length());
                        continue;
                    }

                    String role = (String) map.getOrDefault("role", "user");
                    // OpenAI Responses API 的 "developer" 角色语义等同于 "system"，
                    // 大部分上游 Provider 不支持该角色，统一映射为 system
                    if ("developer".equals(role)) {
                        role = "system";
                    }
                    Object content = map.get("content");

                    // 多模态：content 列表含 input_image → 走 contents 保序路径（text + image 交错）。
                    // 纯文本仍走原 extractTextFromContent 逻辑，零回归。
                    boolean hasImage = content instanceof List<?> list
                            && list.stream().anyMatch(b -> b instanceof java.util.Map<?, ?> bm
                                    && "input_image".equals(bm.get("type")));
                    // 文件引用（input_file / file_id）用户范围暂不支持 → 明确 400，
                    // 与 Anthropic source.type=file 一致，避免「文件被静默丢弃、模型看不到输入」。
                    if (content instanceof List<?> fileList && fileList.stream().anyMatch(b ->
                            b instanceof java.util.Map<?, ?> bm && "input_file".equals(bm.get("type")))) {
                        throw new LlmGatewayException(LlmErrorCode.INVALID_REQUEST,
                                "Responses input_file / file_id 暂不支持，请改用 input_image（URL 或 base64）");
                    }
                    if (hasImage) {
                        List<LlmContent> contents = new ArrayList<>();
                        @SuppressWarnings("unchecked")
                        List<Object> rawBlocks = (List<Object>) content;
                        for (Object block : rawBlocks) {
                            if (!(block instanceof java.util.Map<?, ?> bm)) {
                                continue;
                            }
                            String btype = bm.get("type") instanceof String s ? s : null;
                            if ("input_text".equals(btype) || "text".equals(btype)) {
                                Object t = bm.get("text");
                                if (t instanceof String s && !s.isEmpty()) {
                                    contents.add(LlmContent.text(s));
                                }
                            } else if ("input_image".equals(btype)) {
                                // input_image 的 image_url：兼容「url 字符串」与「{url, detail}」对象两种形态
                                Object imageUrl = bm.get("image_url");
                                String url = null;
                                if (imageUrl instanceof String s) {
                                    url = s;
                                } else if (imageUrl instanceof java.util.Map<?, ?> imgMap
                                        && imgMap.get("url") instanceof String s) {
                                    url = s;
                                }
                                if (url == null || url.isEmpty()) {
                                    continue;
                                }
                                if (url.startsWith("data:")) {
                                    // data URI：base64 直接解析
                                    String[] seg = url.split(",");
                                    if (seg.length >= 2) {
                                        String mime = url.substring(url.indexOf(":") + 1, url.indexOf(";"));
                                        contents.add(LlmContent.image(seg[1], mime));
                                    }
                                } else {
                                    // http URL：建空 base64 引用，返回前响应式下载填充
                                    LlmContent pending = LlmContent.image(null, null);
                                    contents.add(pending);
                                    pendingImageDownloads.add(java.util.Map.entry(pending, url));
                                }
                            }
                            // 其它类型（input_file 已在上面统一 400；其余未知类型）静默跳过
                        }
                        if (!contents.isEmpty()) {
                            messages.add(LlmMessage.builder()
                                    .role(role)
                                    .contents(contents)
                                    .build());
                        }
                    } else {
                        String textContent = extractTextFromContent(content);

                        // 如果 content 为空，尝试从 map 中直接取 text 字段（某些 output item 格式）
                        if (textContent == null || textContent.isEmpty()) {
                            Object directText = map.get("text");
                            if (directText instanceof String) {
                                textContent = (String) directText;
                            }
                        }

                        if (textContent != null && !textContent.isEmpty()) {
                            messages.add(LlmMessage.builder()
                                    .role(role)
                                    .textContent(textContent)
                                    .build());
                        }
                    }
                }
            }
        }

        return messages;
    }

    /**
     * 从 content 字段中提取纯文本
     * content 可以是:
     * 1. String: 直接返回
     * 2. List<Map>: [{type: "input_text"/"text", text: "..."}] → 拼接所有 text
     */
    @SuppressWarnings("unchecked")
    private String extractTextFromContent(Object content) {
        if (content == null) {
            return null;
        }
        if (content instanceof String) {
            return (String) content;
        }
        if (content instanceof List<?> contentList) {
            StringBuilder sb = new StringBuilder();
            for (Object block : contentList) {
                if (block instanceof java.util.Map<?, ?> blockMap) {
                    Object text = blockMap.get("text");
                    if (text instanceof String) {
                        if (!sb.isEmpty()) {
                            sb.append("\n");
                        }
                        sb.append(text);
                    }
                } else if (block instanceof String) {
                    if (!sb.isEmpty()) {
                        sb.append("\n");
                    }
                    sb.append(block);
                }
            }
            return sb.toString();
        }
        return content.toString();
    }

    /**
     * 将 JSON 字符串格式的工具调用参数解析为 Map
     * Responses API 的 function_call 输出项需要 arguments 为 Map 类型
     */
    @SuppressWarnings("unchecked")
    private java.util.Map<String, Object> parseArgumentsToMap(String argumentsJson) {
        if (argumentsJson == null || argumentsJson.isBlank()) {
            return java.util.Map.of();
        }
        try {
            return objectMapper.readValue(argumentsJson, java.util.Map.class);
        } catch (Exception e) {
            log.warn("[ResponsesConverter] 工具调用参数解析失败，返回空Map: {}", e.getMessage());
            return java.util.Map.of();
        }
    }
}
