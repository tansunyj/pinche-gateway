package com.llmate.multiprotocol.converter.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.openai.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 流式 SSE 装配辅助组件
 * 负责将内部标准流数据装配为符合 OpenAI SSE 规范的流
 */
@Component
@Log4j2
public class StreamingConverter {

    private final ObjectMapper objectMapper;

    public StreamingConverter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * 将内部通用流数据转换为标准标准的 OpenAI SSE 规范流
     */
    public Flux<ServerSentEvent<Object>> toOpenAiStream(
            Flux<LlmStreamChunk> internalStream,
            OpenAiChatRequest originalReq,
            String maskedModelName) {

        String streamId = "chatcmpl-" + UUID.randomUUID().toString().replace("-", "");
        AtomicBoolean firstChunk = new AtomicBoolean(true);
        AtomicReference<LlmUsage> finalUsageRef = new AtomicReference<>(null);
        // 追踪本回合是否已发出工具调用增量：流结束时若为 true，finish_reason 必须改写为 "tool_calls"
        AtomicBoolean toolCallEmitted = new AtomicBoolean(false);
        // 工具调用 id 规范化缓存：上游原始 id → OpenAI 规范 id（call_ 前缀）。
        // Gemini 上游 functionCall 的 id 直接用函数名（如 "Bash"），合法但非规范；
        // 部分客户端（Claude Code）可能按 id 格式校验。Gemini 关联 functionResponse
        // 用 name 不用 id（见 VertexFormatConverter），故可安全改写，客户端回传的
        // tool_call_id 仅透传做存在性校验，不影响上游关联。
        Map<String, String> canonicalToolCallIds = new HashMap<>();

        log.info("[StreamingConverter] 开始流式转换: streamId={}, maskedModel={}", streamId, maskedModelName);

        Flux<ServerSentEvent<Object>> dataStream = internalStream.map(chunk -> {
            log.debug("[StreamingConverter] 收到内部chunk: deltaContent={}, finished={}", chunk.getDeltaContent(), chunk.isFinished());

            if (chunk.getUsage() != null) {
                finalUsageRef.set(chunk.getUsage());
            }

            OpenAiDelta.OpenAiDeltaBuilder deltaBuilder = OpenAiDelta.builder();
            // OpenAI 规范：role 只在流首 chunk 出现，后续 chunk 不重复
            if (firstChunk.compareAndSet(true, false)) {
                deltaBuilder.role("assistant");
            }

            // 工具调用流式增量装配
            boolean isToolCallChunk = chunk.getToolCallId() != null || chunk.getToolCallArgumentsDelta() != null;
            if (isToolCallChunk) {
                toolCallEmitted.set(true);
                // 【兼容修复】OpenAI 流式协议要求 tool_calls[].index 必填，客户端 SDK 按 index 拼装工具调用片段。
                // 部分上游（如 Gemini functionCall）不设置 index，若直接透传 null，
                // OpenAiToolCallDelta @JsonInclude(NON_NULL) 会省略该字段，
                // 客户端（Claude Desktop / Agent SDK 等）拼不出完整工具调用而丢弃 → 表现为"无可见输出/空白"。
                // 这里统一兜底为 0。
                String rawToolCallId = chunk.getToolCallId();
                String canonicalToolCallId = rawToolCallId != null
                        ? canonicalToolCallIds.computeIfAbsent(rawToolCallId, k ->
                                (k.startsWith("call_") || k.startsWith("toolu_") || k.startsWith("id_"))
                                        ? k
                                        : "call_" + UUID.randomUUID().toString().replace("-", ""))
                        : null;
                deltaBuilder.toolCalls(List.of(OpenAiToolCallDelta.builder()
                        .index(chunk.getToolCallIndex() != null ? chunk.getToolCallIndex() : 0)
                        .id(canonicalToolCallId)
                        .function(OpenAiFunctionDelta.builder()
                                .name(chunk.getToolCallName())
                                .arguments(chunk.getToolCallArgumentsDelta())
                                .build())
                        .build()));
            } else {
                // 仅在确有文本增量时携带 content；结束 chunk（content 为空串）保持 delta={}，与 OpenAI 规范一致
                if (chunk.getDeltaContent() != null && !chunk.getDeltaContent().isEmpty()) {
                    deltaBuilder.content(chunk.getDeltaContent());
                }
                // 推理增量单独输出到 reasoning_content，绝不拼入 content（reasoning_content 是独立字段）
                if (chunk.getDeltaReasoningContent() != null && !chunk.getDeltaReasoningContent().isEmpty()) {
                    deltaBuilder.reasoningContent(chunk.getDeltaReasoningContent());
                }
            }

            String finishReason = resolveFinishReason(chunk, toolCallEmitted.get());

            OpenAiStreamChunk sseChunk = OpenAiStreamChunk.builder()
                    .id(streamId)
                    .object("chat.completion.chunk")
                    .created(Instant.now().getEpochSecond())
                    .model(maskedModelName)
                    .choices(List.of(OpenAiStreamChoice.builder()
                            .index(0)
                            .delta(deltaBuilder.build())
                            .finishReason(finishReason)
                            .build()))
                    .build();

            ServerSentEvent<Object> sse = ServerSentEvent.builder().data(sseChunk).build();
            // 输出层完整信息：文本 + 工具调用（id/name/args）+ finishReason，方便对端到端排查流式问题
            log.debug("[StreamingConverter] 输出SSE chunk: id={}, deltaContent={}, toolCallId={}, toolCallName={}, toolCallArgs={}, finishReason={}",
                    streamId, chunk.getDeltaContent(), chunk.getToolCallId(), chunk.getToolCallName(),
                    chunk.getToolCallArgumentsDelta(), finishReason);
            // 序列化后的真实 wire JSON：核对与 OpenAI 规范字节级一致（排查 Claude Desktop 中断工具调用）
            if (log.isDebugEnabled()) {
                try {
                    log.debug("[StreamingConverter] SSE wire JSON: {}", objectMapper.writeValueAsString(sseChunk));
                } catch (Exception e) {
                    log.debug("[StreamingConverter] SSE wire JSON 序列化失败: {}", e.getMessage());
                }
            }
            return sse;
        });

        // 延迟追加：在 [DONE] 前附加 usage 包（token 统计）。
        // 网关总是向上游请求 include_usage（计费需要），拿到 usage 后【无条件】下发给客户端，
        // 便于核对 prompt/completion/total tokens —— 即使客户端请求体没带 stream_options.include_usage 也会收到。
        return dataStream.concatWith(Flux.defer(() -> {
            log.info("[StreamingConverter] 流结束，追加最终事件");
            List<ServerSentEvent<Object>> finalEvents = new ArrayList<>();

            if (finalUsageRef.get() != null) {
                OpenAiStreamChunk usageEnvelope = OpenAiStreamChunk.builder()
                        .id(streamId).object("chat.completion.chunk").model(maskedModelName)
                        .choices(List.of()) // 依据 OpenAI 最新规范，携带 usage 的包 choices 必须为空
                        .usage(mapToOpenAiUsage(finalUsageRef.get()))
                        .build();
                finalEvents.add(ServerSentEvent.builder().data(usageEnvelope).build());
            }

            finalEvents.add(ServerSentEvent.builder().data("[DONE]").build());
            log.info("[StreamingConverter] 发送[DONE]标记");
            return Flux.fromIterable(finalEvents);
        }));
    }

    /**
     * 解析输出 chunk 的 finish_reason。
     *
     * 【兼容修复】OpenAI 流式协议要求：回合发出工具调用后，流结束的 chunk 必须携带
     * finish_reason="tool_calls"（而不是 "stop"）。Claude Desktop / Agent SDK 依据
     * finish_reason 判断"该回合是否待执行工具调用"：
     * - 收到 tool_calls 增量后若 finish_reason="stop"，客户端会把回合当作"正常文本结束"，
     *   已收到的工具调用被判为无效而中断 → 记录 [Tool use interrupted] → 自动重试死循环。
     *
     * 上游 Gemini 在 functionCall 之后的事件是 finishReason="STOP"，故必须在流结束时
     * （只要本回合发出过工具调用）强制改写为 "tool_calls"，优先级最高。
     * 其余情况：优先透传上游原始 finishReason，否则在 finished=true 时默认 "stop"。
     */
    private String resolveFinishReason(LlmStreamChunk chunk, boolean toolCallEmitted) {
        if (chunk.isFinished() && toolCallEmitted) {
            return "tool_calls";
        }
        if (chunk.getFinishReason() != null) {
            return chunk.getFinishReason();
        }
        return chunk.isFinished() ? "stop" : null;
    }

    private OpenAiUsage mapToOpenAiUsage(LlmUsage internal) {
        // 缓存命中 tokens 三源合并：Anthropic 风格 cache_read_input_tokens / OpenAI 风格
        // prompt_tokens_details.cached_tokens（OpenAiFormatConverter 解析进 cachedTokens）/
        // 旧格式 prompt_cache_hit_tokens（cacheHitTokens）。
        // 客户端按 OpenAI 规范在 prompt_tokens_details.cached_tokens 查看缓存命中，
        // 之前漏了该嵌套字段 → OpenAI 协议客户端看不到缓存消耗。
        int cacheReadForDetails = internal.getCacheReadTokens() > 0 ? internal.getCacheReadTokens()
                : (internal.getCachedTokens() > 0 ? internal.getCachedTokens() : internal.getCacheHitTokens());
        return OpenAiUsage.builder()
            .promptTokens(internal.getPromptTokens())
            .completionTokens(internal.getCompletionTokens())
            .totalTokens(internal.getTotalTokens())
            .reasoningTokens(internal.getReasoningTokens())
            .cacheHitTokens(internal.getCacheHitTokens())
            .cacheCreationTokens(internal.getCacheCreationTokens())
            .cacheReadTokens(internal.getCacheReadTokens())
            .promptTokensDetails(cacheReadForDetails > 0
                    ? OpenAiUsage.PromptTokensDetails.builder().cachedTokens(cacheReadForDetails).build()
                    : null)
            .build();
    }
}
