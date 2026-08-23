package com.llmate.multiprotocol.converter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentRequest;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentResponse;
import com.llmate.multiprotocol.mapping.ModelMappingResolver;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Vertex AI 协议双向转换器
 * 负责 Google Vertex AI /v1beta/models/{model}:generateContent 协议与内部标准模型的双向转换
 * （Vertex AI 走 Gemini 原生协议，因此协议类型仍为 GOOGLE_GEMINI）
 *
 * Vertex/Gemini 协议关键差异:
 * - contents/parts 替代 messages
 * - systemInstruction 顶层字段替代 system role
 * - 角色: user/model 替代 user/assistant
 * - 流式/非流式是两个不同的 URL 端点
 * - SSE 格式: data: {json}\n\n (无 event 类型，类似 OpenAI 但无 event 行)
 */
@Component
@Log4j2
public class VertexProtocolConverter
        implements ProtocolConverter<VertexGenerateContentRequest, VertexGenerateContentResponse> {

    private final ModelMappingResolver mappingResolver;
    private final ObjectMapper objectMapper;

    public VertexProtocolConverter(ModelMappingResolver mappingResolver, ObjectMapper objectMapper) {
        this.mappingResolver = mappingResolver;
        // 注入 Spring 单例 ObjectMapper，不各自 new 一份
        this.objectMapper = objectMapper;
    }

    @Override
    public ProtocolType getProtocolType() {
        return ProtocolType.GOOGLE_GEMINI;
    }

    @Override
    public Mono<LlmChatRequest> toInternalRequest(VertexGenerateContentRequest externalRequest) {
        if (externalRequest == null) {
            log.warn("[VertexConverter] 外部请求为空");
            return Mono.empty();
        }

        // 解析模型映射
        String externalModel = externalRequest.getModel();
        String internalModel = mappingResolver.resolve(externalModel, ProtocolType.GOOGLE_GEMINI);
        log.info("[VertexConverter] 模型映射: {} -> {}", externalModel, internalModel);

        List<LlmMessage> messages = new ArrayList<>();

        // systemInstruction → system 消息（放在对话开头）
        VertexGenerateContentRequest.VertexContent sysInst = externalRequest.getSystemInstruction();
        if (sysInst != null && sysInst.getParts() != null) {
            String sysText = extractTextFromParts(sysInst.getParts());
            if (sysText != null && !sysText.isEmpty()) {
                messages.add(LlmMessage.system(sysText));
                log.debug("[VertexConverter] 提取systemInstruction: {}",
                        sysText.substring(0, Math.min(50, sysText.length())));
            }
        }

        // contents → user/assistant 消息
        List<VertexGenerateContentRequest.VertexContent> contents = normalizeContents(externalRequest.getContents());
        if (contents != null) {
            log.info("[VertexConverter] 转换 {} 条contents", contents.size());
            for (var gContent : contents) {
                String text = extractTextFromParts(gContent.getParts());
                String role = mapRoleFromVertex(gContent.getRole());

                if (text != null && !text.isEmpty()) {
                    messages.add(LlmMessage.builder()
                            .role(role)
                            .textContent(text)
                            .build());
                }
            }
        }

        // 提取 generationConfig
        Integer maxTokens = null;
        Double temperature = null;
        if (externalRequest.getGenerationConfig() != null) {
            maxTokens = externalRequest.getGenerationConfig().getMaxOutputTokens();
            temperature = externalRequest.getGenerationConfig().getTemperature();
        }

        LlmChatRequest internalReq = LlmChatRequest.builder()
                .model(internalModel)
                .messages(messages)
                .maxTokens(maxTokens)
                .temperature(temperature)
                .stream(false) // 非流式默认，流式由 Controller 根据端点区分
                .build();

        log.info("[VertexConverter] 内部请求构建完成: model={}, messages={}", internalModel, messages.size());
        return Mono.just(internalReq);
    }

    @Override
    public VertexGenerateContentResponse toExternalResponse(LlmChatResponse internalResponse) {
        if (internalResponse == null) {
            log.warn("[VertexConverter] 内部响应为空");
            return null;
        }

        // 构建 candidates
        List<VertexGenerateContentResponse.VertexCandidate> candidates = new ArrayList<>();

        if (internalResponse.getChoices() != null && !internalResponse.getChoices().isEmpty()) {
            LlmChatResponse.Choice firstChoice = internalResponse.getChoices().get(0);
            LlmChatResponse.Message msg = firstChoice.getMessage();

            List<VertexGenerateContentResponse.VertexPart> parts = new ArrayList<>();
            if (msg != null && msg.getContent() != null) {
                parts.add(VertexGenerateContentResponse.VertexPart.builder()
                        .text(msg.getContent())
                        .build());
            }

            VertexGenerateContentResponse.VertexContent content =
                    VertexGenerateContentResponse.VertexContent.builder()
                            .role("model")
                            .parts(parts)
                            .build();

            candidates.add(VertexGenerateContentResponse.VertexCandidate.builder()
                    .content(content)
                    .finishReason(mapToVertexFinishReason(firstChoice.getFinishReason()))
                    .index( ObjectUtils.isEmpty(firstChoice.getIndex()) ? firstChoice.getIndex() : 0)
                    .build());
        }

        // 构建 usageMetadata
        VertexGenerateContentResponse.VertexUsageMetadata usageMetadata = null;
        if (internalResponse.getUsage() != null) {
            usageMetadata = VertexGenerateContentResponse.VertexUsageMetadata.builder()
                    .promptTokenCount(internalResponse.getUsage().getPromptTokens())
                    .candidatesTokenCount(internalResponse.getUsage().getCompletionTokens())
                    .totalTokenCount(internalResponse.getUsage().getTotalTokens())
                    .build();
        }

        log.info("[VertexConverter] 外部响应构建完成: candidates={}", candidates.size());
        return VertexGenerateContentResponse.builder()
                .candidates(candidates)
                .usageMetadata(usageMetadata)
                .build();
    }

    @Override
    public Flux<ServerSentEvent<Object>> toExternalStream(Flux<LlmStreamChunk> internalStream,
            VertexGenerateContentRequest originalReq, String maskedModelName) {
        log.info("[VertexConverter] 开始流式转换: maskedModel={}", maskedModelName);

        // 累积完整文本，用于最后的合并输出
        StringBuilder fullText = new StringBuilder();
        AtomicReference<com.llmate.multiprotocol.dto.LlmUsage> finalUsageRef = new AtomicReference<>(null);

        // 直接映射每个内部 chunk 为 Vertex/Gemini SSE data 行
        Flux<ServerSentEvent<Object>> dataStream = internalStream.mapNotNull(chunk -> {
            // 收集 usage
            if (chunk.getUsage() != null) {
                finalUsageRef.set(chunk.getUsage());
            }

            if (chunk.isFinished()) {
                // 结束帧 — 不发 delta，由尾部事件处理
                log.debug("[VertexConverter] 收到finished标记，跳过delta输出");
                return null;
            }

            String deltaText = chunk.getDeltaContent() != null ? chunk.getDeltaContent() : "";
            fullText.append(deltaText);

            // 构建 Vertex/Gemini 格式的增量响应
            VertexGenerateContentResponse vertexDelta =
                    VertexGenerateContentResponse.builder()
                            .candidates(List.of(
                                    VertexGenerateContentResponse.VertexCandidate.builder()
                                            .content(VertexGenerateContentResponse.VertexContent.builder()
                                                    .role("model")
                                                    .parts(List.of(
                                                            VertexGenerateContentResponse.VertexPart.builder()
                                                                    .text(deltaText)
                                                                    .build()))
                                                    .build())
                                            .index(0)
                                            .build()))
                            .build();

            return ServerSentEvent.builder()
                    .data(vertexDelta)
                    .build();
        });

        // 尾部事件：合并所有文本 + usageMetadata，然后发送 [DONE]
        Flux<ServerSentEvent<Object>> epilogue = Flux.defer(() -> {
            String completedText = fullText.toString();
            log.info("[VertexConverter] 流结束，完整文本长度={}", completedText.length());

            // 构建 usageMetadata
            VertexGenerateContentResponse.VertexUsageMetadata usageMeta = null;
            if (finalUsageRef.get() != null) {
                var u = finalUsageRef.get();
                usageMeta = VertexGenerateContentResponse.VertexUsageMetadata.builder()
                        .promptTokenCount(u.getPromptTokens())
                        .candidatesTokenCount(u.getCompletionTokens())
                        .totalTokenCount(u.getTotalTokens())
                        .build();
            } else {
                usageMeta = VertexGenerateContentResponse.VertexUsageMetadata.builder()
                        .promptTokenCount(0)
                        .candidatesTokenCount(0)
                        .totalTokenCount(0)
                        .build();
            }

            // 最后一条带 usageMetadata 的完整响应
            VertexGenerateContentResponse finalChunk =
                    VertexGenerateContentResponse.builder()
                            .candidates(List.of(
                                    VertexGenerateContentResponse.VertexCandidate.builder()
                                            .content(VertexGenerateContentResponse.VertexContent.builder()
                                                    .role("model")
                                                    .parts(List.of(
                                                            VertexGenerateContentResponse.VertexPart.builder()
                                                                    .text(completedText)
                                                                    .build()))
                                                    .build())
                                            .finishReason("STOP")
                                            .index(0)
                                            .build()))
                            .usageMetadata(usageMeta)
                            .build();

            return Flux.just(
                    ServerSentEvent.builder().data(finalChunk).build(),
                    ServerSentEvent.builder().data("[DONE]").build());
        });

        return dataStream
                .concatWith(epilogue)
                .doOnComplete(() -> log.info("[VertexConverter] 流式转换完成"))
                // 上游流错误禁止在此吞成 SSE error（会丢失统一脱敏 + request_id），必须 Flux.error 传播到
                // Controller 的 onErrorResume 统一处理（GatewayErrorResponseBuilder.streamErrorEvents）
                .doOnError(e -> log.error("[VertexConverter] 流式转换错误", e));
    }

    @Override
    public String extractModelName(VertexGenerateContentRequest externalRequest) {
        return externalRequest != null ? externalRequest.getModel() : null;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 从 Vertex/Gemini parts 列表中提取纯文本
     */
    private String extractTextFromParts(List<VertexGenerateContentRequest.VertexPart> parts) {
        if (parts == null || parts.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (var part : parts) {
            if (part.getText() != null) {
                if (!sb.isEmpty()) {
                    sb.append("\n");
                }
                sb.append(part.getText());
            }
        }
        return sb.toString();
    }

    /**
     * Vertex/Gemini 角色 → 内部角色
     * "model" → "assistant", "user" → "user"
     */
    private String mapRoleFromVertex(String geminiRole) {
        if (geminiRole == null) return "user";
        return switch (geminiRole) {
            case "model" -> "assistant";
            case "user" -> "user";
            default -> "user";
        };
    }

    /**
     * 内部 finish_reason → Vertex/Gemini finishReason
     */
    private String mapToVertexFinishReason(String internalReason) {
        if (internalReason == null) return "STOP";
        return switch (internalReason) {
            case "stop" -> "STOP";
            case "length" -> "MAX_TOKENS";
            case "content_filter" -> "SAFETY";
            case "tool_calls" -> "STOP"; // Vertex/Gemini 没有 tool_calls 等价物
            default -> "STOP";
        };
    }

    /**
     * 规范化 contents 字段，支持单对象和数组两种格式
     * Jackson 将 Object 类型字段反序列化时:
     * - JSON 对象 → LinkedHashMap
     * - JSON 数组 → ArrayList<LinkedHashMap>
     * 需要手动转换为 VertexContent DTO
     */
    @SuppressWarnings("unchecked")
    private List<VertexGenerateContentRequest.VertexContent> normalizeContents(Object contents) {
        if (contents == null) {
            return Collections.emptyList();
        }
        if (contents instanceof List) {
            return ((List<?>) contents).stream()
                    .map(this::convertToVertexContent)
                    .filter(c -> c != null)
                    .collect(java.util.stream.Collectors.toList());
        }
        // 单个对象（LinkedHashMap 或已转换的 VertexContent）
        VertexGenerateContentRequest.VertexContent content = convertToVertexContent(contents);
        return content != null ? List.of(content) : Collections.emptyList();
    }

    /**
     * 将各种格式的对象转换为 VertexContent
     * 支持: VertexContent (透传), Map (Jackson convertValue)
     */
    private VertexGenerateContentRequest.VertexContent convertToVertexContent(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof VertexGenerateContentRequest.VertexContent) {
            return (VertexGenerateContentRequest.VertexContent) obj;
        }
        if (obj instanceof java.util.Map) {
            try {
                return objectMapper.convertValue(obj, VertexGenerateContentRequest.VertexContent.class);
            } catch (Exception e) {
                log.warn("[VertexConverter] 无法转换contents元素: {}", e.getMessage());
                return null;
            }
        }
        log.warn("[VertexConverter] 未知的contents元素类型: {}", obj.getClass().getName());
        return null;
    }
}
