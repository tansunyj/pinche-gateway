package com.llmate.multiprotocol.converter;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.converter.support.MessageConverter;
import com.llmate.multiprotocol.converter.support.StreamingConverter;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmToolDefinition;
import com.llmate.multiprotocol.dto.openai.*;
import com.llmate.multiprotocol.mapping.ModelMappingResolver;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * OpenAI 协议双向转换器
 * 负责 OpenAI 规范 DTO 与系统内部标准 DTO 的高并发非阻塞互转
 */
@Component
public class OpenAiProtocolConverter implements ProtocolConverter<OpenAiChatRequest, OpenAiChatResponse> {

    private final MessageConverter messageConverter;
    private final StreamingConverter streamingConverter;
    private final ModelMappingResolver mappingResolver;

    public OpenAiProtocolConverter(MessageConverter messageConverter,
                                    StreamingConverter streamingConverter,
                                    ModelMappingResolver mappingResolver) {
        this.messageConverter = messageConverter;
        this.streamingConverter = streamingConverter;
        this.mappingResolver = mappingResolver;
    }

    @Override
    public ProtocolType getProtocolType() {
        return ProtocolType.OPENAI_CHAT_COMPLETIONS;
    }

    @Override
    public Mono<LlmChatRequest> toInternalRequest(OpenAiChatRequest externalRequest) {
        if (externalRequest == null) {
            return Mono.empty();
        }

        // 解析模型映射：外部模型名 -> 内部路由标识
        // 注意：这里只做模型名映射，不做 provider 路由前缀添加
        // provider 前缀由 ModelMappingResolver 在配置中定义
        String externalModel = externalRequest.getModel();
        String internalModel = mappingResolver.resolve(externalModel, ProtocolType.OPENAI_CHAT_COMPLETIONS);

        // 兜底透传：OpenAI 显式字段（stream_options）与未建模字段合并进 extraParams，保证零遗漏
        java.util.Map<String, Object> extraParams = new java.util.LinkedHashMap<>();
        if (externalRequest.getExtraParams() != null) {
            extraParams.putAll(externalRequest.getExtraParams());
        }
        if (externalRequest.getStreamOptions() != null) {
            extraParams.put("stream_options", externalRequest.getStreamOptions());
        }

        // 创建内部标准请求体基础骨架
        LlmChatRequest internalReq = LlmChatRequest.builder()
                .model(internalModel)
                .temperature(externalRequest.getTemperature())
                .maxTokens(externalRequest.getMaxTokens())
                .stream(externalRequest.getStream())
                // ===== 透传字段：跨协议零遗漏 =====
                .tools(toInternalTools(externalRequest.getTools()))
                .toolChoice(externalRequest.getToolChoice())
                .thinking(externalRequest.getExtraParams() != null
                        ? externalRequest.getExtraParams().get("reasoning_effort") : null)
                .topP(externalRequest.getTopP())
                .topK(externalRequest.getTopK())
                .stopSequences(externalRequest.getStop())
                .extraParams(extraParams.isEmpty() ? null : extraParams)
                .build();

        // 核心修复：调用异步非阻塞的 MessageConverter 处理可能带有网络图片 URL 的消息体转换
        return messageConverter.openAiToInternal(externalRequest.getMessages())
                .map(internalMessages -> {
                    internalReq.setMessages(internalMessages);
                    return internalReq;
                });
    }

    /**
     * OpenAI 工具定义 → 内部标准工具定义
     * OpenAI: [{type: function, function: {name, description, parameters}}] → LlmToolDefinition{name, description, parameters}
     */
    @SuppressWarnings("unchecked")
    private List<LlmToolDefinition> toInternalTools(List<Object> tools) {
        if (tools == null || tools.isEmpty()) {
            return null;
        }
        List<LlmToolDefinition> result = new java.util.ArrayList<>();
        for (Object toolObj : tools) {
            if (toolObj instanceof java.util.Map<?, ?> toolMap) {
                Object function = toolMap.get("function");
                if (function instanceof java.util.Map<?, ?> func) {
                    Object name = func.get("name");
                    Object description = func.get("description");
                    Object parameters = func.get("parameters");
                    result.add(LlmToolDefinition.builder()
                            .name(name instanceof String ? (String) name : null)
                            .description(description instanceof String ? (String) description : null)
                            .parameters(parameters instanceof Map ? (Map<String, Object>) parameters : null)
                            .build());
                }
            }
        }
        return result;
    }

    @Override
    public OpenAiChatResponse toExternalResponse(LlmChatResponse internalResponse) {
        if (internalResponse == null) {
            return null;
        }

        // 将系统内部标准 Choice 转化为符合 OpenAI 格式的 Choices
        List<OpenAiChatResponse.Choice> openAiChoices = null;
        if (internalResponse.getChoices() != null) {
            openAiChoices = internalResponse.getChoices().stream()
                    .map(internalChoice -> {
                        LlmChatResponse.Message internalMsg = internalChoice.getMessage();

                        // 构建响应消息，包含工具调用等扩展字段
                        OpenAiChatResponse.ResponseMessage.ResponseMessageBuilder msgBuilder =
                                OpenAiChatResponse.ResponseMessage.builder()
                                        .role(internalMsg != null ? internalMsg.getRole() : null)
                                        .content(internalMsg != null ? internalMsg.getContent() : null);

                        // 映射工具调用 (assistant 消息)
                        if (internalMsg != null && internalMsg.getToolCalls() != null && !internalMsg.getToolCalls().isEmpty()) {
                            msgBuilder.toolCalls(internalMsg.getToolCalls().stream()
                                    .map(tc -> OpenAiToolCall.builder()
                                            .id(tc.getId())
                                            .type(tc.getType())
                                            .function(OpenAiToolCall.OpenAiFunctionCall.builder()
                                                    .name(tc.getName())
                                                    .arguments(tc.getArguments())
                                                    .build())
                                            .build())
                                    .collect(Collectors.toList()));
                        }

                        // 映射工具结果回传 (tool 角色消息)
                        if (internalMsg != null && internalMsg.getToolCallId() != null) {
                            msgBuilder.toolCallId(internalMsg.getToolCallId());
                        }
                        if (internalMsg != null && internalMsg.getName() != null) {
                            msgBuilder.name(internalMsg.getName());
                        }

                        return OpenAiChatResponse.Choice.builder()
                                .index(internalChoice.getIndex())
                                .finishReason(internalChoice.getFinishReason() != null ? internalChoice.getFinishReason() : "stop")
                                .message(msgBuilder.build())
                                .build();
                    })
                    .collect(Collectors.toList());
        }

        // 组装 usage，处理 null 情况
        OpenAiUsage usage = null;
        if (internalResponse.getUsage() != null) {
            LlmChatResponse.Usage internalUsage = internalResponse.getUsage();
            OpenAiUsage.OpenAiUsageBuilder usageBuilder = OpenAiUsage.builder()
                    .promptTokens(internalUsage.getPromptTokens())
                    .completionTokens(internalUsage.getCompletionTokens())
                    .totalTokens(internalUsage.getTotalTokens());

            // 推理 tokens：仅在 >0 时返回，避免给普通模型补 0。
            // 同时透传 completion_tokens_details.reasoning_tokens（OpenAI 标准嵌套格式）
            if (internalUsage.getReasoningTokens() > 0) {
                usageBuilder.reasoningTokens(internalUsage.getReasoningTokens())
                        .completionTokensDetails(OpenAiUsage.CompletionTokensDetails.builder()
                                .reasoningTokens(internalUsage.getReasoningTokens())
                                .build());
            }
            // 缓存 tokens
            if (internalUsage.getCacheHitTokens() > 0) {
                usageBuilder.cacheHitTokens(internalUsage.getCacheHitTokens());
            }
            if (internalUsage.getCacheMissTokens() > 0) {
                usageBuilder.cacheMissTokens(internalUsage.getCacheMissTokens());
            }
            if (internalUsage.getCacheCreationTokens() > 0) {
                usageBuilder.cacheCreationTokens(internalUsage.getCacheCreationTokens());
            }
            if (internalUsage.getCacheReadTokens() > 0) {
                usageBuilder.cacheReadTokens(internalUsage.getCacheReadTokens());
            }
            // prompt_tokens_details.cached_tokens（DeepSeek 嵌套格式）
            // 优先用真正的 cachedTokens（OpenAiFormatConverter 解析的 prompt_tokens_details.cached_tokens），
            // 缺失时回退 cacheHitTokens（prompt_cache_hit_tokens），二者语义同为"缓存命中"。
            int cachedForDetails = internalUsage.getCachedTokens() > 0
                    ? internalUsage.getCachedTokens() : internalUsage.getCacheHitTokens();
            if (cachedForDetails > 0) {
                usageBuilder.promptTokensDetails(OpenAiUsage.PromptTokensDetails.builder()
                        .cachedTokens(cachedForDetails)
                        .build());
            }

            usage = usageBuilder.build();
        }

        // 组装标准的 OpenAI 阻塞非流式 JSON 响应包
        return OpenAiChatResponse.builder()
                .id("chatcmpl-" + (internalResponse.getId() != null ? internalResponse.getId() : "unknown"))
                .object("chat.completion")
                .created(Instant.now().getEpochSecond())
                .model(internalResponse.getModel())
                .choices(openAiChoices)
                .usage(usage)
                .build();
    }

    @Override
    public Flux<ServerSentEvent<Object>> toExternalStream(Flux<LlmStreamChunk> internalStream, OpenAiChatRequest originalReq, String maskedModelName) {
        // 直接委派给底层的专业流式装配器组装标准的 OpenAI SSE 响应包
        return streamingConverter.toOpenAiStream(internalStream, originalReq, maskedModelName);
    }

    @Override
    public String extractModelName(OpenAiChatRequest externalRequest) {
        return externalRequest != null ? externalRequest.getModel() : null;
    }
}
