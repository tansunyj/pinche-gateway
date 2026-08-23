package com.llmate.multiprotocol.converter.upstream;

import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmContent;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.openai.*;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * OpenAI 格式上游转换器
 * 集中管理内部标准模型与 OpenAI 兼容格式之间的双向转换
 * 所有 OpenAI 兼容的 ProviderAdapter (DashScope, DeepSeek, Azure 等) 共用此组件
 * 消除各 Provider 中重复的 convertToOpenAiRequest / convertToInternalResponse / convertToInternalStreamChunk
 */
@Component
@Log4j2
public class OpenAiFormatConverter {

    /**
     * OpenAI chat/completions 认识的额外顶层参数（白名单）。
     * 入口协议（Anthropic）专有字段如 metadata / output_config / cache_control 会被 OpenAI 严格校验
     * 400 拒绝，故 extraParams 只透传此白名单内的字段（OpenAI 官方支持的可选参数）。
     */
    private static final java.util.Set<String> OPENAI_EXTRA_ALLOWED = java.util.Set.of(
            "presence_penalty", "frequency_penalty", "logit_bias", "user", "seed",
            "response_format", "n", "parallel_tool_calls", "stop", "stream_options",
            "reasoning_effort", "service_tier", "modalities", "audio"
    );

    /**
     * max_tokens / max_completion_tokens 下限。保持客户端传多少就透传多少（不做最小限制），
     * 仅对 0 或负值这种非法参数兜底为 1，避免上游参数校验报错。null 时由默认值兜底（见 toOpenAiRequest）。
     */
    private static final int MIN_MAX_TOKENS = 1;

    /**
     * 内部标准请求 → OpenAI 上游请求格式
     */
    public OpenAiChatRequest toOpenAiRequest(LlmChatRequest internalReq) {
        OpenAiChatRequest req = new OpenAiChatRequest();

        // 使用上游模型名（LlmGateway 已经处理过，直接透传）
        // 注意：不要在这里再去掉 provider 前缀，因为 LlmGateway 已经处理过了
        req.setModel(internalReq.getModel());
        req.setTemperature(internalReq.getTemperature());
        // GPT-5.x 等新模型使用 max_completion_tokens 替代 max_tokens
        // 纯透传客户端值：null 时给默认 10240；仅 0/负值这类非法参数兜底为 1，不做其他限制。
        Integer maxTokens = internalReq.getMaxTokens();
        if (maxTokens == null) {
            maxTokens = 10240;
        } else if (maxTokens < MIN_MAX_TOKENS) {
            log.debug("[OpenAiFormatConverter] 客户端 max_tokens={} 过小，兜底为 {}", maxTokens, MIN_MAX_TOKENS);
            maxTokens = MIN_MAX_TOKENS;
        }
        req.setMaxCompletionTokens(maxTokens);
        req.setStream(internalReq.getStream());

        // ===== 透传字段：跨协议零遗漏 =====
        req.setTopP(internalReq.getTopP());
        req.setTopK(internalReq.getTopK());
        req.setStop(internalReq.getStopSequences());
        // tool_choice 必须与 tools 一起出现；有 tools 没 tool_choice 上游会补默认值，
        // 但有 tool_choice 没 tools 上游直接 400（OpenAI/Azure 均如此）。
        if (internalReq.getTools() != null && !internalReq.getTools().isEmpty()) {
            req.setToolChoice(internalReq.getToolChoice());
        }
        // tools: LlmToolDefinition → OpenAI {type: function, function: {name, description, parameters}}
        if (internalReq.getTools() != null && !internalReq.getTools().isEmpty()) {
            req.setTools(internalReq.getTools().stream()
                    .map(tool -> java.util.Map.of(
                            "type", "function",
                            "function", java.util.Map.of(
                                    "name", tool.getName() != null ? tool.getName() : "",
                                    "description", tool.getDescription() != null ? tool.getDescription() : "",
                                    "parameters", tool.getParameters() != null ? tool.getParameters() : java.util.Map.of())))
                    .collect(Collectors.toList()));
        }
        // 未建模字段兜底透传。
        // OpenAI chat/completions 同样严格校验未知顶层参数（Anthropic 专有字段 metadata / output_config /
        // cache_control 会被 400 拒绝），只透传 OpenAI 认识的字段，其余丢弃。
        if (internalReq.getExtraParams() != null && !internalReq.getExtraParams().isEmpty()) {
            java.util.Map<String, Object> openAiExtra = new java.util.LinkedHashMap<>();
            for (var entry : internalReq.getExtraParams().entrySet()) {
                if (OPENAI_EXTRA_ALLOWED.contains(entry.getKey())) {
                    openAiExtra.put(entry.getKey(), entry.getValue());
                }
            }
            if (!openAiExtra.isEmpty()) {
                req.setExtraParams(openAiExtra);
            }
        }
        // thinking（Anthropic adaptive / Responses reasoning）→ OpenAI reasoning_effort，避免跨协议丢失
        // 但当 tools 存在时跳过：Qwen/DashScope 等模型在 thinking 模式下会陷入
        // "思考工具调用" 循环而从不实际发出 tool_calls（reasoning_content 占满输出）。
        boolean hasTools = internalReq.getTools() != null && !internalReq.getTools().isEmpty();
        if (internalReq.getThinking() != null && !hasTools) {
            if (req.getExtraParams() == null) {
                req.setExtraParams(new java.util.LinkedHashMap<>());
            }
            if (!req.getExtraParams().containsKey("reasoning_effort")) {
                Object effort = null;
                if (internalReq.getThinking() instanceof java.util.Map<?, ?> map) {
                    Object e = map.get("effort");
                    if (e instanceof String) {
                        effort = e;
                    }
                }
                if (effort == null) {
                    effort = "high"; // adaptive 思考映射为高推理强度（尽力而为）
                }
                req.getExtraParams().put("reasoning_effort", effort);
            }
        }

        // 转换消息列表
        if (internalReq.getMessages() != null) {
            req.setMessages(internalReq.getMessages().stream()
                    .map(this::toOpenAiMessage)
                    .collect(Collectors.toList()));
        }

        return req;
    }

    /**
     * OpenAI 上游响应 → 内部标准响应
     */
    public LlmChatResponse toInternalResponse(OpenAiChatResponse openAiResp) {
        LlmChatResponse resp = new LlmChatResponse();
        resp.setId(openAiResp.getId());
        resp.setModel(openAiResp.getModel());

        if (openAiResp.getChoices() != null) {
            resp.setChoices(openAiResp.getChoices().stream()
                    .map(this::toInternalChoice)
                    .collect(Collectors.toList()));
        }

        if (openAiResp.getUsage() != null) {
            OpenAiUsage upstreamUsage = openAiResp.getUsage();
            LlmChatResponse.Usage usage = new LlmChatResponse.Usage();
            // 上游 usage 字段可能为 null（如协议错配时上游回残缺 {"usage":{}}），
            // 直接传给原始 int 字段会拆箱 NPE → 统一 null 兜底（与 Vertex/Anthropic 转换器对齐）
            usage.setPromptTokens(upstreamUsage.getPromptTokens() != null ? upstreamUsage.getPromptTokens() : 0);
            usage.setCompletionTokens(upstreamUsage.getCompletionTokens() != null ? upstreamUsage.getCompletionTokens() : 0);
            usage.setTotalTokens(upstreamUsage.getTotalTokens() != null ? upstreamUsage.getTotalTokens() : 0);
            // 推理/缓存 tokens 透传（reasoning_tokens / completion_tokens_details.reasoning_tokens 等）
            usage.setReasoningTokens(upstreamUsage.getReasoningTokens() != null ? upstreamUsage.getReasoningTokens() : 0);
            usage.setCacheHitTokens(upstreamUsage.getCacheHitTokens() != null ? upstreamUsage.getCacheHitTokens() : 0);
            usage.setCacheMissTokens(upstreamUsage.getCacheMissTokens() != null ? upstreamUsage.getCacheMissTokens() : 0);
            usage.setCacheCreationTokens(upstreamUsage.getCacheCreationTokens() != null ? upstreamUsage.getCacheCreationTokens() : 0);
            usage.setCacheReadTokens(upstreamUsage.getCacheReadTokens() != null ? upstreamUsage.getCacheReadTokens() : 0);
            // OpenAI/Azure 缓存命中走 prompt_tokens_details.cached_tokens（与流式 LlmUsage.cachedTokens 对齐）
            usage.setCachedTokens(upstreamUsage.getPromptTokensDetails() != null && upstreamUsage.getPromptTokensDetails().getCachedTokens() != null
                    ? upstreamUsage.getPromptTokensDetails().getCachedTokens() : 0);
            resp.setUsage(usage);
        }

        return resp;
    }

    /**
     * OpenAI 上游流式块 → 内部标准流式块
     * 以 AzureProviderAdapter 的实现为基准（最完整，包含 reasoningContent 和 toolCalls 处理）
     */
    public LlmStreamChunk toInternalStreamChunk(OpenAiStreamChunk chunk) {
        LlmStreamChunk internalChunk = new LlmStreamChunk();
        internalChunk.setId(chunk.getId());
        internalChunk.setModel(chunk.getModel());

        if (chunk.getChoices() != null && !chunk.getChoices().isEmpty()) {
            var choice = chunk.getChoices().get(0);
            if (choice != null && choice.getDelta() != null) {
                OpenAiDelta delta = choice.getDelta();

                // 优先使用 reasoningContent (DeepSeek/o1 推理内容)，否则使用 content
                String content = delta.getReasoningContent() != null
                        ? delta.getReasoningContent()
                        : delta.getContent();
                internalChunk.setDeltaContent(content != null ? content : "");

                // 处理工具调用增量
                if (delta.getToolCalls() != null && !delta.getToolCalls().isEmpty()) {
                    var toolCall = delta.getToolCalls().get(0);
                    internalChunk.setToolCallId(toolCall.getId());
                    internalChunk.setToolCallIndex(toolCall.getIndex());
                    if (toolCall.getFunction() != null) {
                        internalChunk.setToolCallName(toolCall.getFunction().getName());
                        internalChunk.setToolCallArgumentsDelta(toolCall.getFunction().getArguments());
                    }
                }

                // 映射 finish_reason：有值表示流结束，同时透传原始值
                if (choice.getFinishReason() != null) {
                    internalChunk.setFinished(true);
                    internalChunk.setFinishReason(choice.getFinishReason());
                }
            } else {
                internalChunk.setDeltaContent("");
                // choice 存在但 delta 为空时，也可能携带 finish_reason
                if (choice != null && choice.getFinishReason() != null) {
                    internalChunk.setFinished(true);
                    internalChunk.setFinishReason(choice.getFinishReason());
                }
            }
        } else {
            internalChunk.setDeltaContent("");
        }

        // 处理 usage（最后一个 chunk 可能包含 token 统计）
        if (chunk.getUsage() != null) {
            OpenAiUsage usage = chunk.getUsage();
            internalChunk.setUsage(LlmUsage.builder()
                    // 与 toInternalResponse 同：usage 字段可能为 null，拆箱 NPE 兜底
                    .promptTokens(usage.getPromptTokens() != null ? usage.getPromptTokens() : 0)
                    .completionTokens(usage.getCompletionTokens() != null ? usage.getCompletionTokens() : 0)
                    .totalTokens(usage.getTotalTokens() != null ? usage.getTotalTokens() : 0)
                    .reasoningTokens(usage.getReasoningTokens() != null ? usage.getReasoningTokens() : 0)
                    .cacheHitTokens(usage.getCacheHitTokens() != null ? usage.getCacheHitTokens() : 0)
                    .cacheMissTokens(usage.getCacheMissTokens() != null ? usage.getCacheMissTokens() : 0)
                    .cacheCreationTokens(usage.getCacheCreationTokens() != null ? usage.getCacheCreationTokens() : 0)
                    .cacheReadTokens(usage.getCacheReadTokens() != null ? usage.getCacheReadTokens() : 0)
                    .cachedTokens(usage.getPromptTokensDetails() != null && usage.getPromptTokensDetails().getCachedTokens() != null
                            ? usage.getPromptTokensDetails().getCachedTokens() : 0)
                    .build());
        }

        return internalChunk;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 内部消息 → OpenAI 消息格式
     */
    private OpenAiMessage toOpenAiMessage(LlmMessage m) {
        OpenAiMessage msg = new OpenAiMessage();
        msg.setRole(m.getRole());

        // 优先使用 textContent，如果没有则从 contents 列表中提取文本（拼接所有文本块）
        String textContent = m.getTextContent();
        if (textContent == null && m.getContents() != null && !m.getContents().isEmpty()) {
            textContent = m.getContents().stream()
                    .filter(c -> "text".equals(c.getType()))
                    .map(LlmContent::getText)
                    .filter(t -> t != null && !t.isEmpty())
                    .reduce((a, b) -> a + "\n" + b)
                    .orElse(null);
        }

        // 多模态：contents 含图片时，content 必须是数组（text 与 image_url 交错，顺序有语义）。
        // 纯文本消息仍走原逻辑（String），保证零回归；assistant 工具消息无图不受影响。
        boolean hasImage = m.getContents() != null && m.getContents().stream()
                .anyMatch(c -> "image".equals(c.getType())
                        && c.getBase64Data() != null && !c.getBase64Data().isEmpty());
        if (hasImage) {
            List<ContentPart> parts = new java.util.ArrayList<>();
            // 先补主文本（textContent 通常为 null，此时靠下面的 contents 遍历保序）
            if (textContent != null && !textContent.isEmpty()) {
                parts.add(ContentPart.builder().type("text").text(textContent).build());
            }
            for (LlmContent c : m.getContents()) {
                if ("text".equals(c.getType()) && c.getText() != null && !c.getText().isEmpty()) {
                    parts.add(ContentPart.builder().type("text").text(c.getText()).build());
                } else if ("image".equals(c.getType()) && c.getBase64Data() != null && !c.getBase64Data().isEmpty()) {
                    String mime = c.getMimeType() != null && !c.getMimeType().isEmpty() ? c.getMimeType() : "image/jpeg";
                    parts.add(ContentPart.builder()
                            .type("image_url")
                            .imageUrl(ContentPart.ImageUrl.builder()
                                    .url("data:" + mime + ";base64," + c.getBase64Data())
                                    .build())
                            .build());
                }
            }
            msg.setContent(parts);
        } else {
            msg.setContent(textContent);
        }

        // ===== 工具调用/工具结果上行转换 =====
        // assistant 消息携带 tool_calls → OpenAI 标准的 tool_calls 数组
        if ("assistant".equals(m.getRole()) && m.getToolCalls() != null && !m.getToolCalls().isEmpty()) {
            msg.setToolCalls(m.getToolCalls().stream()
                    .map(tc -> OpenAiToolCall.builder()
                            .id(tc.getId())
                            .type(tc.getType())
                            .function(OpenAiToolCall.OpenAiFunctionCall.builder()
                                    .name(tc.getName())
                                    .arguments(tc.getArguments())
                                    .build())
                            .build())
                    .collect(Collectors.toList()));
            // OpenAI 规范：有 tool_calls 时 content 应为 null
            msg.setContent(null);
        }

        // tool 消息携带 tool_call_id 和 name → 关联到对应的 assistant tool_call
        if ("tool".equals(m.getRole())) {
            msg.setToolCallId(m.getToolCallId());
            msg.setName(m.getName());
        }

        return msg;
    }

    /**
     * OpenAI Choice → 内部 Choice
     */
    private LlmChatResponse.Choice toInternalChoice(OpenAiChatResponse.Choice c) {
        LlmChatResponse.Choice choice = new LlmChatResponse.Choice();
        choice.setIndex(c.getIndex());
        choice.setFinishReason(c.getFinishReason());

        // 转换消息
        LlmChatResponse.Message msg = new LlmChatResponse.Message();
        if (c.getMessage() != null) {
            msg.setRole(c.getMessage().getRole());
            msg.setContent(c.getMessage().getContent());

            // 映射工具调用
            if (c.getMessage().getToolCalls() != null) {
                msg.setToolCalls(c.getMessage().getToolCalls().stream()
                        .map(this::toInternalToolCall)
                        .collect(Collectors.toList()));
            }

            // 映射工具结果回传
            if (c.getMessage().getToolCallId() != null) {
                msg.setToolCallId(c.getMessage().getToolCallId());
            }
            if (c.getMessage().getName() != null) {
                msg.setName(c.getMessage().getName());
            }
        }
        choice.setMessage(msg);

        return choice;
    }

    /**
     * OpenAI ToolCall → 内部 LlmToolCall
     */
    private LlmToolCall toInternalToolCall(OpenAiToolCall tc) {
        return LlmToolCall.builder()
                .id(tc.getId())
                .type(tc.getType())
                .name(tc.getFunction() != null ? tc.getFunction().getName() : null)
                .arguments(tc.getFunction() != null ? tc.getFunction().getArguments() : null)
                .build();
    }
}
