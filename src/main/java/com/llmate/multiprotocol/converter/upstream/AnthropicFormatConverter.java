package com.llmate.multiprotocol.converter.upstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmContent;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.LlmToolDefinition;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesRequest;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesResponse;
import com.llmate.multiprotocol.dto.anthropic.AnthropicStreamEvent;
import com.llmate.multiprotocol.dto.anthropic.AnthropicTool;
import com.llmate.multiprotocol.dto.anthropic.AnthropicUsage;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Anthropic 格式上游转换器
 * 集中管理内部标准模型与 Anthropic /v1/messages 格式之间的双向转换
 * 所有 Anthropic 兼容的 ProviderAdapter 共用此组件
 * 使用公共 dto/anthropic 包下的 DTO，而非 Provider 内部私有 DTO
 */
@Component
@Log4j2
public class AnthropicFormatConverter {

    private final ObjectMapper objectMapper;

    public AnthropicFormatConverter(ObjectMapper objectMapper) {
        // 注入 Spring 单例 ObjectMapper，不各自 new 一份
        this.objectMapper = objectMapper;
    }

    /**
     * 内部标准请求 → Anthropic 上游请求格式
     * 使用公共 AnthropicMessagesRequest DTO
     */
    public AnthropicMessagesRequest toAnthropicRequest(LlmChatRequest internalReq) {
        // 提取实际模型名（去掉 provider 前缀）
        String model = internalReq.getModel();
        if (model != null && model.contains("/")) {
            model = model.substring(model.indexOf("/") + 1);
        }

        // 转换消息：Anthropic 规则 system 必须在顶层字段，messages 中只允许 user/assistant
        // tool 角色消息转换为 user 消息 + tool_result 内容块
        List<AnthropicMessagesRequest.AnthropicMessage> messages = new ArrayList<>();
        List<String> systemTexts = new ArrayList<>();

        if (internalReq.getMessages() != null) {
            for (var msg : internalReq.getMessages()) {
                if ("system".equals(msg.getRole())) {
                    // 收集 system 内容到顶层字段（多条 system 拼接，避免互相覆盖）
                    String systemText = msg.getTextContent();
                    if (systemText != null && !systemText.isEmpty()) {
                        systemTexts.add(systemText);
                    }
                } else if ("user".equals(msg.getRole()) || "assistant".equals(msg.getRole())) {
                    AnthropicMessagesRequest.AnthropicMessage anthropicMsg = new AnthropicMessagesRequest.AnthropicMessage();
                    anthropicMsg.setRole(msg.getRole());

                    // 处理 assistant 消息中的工具调用 → Anthropic tool_use 内容块
                    if ("assistant".equals(msg.getRole()) && msg.getToolCalls() != null && !msg.getToolCalls().isEmpty()) {
                        // Anthropic assistant 消息的 content 是内容块列表
                        List<Object> contentBlocks = new ArrayList<>();

                        // 先添加文本内容（如果有）
                        if (msg.getTextContent() != null && !msg.getTextContent().isEmpty()) {
                            contentBlocks.add(java.util.Map.of("type", "text", "text", msg.getTextContent()));
                        }

                        // 添加工具调用
                        // Anthropic 要求 tool_use.input 必须是 JSON 对象（dictionary），
                        // 不能是 JSON 字符串。arguments 在内部模型中存为 String，
                        // 需用 Jackson 反序列化为 Map 再传给上游。
                        for (var tc : msg.getToolCalls()) {
                            contentBlocks.add(java.util.Map.of(
                                    "type", "tool_use",
                                    "id", tc.getId() != null ? tc.getId() : "",
                                    "name", tc.getName() != null ? tc.getName() : "",
                                    "input", parseArgumentsToMap(tc.getArguments())
                            ));
                        }
                        anthropicMsg.setContent(contentBlocks);
                    } else {
                        // 普通文本消息
                        String text = msg.getTextContent();
                        if (text == null && msg.getContents() != null && !msg.getContents().isEmpty()) {
                            // fallback: 从 contents 列表中拼接所有文本块
                            text = msg.getContents().stream()
                                    .filter(c -> "text".equals(c.getType()))
                                    .map(LlmContent::getText)
                                    .filter(t -> t != null && !t.isEmpty())
                                    .reduce((a, b) -> a + "\n" + b)
                                    .orElse("");
                        }

                        // 多模态：contents 含图片时，content 必须是内容块列表（text 与 image 块交错，保序）。
                        // 纯文本仍走 String，保证零回归。
                        boolean hasImage = msg.getContents() != null && msg.getContents().stream()
                                .anyMatch(c -> "image".equals(c.getType())
                                        && c.getBase64Data() != null && !c.getBase64Data().isEmpty());
                        if (hasImage) {
                            List<Object> contentBlocks = new ArrayList<>();
                            if (text != null && !text.isEmpty()) {
                                contentBlocks.add(java.util.Map.of("type", "text", "text", text));
                            }
                            for (LlmContent c : msg.getContents()) {
                                if ("text".equals(c.getType()) && c.getText() != null && !c.getText().isEmpty()) {
                                    contentBlocks.add(java.util.Map.of("type", "text", "text", c.getText()));
                                } else if ("image".equals(c.getType()) && c.getBase64Data() != null && !c.getBase64Data().isEmpty()) {
                                    String mime = c.getMimeType() != null && !c.getMimeType().isEmpty() ? c.getMimeType() : "image/jpeg";
                                    contentBlocks.add(java.util.Map.of(
                                            "type", "image",
                                            "source", java.util.Map.of(
                                                    "type", "base64",
                                                    "media_type", mime,
                                                    "data", c.getBase64Data())));
                                }
                            }
                            anthropicMsg.setContent(contentBlocks);
                        } else {
                            anthropicMsg.setContent(text);
                        }
                    }

                    messages.add(anthropicMsg);
                } else if ("tool".equals(msg.getRole())) {
                    // 工具执行结果 → Anthropic tool_result 内容块（必须包在 user 消息里）
                    // Anthropic 强制要求：每个 tool_use 后面必须紧跟一个 tool_result，
                    // 否则 400："tool_use ids were found without tool_result blocks immediately after"
                    AnthropicMessagesRequest.AnthropicMessage toolResultMsg = new AnthropicMessagesRequest.AnthropicMessage();
                    toolResultMsg.setRole("user");
                    String toolOutput = msg.getTextContent();
                    // 空输出退化为空字符串，确保 tool_result.content 有值
                    if (toolOutput == null) {
                        toolOutput = "";
                    }
                    toolResultMsg.setContent(java.util.List.of(java.util.Map.of(
                            "type", "tool_result",
                            "tool_use_id", msg.getToolCallId() != null ? msg.getToolCallId() : "",
                            "content", toolOutput
                    )));
                    messages.add(toolResultMsg);
                }
            }
        }

        String systemContent = systemTexts.isEmpty() ? null : String.join("\n", systemTexts);

        // 从 extraParams 中拆出 Anthropic 显式字段。
        // Anthropic Messages API 严格校验顶层参数，不认识的字段直接 400。
        // Codex Desktop / OpenAI 客户端会在请求里塞大量 OpenAI 专有字段
        //（stream_options、parallel_tool_calls、include_reasoning 等），
        // 黑名单模式逐个加永远追不上→改用白名单：只透传 Anthropic 官方支持的额外字段。
        Object metadata = null;
        Object outputConfig = null;
        java.util.Map<String, Object> extraParams = null;
        if (internalReq.getExtraParams() != null && !internalReq.getExtraParams().isEmpty()) {
            // 显式字段拆出
            metadata = internalReq.getExtraParams().get("metadata");
            outputConfig = internalReq.getExtraParams().get("output_config");
            // 【模型兼容】output_config.effort 是 Sonnet 4.5 / Opus 4.5 专属字段，
            // Haiku 4.5 及更早模型不识别 → 直接透传会被上游 400
            // "output_config.effort: Extra inputs are not permitted"。
            if (outputConfig != null && !supportsOutputConfig(model)) {
                log.debug("[AnthropicFormatConverter] 模型 {} 不支持 output_config，丢弃: {}",
                        model, outputConfig);
                outputConfig = null;
            }
            // 白名单：Anthropic 官方支持且未在上方单独建模的顶层参数
            for (var entry : internalReq.getExtraParams().entrySet()) {
                if (ANTHROPIC_EXTRA_ALLOWED.contains(entry.getKey())) {
                    if (extraParams == null) {
                        extraParams = new java.util.LinkedHashMap<>();
                    }
                    extraParams.put(entry.getKey(), entry.getValue());
                }
            }
        }

        return AnthropicMessagesRequest.builder()
                .model(model)
                // VP 上游使用 fastjson 并期望 system 为结构化内容块列表，而非纯文本 String
                .system(systemContent != null
                        ? java.util.List.of(java.util.Map.of("type", "text", "text", systemContent))
                        : null)
                .messages(messages)
                .maxTokens(internalReq.getMaxTokens() != null ? internalReq.getMaxTokens() : 4096)
                .temperature(internalReq.getTemperature())
                .stream(internalReq.getStream())
                // ===== 透传字段：跨协议零遗漏 =====
                .tools(toAnthropicTools(internalReq.getTools()))
                .toolChoice(normalizeToolChoice(internalReq.getToolChoice()))
                .thinking(normalizeThinking(internalReq.getThinking()))
                .topP(internalReq.getTopP())
                .topK(internalReq.getTopK())
                .stopSequences(internalReq.getStopSequences())
                .metadata(metadata)
                .outputConfig(outputConfig)
                .extraParams(extraParams)
                .build();
    }

    /**
     * Anthropic 上游响应 → 内部标准响应
     * 使用公共 AnthropicMessagesResponse DTO
     */
    public LlmChatResponse toInternalResponse(AnthropicMessagesResponse anthropicResp) {
        LlmChatResponse resp = new LlmChatResponse();
        resp.setId(anthropicResp.getId());
        resp.setModel(anthropicResp.getModel());

        // 提取文本内容和工具调用
        StringBuilder textContent = new StringBuilder();
        List<LlmToolCall> toolCalls = new ArrayList<>();

        if (anthropicResp.getContent() != null) {
            for (var block : anthropicResp.getContent()) {
                if ("text".equals(block.getType()) && block.getText() != null) {
                    textContent.append(block.getText());
                } else if ("tool_use".equals(block.getType())) {
                    toolCalls.add(LlmToolCall.builder()
                            .id(block.getId())
                            .type("function")
                            .name(block.getName())
                            .arguments(block.getInput() instanceof String
                                    ? (String) block.getInput()
                                    : convertInputToString(block.getInput()))
                            .build());
                }
            }
        }

        LlmChatResponse.Choice choice = new LlmChatResponse.Choice();
        choice.setIndex(0);
        LlmChatResponse.Message msg = new LlmChatResponse.Message();
        msg.setRole("assistant");
        msg.setContent(textContent.toString());
        if (!toolCalls.isEmpty()) {
            msg.setToolCalls(toolCalls);
        }
        choice.setMessage(msg);

        // 根据 stop_reason 映射 finish_reason
        String stopReason = anthropicResp.getStopReason();
        choice.setFinishReason(mapStopReason(stopReason));

        resp.setChoices(List.of(choice));

        // Usage
        if (anthropicResp.getUsage() != null) {
            AnthropicUsage antUsage = anthropicResp.getUsage();
            // Anthropic 的 input_tokens 不含缓存 tokens，缓存创建/读取是独立字段；
            // 内部模型按 OpenAI 口径（prompt_tokens 含全部输入），总输入 = input + cache_creation + cache_read，
            // 否则总输入少算，且「新输入 vs 历史缓存」拆分计费缺输入源。
            int cacheCreation = antUsage.getCacheCreationInputTokens() != null ? antUsage.getCacheCreationInputTokens() : 0;
            int cacheRead = antUsage.getCacheReadInputTokens() != null ? antUsage.getCacheReadInputTokens() : 0;
            int totalInput = antUsage.getInputTokens() + cacheCreation + cacheRead;
            LlmChatResponse.Usage usage = new LlmChatResponse.Usage();
            usage.setPromptTokens(totalInput);
            usage.setCompletionTokens(antUsage.getOutputTokens());
            usage.setTotalTokens(totalInput + antUsage.getOutputTokens());
            usage.setCacheCreationTokens(cacheCreation);
            usage.setCacheReadTokens(cacheRead);
            resp.setUsage(usage);
        }

        return resp;
    }

    /**
     * Anthropic 上游流式事件 → 内部标准流式块
     * 使用公共 AnthropicStreamEvent DTO
     */
    public LlmStreamChunk toInternalStreamChunk(AnthropicStreamEvent event) {
        LlmStreamChunk chunk = new LlmStreamChunk();

        switch (event.getType()) {
            case "content_block_delta":
                if (event.getDelta() != null) {
                    // 文本增量
                    if (event.getDelta().getText() != null) {
                        chunk.setDeltaContent(event.getDelta().getText());
                    } else if (event.getDelta().getPartialJson() != null) {
                        // tool_use 增量（input_json_delta）
                        chunk.setToolCallArgumentsDelta(event.getDelta().getPartialJson());
                        // input_json_delta 事件只带 partial_json 和块 index，不带 name/id；
                        // 必须透传 index，下游协议转换器才能把 arguments 增量关联到
                        // content_block_start 时登记的 tool call，否则参数会被整体丢弃
                        chunk.setToolCallIndex(event.getIndex());
                    } else if (event.getDelta().getThinking() != null) {
                        // thinking 增量（thinking_delta），透传到 deltaContent
                        chunk.setDeltaContent(event.getDelta().getThinking());
                    } else {
                        chunk.setDeltaContent("");
                    }
                } else {
                    chunk.setDeltaContent("");
                }
                break;

            case "message_start":
                // 消息开始，设置 isFirstChunk 标记
                chunk.setFirstChunk(true);
                chunk.setDeltaContent("");
                // 提取 id 和 model
                if (event.getMessage() != null) {
                    chunk.setId(event.getMessage().getId());
                    chunk.setModel(event.getMessage().getModel());
                    // 提取 message_start 的输入 tokens（Anthropic 官方 message.usage.input_tokens），
                    // 供外部 message_start 事件回填，客户端据此统计输入用量
                    AnthropicUsage startUsage = event.getMessage().getUsage();
                    if (startUsage != null) {
                        int cacheCreation = startUsage.getCacheCreationInputTokens() != null ? startUsage.getCacheCreationInputTokens() : 0;
                        int cacheRead = startUsage.getCacheReadInputTokens() != null ? startUsage.getCacheReadInputTokens() : 0;
                        int totalInput = startUsage.getInputTokens() + cacheCreation + cacheRead;
                        chunk.setUsage(LlmUsage.builder()
                                .promptTokens(totalInput)
                                .completionTokens(startUsage.getOutputTokens())
                                .totalTokens(totalInput + startUsage.getOutputTokens())
                                .cacheCreationTokens(cacheCreation)
                                .cacheReadTokens(cacheRead)
                                .build());
                    }
                }
                break;

            case "message_delta":
                // 消息结束标记
                chunk.setFinished(true);
                chunk.setDeltaContent("");
                // 提取 stop_reason（从 delta 字段）并映射为内部 finishReason
                if (event.getDelta() != null) {
                    String stopReason = event.getDelta().getStopReason();
                    if (stopReason != null) {
                        chunk.setFinishReason(mapStopReason(stopReason));
                    }
                }
                // 提取 usage（顶层字段，对齐 Anthropic 官方 SSE 格式）
                if (event.getUsage() != null) {
                    AnthropicUsage antUsage = event.getUsage();
                    int cacheCreation = antUsage.getCacheCreationInputTokens() != null ? antUsage.getCacheCreationInputTokens() : 0;
                    int cacheRead = antUsage.getCacheReadInputTokens() != null ? antUsage.getCacheReadInputTokens() : 0;
                    int totalInput = antUsage.getInputTokens() + cacheCreation + cacheRead;
                    chunk.setUsage(LlmUsage.builder()
                            .promptTokens(totalInput) // 总输入 = input + cache_creation + cache_read
                            .completionTokens(antUsage.getOutputTokens())
                            .totalTokens(totalInput + antUsage.getOutputTokens())
                            .cacheCreationTokens(cacheCreation)
                            .cacheReadTokens(cacheRead)
                            .build());
                }
                break;

            case "message_stop":
                // 消息完全结束
                chunk.setFinished(true);
                chunk.setDeltaContent("");
                break;

            case "content_block_start":
                // 检测 content_block 类型。tool_use 块需要记录 id/name/index
                // 供后续 content_block_delta 的 input_json_delta 关联
                if (event.getContentBlock() != null) {
                    String blockType = (String) event.getContentBlock().get("type");
                    if ("tool_use".equals(blockType)) {
                        chunk.setToolCallIndex(event.getIndex());
                        chunk.setToolCallId((String) event.getContentBlock().get("id"));
                        chunk.setToolCallName((String) event.getContentBlock().get("name"));
                    }
                }
                chunk.setDeltaContent("");
                break;

            case "content_block_stop":
            case "ping":
                // 这些事件不需要产生文本增量
                chunk.setDeltaContent("");
                break;

            default:
                log.debug("[AnthropicFormatConverter] 未处理的流式事件类型: {}", event.getType());
                chunk.setDeltaContent("");
                break;
        }

        return chunk;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 规范化 tool_choice 为 Anthropic 格式。
     *
     * <p>不同协议对 tool_choice 的表示不同：
     * <ul>
     *   <li>OpenAI Chat Completions / Responses：字符串 {@code "auto"|"none"|"required"}
     *       或对象 {@code {"type":"function","function":{"name":"..."}}}</li>
     *   <li>Anthropic Messages：对象 {@code {"type":"auto"|"any"|"tool","name":"..."}}</li>
     * </ul>
     *
     * <p>直接透传会导致字符串 {@code "auto"} 被序列化为 JSON 字符串而非对象，
     * Anthropic 上游校验拒绝：{@code tool_choice: Input should be a valid dictionary or object}。
     *
     * @param toolChoice 内部格式的 tool_choice（可能来自任意入口协议）
     * @return Anthropic 格式的 tool_choice，无法识别则返回 null
     */
    private Object normalizeToolChoice(Object toolChoice) {
        if (toolChoice == null) {
            return null;
        }

        // 字符串格式："auto" | "none" | "required" → 转为 Anthropic 对象格式
        if (toolChoice instanceof String s) {
            return switch (s) {
                case "auto"     -> java.util.Map.of("type", "auto");
                case "none"     -> java.util.Map.of("type", "none");
                case "required" -> java.util.Map.of("type", "any");
                default -> {
                    log.debug("[AnthropicFormatConverter] 未知 tool_choice 字符串值: {}，使用 auto", s);
                    yield java.util.Map.of("type", "auto");
                }
            };
        }

        // 对象格式：检查是否为 Responses 格式 {"type":"function","function":{"name":"..."}}
        if (toolChoice instanceof java.util.Map<?, ?> map) {
            if ("function".equals(map.get("type")) && map.get("function") instanceof java.util.Map<?, ?> funcMap) {
                java.util.Map<String, Object> result = new java.util.LinkedHashMap<>();
                result.put("type", "tool");
                result.put("name", funcMap.get("name"));
                return result;
            }
            // 已经是 Anthropic 格式，直接透传
            return toolChoice;
        }

        log.debug("[AnthropicFormatConverter] 无法识别的 tool_choice 类型: {}，丢弃",
                toolChoice.getClass().getSimpleName());
        return null;
    }

    /**
     * 规范化 thinking/推理配置为 Anthropic 格式。
     *
     * <p>不同协议对推理配置的表示不同：
     * <ul>
     *   <li>OpenAI Responses：{@code {"reasoning": {"effort": "low|medium|high"}}}
     *       或 {@code {"include_reasoning": true}}</li>
     *   <li>Anthropic Messages：{@code {"type": "enabled", "budget_tokens": N}}
     *       或 {@code {"type": "adaptive"}}</li>
     * </ul>
     *
     * <p>直接透传 OpenAI 格式会导致 Anthropic 上游 400：
     * {@code thinking.type: Field required}。
     */
    private Object normalizeThinking(Object thinking) {
        if (thinking == null) {
            return null;
        }

        // 已经是 Anthropic 格式（包含 "type" 键），直接透传
        if (thinking instanceof java.util.Map<?, ?> map) {
            if (map.containsKey("type")) {
                return thinking;
            }
            // OpenAI Responses 格式：{effort: "low|medium|high"}
            if (map.containsKey("effort")) {
                Object effortObj = map.get("effort");
                String effort = effortObj instanceof String ? (String) effortObj : "medium";
                int budgetTokens = switch (effort) {
                    case "low" -> 1024;
                    case "high" -> 16384;
                    default -> 4096; // medium
                };
                return java.util.Map.of("type", "enabled", "budget_tokens", budgetTokens);
            }
            // 未知格式，尝试透传（可能工作也可能不工作）
            log.debug("[AnthropicFormatConverter] thinking Map 格式未知: {}", map.keySet());
        }

        // 未知类型，返回 null（不设 thinking）
        log.debug("[AnthropicFormatConverter] 无法识别的 thinking 类型: {}，丢弃",
                thinking.getClass().getSimpleName());
        return null;
    }

    /**
     * 内部标准工具定义 → Anthropic 工具定义
     * LlmToolDefinition{name, description, parameters} → AnthropicTool{name, description, input_schema}
     */
    /**
     * Anthropic Messages API 支持的额外顶层参数（白名单）。
     * 入口协议（OpenAI/Responses）会带大量 Anthropic 不认识的字段
     * （stream_options、parallel_tool_calls、include_reasoning 等），
     * 不在白名单内的 direct pass-through 会导致上游 400："Extra inputs are not permitted"。
     */
    private static final java.util.Set<String> ANTHROPIC_EXTRA_ALLOWED = java.util.Set.of();

    private static final Map<String, Object> EMPTY_INPUT_SCHEMA =
            Map.of("type", "object", "properties", Map.of());

    /**
     * Anthropic 官方支持 output_config（含 effort）的模型判断。
     * 仅 Sonnet 4.5 / Opus 4.5 系列支持（含日期后缀版本，如 claude-opus-4-5-20251101）；
     * Haiku 4.5 及更早模型不识别该字段，透传会被上游 400 "Extra inputs are not permitted"。
     */
    private static boolean supportsOutputConfig(String model) {
        return model != null && model.matches(".*(sonnet|opus)-4[-.]5.*");
    }

    private List<AnthropicTool> toAnthropicTools(List<LlmToolDefinition> tools) {
        if (tools == null || tools.isEmpty()) {
            return null;
        }
        List<AnthropicTool> result = new ArrayList<>();
        for (LlmToolDefinition tool : tools) {
            // Anthropic 要求每把工具必须有 input_schema，缺失则 400：
            // "tools.N.custom.input_schema: Field required"
            // Codex Desktop 的工具可能不定义 parameters → 给默认空 schema
            Map<String, Object> inputSchema = tool.getParameters();
            if (inputSchema == null || inputSchema.isEmpty()) {
                inputSchema = EMPTY_INPUT_SCHEMA;
            }
            result.add(AnthropicTool.builder()
                    .name(tool.getName())
                    .description(tool.getDescription())
                    .inputSchema(inputSchema)
                    .build());
        }
        return result;
    }

    /**
     * Anthropic stop_reason → 内部 finish_reason 映射
     */
    private String mapStopReason(String stopReason) {
        if (stopReason == null) return "stop";
        return switch (stopReason) {
            case "end_turn" -> "stop";
            case "tool_use" -> "tool_calls";
            case "max_tokens" -> "length";
            case "stop_sequence" -> "stop";
            default -> "stop";
        };
    }

    /**
     * 将工具调用 arguments JSON 字符串解析为 Map。
     * Anthropic 要求 tool_use.input 必须是 JSON 对象（dictionary），
     * 不能是 JSON 字符串 —— 传字符串会被 400 拒绝："Input should be a valid dictionary"。
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseArgumentsToMap(String arguments) {
        if (arguments == null || arguments.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(arguments, Map.class);
        } catch (Exception e) {
            log.warn("[AnthropicFormatConverter] 工具调用参数 JSON 解析失败，回退为空对象: args={}", arguments, e);
            return Map.of();
        }
    }

    /**
     * 将工具调用 input 对象转为 JSON 字符串
     */
    private String convertInputToString(Object input) {
        if (input == null) return "{}";
        if (input instanceof String) return (String) input;
        try {
            return objectMapper.writeValueAsString(input);
        } catch (Exception e) {
            log.warn("[AnthropicFormatConverter] 工具调用参数序列化失败: {}", e.getMessage());
            return "{}";
        }
    }
}
