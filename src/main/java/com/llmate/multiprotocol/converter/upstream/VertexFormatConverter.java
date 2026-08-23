package com.llmate.multiprotocol.converter.upstream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmContent;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.LlmToolDefinition;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmUsage;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentRequest;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentResponse;
import com.llmate.multiprotocol.converter.support.PollutionCleaner;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Vertex AI 格式上游转换器
 * 集中管理内部标准模型与 Google Vertex AI API 格式之间的双向转换
 *
 * Vertex AI 协议与 Gemini 原生协议基本一致，但有一些细微差异：
 * - 端点结构不同: publishers/google/models/{model}:generateContent
 * - 认证方式: GCP OAuth2 / Service Account (Bearer token)
 * - 需要 projectId 和 location 构建完整 URL
 *
 * 请求体格式与 Gemini 相同（contents/parts/systemInstruction/generationConfig）
 * 参考用户提供的实际请求示例：
 * {
 *   "contents": {
 *     "parts": [{"text": "一只飞鸟"}],
 *     "role": "user"
 *   },
 *   "generationConfig": {
 *     "maxOutputTokens": 10240,
 *     "temperature": 0.7,
 *     "topP": 1
 *   },
 *   "systemInstruction": {
 *     "parts": [{"text": "逼真,高清"}]
 *   }
 * }
 *
 * 注意：Vertex API 中 contents 可以是单个对象（单轮对话）或数组（多轮对话）
 */
@Component
@Log4j2
public class VertexFormatConverter {

    /**
     * Gemini 顶层请求体认识的额外字段（白名单）。
     * 入口协议（Anthropic/OpenAI）专有字段如 metadata / output_config / stream_options 等
     * 会被 Gemini 严格校验拒绝，故 extraParams 只透传此白名单内的字段。
     */
    private static final java.util.Set<String> GEMINI_EXTRA_ALLOWED = java.util.Set.of(
            "safetySettings", "cachedContent", "labels", "responseModalities"
    );

    /**
     * Gemini 3 对历史 functionCall 强制校验 thought_signature。
     * 经 Anthropic/OpenAI 协议往返的工具调用历史无法携带真实签名，
     * Google 官方文档允许对外部注入的 functionCall 使用此固定占位签名绕过校验。
     */
    private static final String DUMMY_THOUGHT_SIGNATURE = "context_engineering_is_the_way_to_go";

    /**
     * Gemini functionDeclarations.parameters 不认识的 JSON Schema 方言键。
     * Anthropic input_schema / OpenAI parameters 常携带这些键，需递归删除，否则 Gemini 400 拒绝
     * （运行时日志实测被拒：$schema / propertyNames / const / exclusiveMinimum）。
     * 注意：anyOf / oneOf 等组合键 Gemini 是接受的（日志中 const 出现在 any_of 内部被拒而非 anyOf 本身），
     * 故不能整段删除 anyOf/oneOf，否则会丢失工具入参结构。
     * 额外规则：所有 $ 前缀的键（$ref / $id / $anchor / $defs 等）都属于 JSON Schema 引用/方言机制，
     * Gemini 完全不支持，统一拦截。
     */
    private static final java.util.Set<String> GEMINI_SCHEMA_BLOCKED = java.util.Set.of(
            "definitions", "propertyNames", "const",
            "exclusiveMinimum", "exclusiveMaximum", "patternProperties", "unevaluatedProperties"
    );

    /**
     * maxOutputTokens 下限。保持客户端传多少就透传多少（不做最小限制），
     * 仅对 0 或负值这种非法参数兜底为 1，避免上游参数校验报错。null 时由 resolveMaxOutputTokens 给默认。
     */
    private static final int MIN_MAX_OUTPUT_TOKENS = 1;

    private final ObjectMapper objectMapper;

    public VertexFormatConverter(ObjectMapper objectMapper) {
        // 注入 Spring 单例 ObjectMapper（Spring Boot 默认已禁用 FAIL_ON_UNKNOWN_PROPERTIES），不各自 new 一份
        this.objectMapper = objectMapper;
    }

    /**
     * 将 LinkedHashMap 转换为 VertexContent 对象
     */
    private VertexGenerateContentRequest.VertexContent convertToVertexContent(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof VertexGenerateContentRequest.VertexContent) {
            return (VertexGenerateContentRequest.VertexContent) obj;
        }
        if (obj instanceof Map) {
            try {
                return objectMapper.convertValue(obj, VertexGenerateContentRequest.VertexContent.class);
            } catch (Exception e) {
                log.warn("[VertexFormatConverter] 无法转换 contents 对象: {}", e.getMessage());
                return null;
            }
        }
        return null;
    }

    /**
     * 内部标准请求 → Vertex AI 上游请求格式
     *
     * 关键处理：
     * 1. 分离 system 指令 → systemInstruction 顶层字段（多条 system 消息拼接，避免互相覆盖）
     * 2. user/assistant 消息 → contents 列表
     * 3. assistant 角色映射为 "model"
     * 4. 支持 contents 为单个对象（单轮）或数组（多轮）
     * 5. 透传 tools / toolChoice / thinking / topP / topK / stopSequences / 未建模字段
     */
    /**
     * 解析 Gemini maxOutputTokens：纯透传客户端值；null 时默认 4096；
     * 仅 0/负值这种非法参数兜底为 1，不做其他最小限制（客户端传多少就是多少）。
     */
    private Integer resolveMaxOutputTokens(Integer maxTokens) {
        if (maxTokens == null) {
            return 10240;
        }
        if (maxTokens < MIN_MAX_OUTPUT_TOKENS) {
            log.debug("[VertexFormatConverter] 客户端 max_tokens={} 过小，兜底为 {}", maxTokens, MIN_MAX_OUTPUT_TOKENS);
            return MIN_MAX_OUTPUT_TOKENS;
        }
        return maxTokens;
    }

    public VertexGenerateContentRequest toVertexRequest(LlmChatRequest internalReq) {
        // 提取实际模型名（去掉 provider 前缀）
        String model = internalReq.getModel();
        if (model != null && model.contains("/")) {
            model = model.substring(model.indexOf("/") + 1);
        }

        // 分离 system 指令和对话消息
        List<String> systemTexts = new ArrayList<>();
        List<VertexGenerateContentRequest.VertexContent> contents = new ArrayList<>();

        if (internalReq.getMessages() != null) {
            // 连续 tool 消息（同一 functionCall 回合的多个函数结果）必须合并进【同一个】user content。
            // Gemini 严格校验 "functionResponse parts 数量必须等于 functionCall parts 数量"；
            // 若每个 tool 消息各自生成独立 user content，则 2 个 functionCall 后跟 1 个 functionResponse
            // 的 user content 会被 400 拒绝（实测报错 "Please ensure that the number of function response parts
            // is equal to the number of function call parts of the function call turn."）。
            List<VertexGenerateContentRequest.VertexPart> pendingFuncResponses = new ArrayList<>();

            for (var msg : internalReq.getMessages()) {
                if ("system".equals(msg.getRole()) || "developer".equals(msg.getRole())) {
                    // 非 tool 消息前先冲刷已累积的 functionResponse，保持其在 contents 中的相对位置
                    flushFunctionResponses(pendingFuncResponses, contents);
                    // system/developer → 顶层 systemInstruction（多段拼接，保留所有 system 内容）
                    String text = extractText(msg);
                    if (text != null && !text.isEmpty()) {
                        systemTexts.add(text);
                    }
                } else if ("tool".equals(msg.getRole())) {
                    // tool 消息（函数执行结果）→ 累积到 pending，待连续 tool 结束或循环结束再合并
                    String toolName = msg.getName() != null ? msg.getName() : msg.getToolCallId();
                    String toolOutput = extractText(msg);
                    if (toolName != null) {
                        java.util.Map<String, Object> funcResponse = new java.util.LinkedHashMap<>();
                        funcResponse.put("name", toolName);
                        funcResponse.put("response", java.util.Map.of("result", toolOutput != null ? toolOutput : ""));
                        pendingFuncResponses.add(VertexGenerateContentRequest.VertexPart.builder()
                                .functionResponse(funcResponse)
                                .build());
                    } else {
                        log.warn("[VertexFormatConverter] tool 消息缺少函数名与 toolCallId，已丢弃工具结果");
                    }
                } else if ("assistant".equals(msg.getRole()) && msg.getToolCalls() != null && !msg.getToolCalls().isEmpty()) {
                    flushFunctionResponses(pendingFuncResponses, contents);
                    // assistant 消息中包含工具调用 → functionCall parts。
                    // Gemini 3 要求历史 functionCall 必须带 thought_signature：
                    // 有真实签名（从上游 Gemini 响应捕获）则原样附带；
                    // 无签名（经 Anthropic/OpenAI 协议往返后必然丢失）则用 Google 官方
                    // 文档给出的占位签名绕过校验，保持结构化 functionCall，
                    // 避免降级成文本后 functionResponse 落单导致 Gemini 400。
                    List<VertexGenerateContentRequest.VertexPart> parts = new ArrayList<>();
                    String text = extractText(msg);
                    if (text != null && !text.isEmpty()) {
                        parts.add(VertexGenerateContentRequest.VertexPart.builder().text(text).build());
                    }
                    for (var tc : msg.getToolCalls()) {
                        java.util.Map<String, Object> funcCall = new java.util.LinkedHashMap<>();
                        funcCall.put("name", tc.getName());
                        try {
                            funcCall.put("args", tc.getArguments() instanceof String
                                    ? objectMapper.readValue((String) tc.getArguments(), java.util.Map.class)
                                    : (tc.getArguments() != null ? tc.getArguments() : java.util.Map.of()));
                        } catch (Exception e) {
                            log.warn("[VertexFormatConverter] 工具调用参数解析失败: {}", e.getMessage());
                            funcCall.put("args", java.util.Map.of());
                        }
                        String thoughtSig = tc.getThoughtSignature() != null && !tc.getThoughtSignature().isEmpty()
                                ? tc.getThoughtSignature()
                                : DUMMY_THOUGHT_SIGNATURE;
                        parts.add(VertexGenerateContentRequest.VertexPart.builder()
                                .functionCall(funcCall)
                                .thoughtSignature(thoughtSig)
                                .build());
                    }
                    if (!parts.isEmpty()) {
                        contents.add(VertexGenerateContentRequest.VertexContent.builder()
                                .role("model")
                                .parts(parts)
                                .build());
                    }
                } else {
                    // 非 tool 消息前先冲刷已累积的 functionResponse
                    flushFunctionResponses(pendingFuncResponses, contents);
                    // user/assistant → contents
                    String role = mapRoleToVertex(msg.getRole());

                    // 多模态：contents 含图片时 parts 追加 inlineData{base64}（text 与图片保序）；
                    // 纯文本仍走原 extractText 单 part 逻辑，零回归。
                    boolean hasImage = msg.getContents() != null && msg.getContents().stream()
                            .anyMatch(c -> "image".equals(c.getType())
                                    && c.getBase64Data() != null && !c.getBase64Data().isEmpty());
                    if (hasImage) {
                        List<VertexGenerateContentRequest.VertexPart> parts = new ArrayList<>();
                        String textContent = msg.getTextContent();
                        if (textContent != null && !textContent.isEmpty()) {
                            parts.add(VertexGenerateContentRequest.VertexPart.builder().text(textContent).build());
                        }
                        for (LlmContent c : msg.getContents()) {
                            if ("text".equals(c.getType()) && c.getText() != null && !c.getText().isEmpty()) {
                                parts.add(VertexGenerateContentRequest.VertexPart.builder().text(c.getText()).build());
                            } else if ("image".equals(c.getType()) && c.getBase64Data() != null && !c.getBase64Data().isEmpty()) {
                                String mime = c.getMimeType() != null && !c.getMimeType().isEmpty() ? c.getMimeType() : "image/jpeg";
                                parts.add(VertexGenerateContentRequest.VertexPart.builder()
                                        .inlineData(VertexGenerateContentRequest.VertexInlineData.builder()
                                                .mimeType(mime)
                                                .data(c.getBase64Data())
                                                .build())
                                        .build());
                            }
                        }
                        if (!parts.isEmpty()) {
                            contents.add(VertexGenerateContentRequest.VertexContent.builder()
                                    .role(role)
                                    .parts(parts)
                                    .build());
                        }
                    } else {
                        String text = extractText(msg);
                        if (text != null && !text.isEmpty()) {
                            contents.add(VertexGenerateContentRequest.VertexContent.builder()
                                    .role(role)
                                    .parts(List.of(VertexGenerateContentRequest.VertexPart.builder()
                                            .text(text)
                                            .build()))
                                    .build());
                        }
                    }
                }
            }
            // 循环结束，冲刷末尾残留的 functionResponse
            flushFunctionResponses(pendingFuncResponses, contents);
        }

        VertexGenerateContentRequest.VertexContent systemInstruction = null;
        if (!systemTexts.isEmpty()) {
            systemInstruction = VertexGenerateContentRequest.VertexContent.builder()
                    .parts(List.of(VertexGenerateContentRequest.VertexPart.builder()
                            .text(String.join("\n", systemTexts))
                            .build()))
                    .build();
        }

        // 构建 generationConfig
        VertexGenerateContentRequest.VertexGenerationConfig.VertexGenerationConfigBuilder genConfigBuilder =
                VertexGenerateContentRequest.VertexGenerationConfig.builder()
                        .maxOutputTokens(resolveMaxOutputTokens(internalReq.getMaxTokens()))
                        .temperature(internalReq.getTemperature())
                        .topP(internalReq.getTopP() != null ? internalReq.getTopP() : 1.0)
                        .topK(internalReq.getTopK())
                        .stopSequences(internalReq.getStopSequences());

        // thinking → thinkingConfig
        VertexGenerateContentRequest.VertexThinkingConfig thinkingConfig = toVertexThinkingConfig(internalReq.getThinking());
        if (thinkingConfig != null) {
            genConfigBuilder.thinkingConfig(thinkingConfig);
        }

        VertexGenerateContentRequest.VertexGenerationConfig genConfig = genConfigBuilder.build();

        // 单轮对话时 contents 可以是单个对象（兼容用户提供的示例格式）
        Object contentsField = contents.size() == 1 ? contents.get(0) : contents;

        VertexGenerateContentRequest.VertexGenerateContentRequestBuilder builder =
                VertexGenerateContentRequest.builder()
                        .model(model)
                        .systemInstruction(systemInstruction)
                        .contents(contentsField)
                        .generationConfig(genConfig);

        // tools → functionDeclarations
        if (internalReq.getTools() != null && !internalReq.getTools().isEmpty()) {
            List<VertexGenerateContentRequest.VertexFunctionDeclaration> declarations = new ArrayList<>();
            for (var tool : internalReq.getTools()) {
                // 防御：跳过 name 为空的工具（上游 Gemini 要求 name 必填，
                // 否则报 REQUIRED_FIELD_MISSING）
                if (tool.getName() == null || tool.getName().isBlank()) {
                    log.debug("[VertexFormatConverter] 跳过无名称的工具定义: description={}",
                            tool.getDescription());
                    continue;
                }
                declarations.add(VertexGenerateContentRequest.VertexFunctionDeclaration.builder()
                        .name(tool.getName())
                        .description(tool.getDescription())
                        // Gemini 只接受 JSON Schema 子集，Anthropic input_schema / OpenAI parameters
                        // 携带的 $schema / propertyNames / const 等方言字段会被 Gemini 400 拒绝，需清洗
                        .parameters(sanitizeJsonSchema(tool.getParameters()))
                        .build());
            }
            if (!declarations.isEmpty()) {
                builder.tools(List.of(VertexGenerateContentRequest.VertexTool.builder()
                        .functionDeclarations(declarations)
                        .build()));
            }
        }

        // toolChoice → toolConfig.functionCallingConfig
        VertexGenerateContentRequest.VertexToolConfig toolConfig = toVertexToolConfig(internalReq.getToolChoice());
        if (toolConfig != null) {
            builder.toolConfig(toolConfig);
        }

        // 未建模字段兜底透传。
        // 注意：Gemini 严格校验字段，Anthropic 专有字段（metadata / output_config / cache_control）及
        // OpenAI 专有字段（stream_options / presence_penalty / frequency_penalty / response_format 等）
        // 会被 Gemini 400 拒绝，因此只透传 Gemini 认识的顶层字段白名单，其余丢弃（不遗漏到会报错的程度）。
        if (internalReq.getExtraParams() != null && !internalReq.getExtraParams().isEmpty()) {
            java.util.Map<String, Object> geminiExtra = new java.util.LinkedHashMap<>();
            for (var entry : internalReq.getExtraParams().entrySet()) {
                if (GEMINI_EXTRA_ALLOWED.contains(entry.getKey())) {
                    geminiExtra.put(entry.getKey(), entry.getValue());
                }
            }
            if (!geminiExtra.isEmpty()) {
                builder.extraParams(geminiExtra);
            }
        }

        return builder.build();
    }

    /**
     * thinking 配置 → Gemini thinkingConfig
     * Anthropic: {type: adaptive} 或 {type: enabled, budget_tokens: N}
     * OpenAI: {effort: low|medium|high}
     * Gemini: {includeThoughts: true, thinkingBudget: N}
     */
    @SuppressWarnings("unchecked")
    private VertexGenerateContentRequest.VertexThinkingConfig toVertexThinkingConfig(Object thinking) {
        if (thinking == null) {
            return null;
        }
        if (thinking instanceof Map<?, ?> map) {
            Object type = map.get("type");
            boolean adaptive = "adaptive".equals(type) || "enabled".equals(type);
            boolean hasThinkingConfig = map.containsKey("budget_tokens") || adaptive;
            if (hasThinkingConfig) {
                Object budget = map.get("budget_tokens");
                Integer budgetInt = budget instanceof Number n ? n.intValue() : null;
                return VertexGenerateContentRequest.VertexThinkingConfig.builder()
                        .includeThoughts(true)
                        .thinkingBudget(budgetInt)
                        .build();
            }
        }
        // OpenAI effort 等：无直接等价，映射为 includeThoughts=true（尽力而为，不丢字段）
        return VertexGenerateContentRequest.VertexThinkingConfig.builder()
                .includeThoughts(true)
                .build();
    }

    /**
     * toolChoice → Gemini toolConfig.functionCallingConfig
     * Anthropic: {type: auto|any|none|tool, name}
     * OpenAI: "auto"|"none"|"required" 或 {type: function, function: {name}}
     * Gemini: {mode: AUTO|ANY|NONE, allowedFunctionNames: [...]}
     */
    @SuppressWarnings("unchecked")
    private VertexGenerateContentRequest.VertexToolConfig toVertexToolConfig(Object toolChoice) {
        if (toolChoice == null) {
            return null;
        }
        String mode = null;
        List<String> allowed = new ArrayList<>();
        if (toolChoice instanceof String s) {
            mode = switch (s) {
                case "auto", "required" -> "AUTO";
                case "none" -> "NONE";
                default -> "AUTO";
            };
        } else if (toolChoice instanceof Map<?, ?> map) {
            Object type = map.get("type");
            if ("none".equals(type)) {
                mode = "NONE";
            } else if ("any".equals(type)) {
                mode = "ANY";
            } else if ("auto".equals(type)) {
                mode = "AUTO";
            } else if ("tool".equals(type)) {
                mode = "ANY";
                Object name = map.get("name");
                if (name instanceof String) {
                    allowed.add((String) name);
                }
            }
        }
        if (mode == null) {
            mode = "AUTO";
        }
        VertexGenerateContentRequest.VertexFunctionCallingConfig.VertexFunctionCallingConfigBuilder cfgBuilder =
                VertexGenerateContentRequest.VertexFunctionCallingConfig.builder()
                        .mode(mode);
        if (!allowed.isEmpty()) {
            cfgBuilder.allowedFunctionNames(allowed);
        }
        return VertexGenerateContentRequest.VertexToolConfig.builder()
                .functionCallingConfig(cfgBuilder.build())
                .build();
    }

    /**
     * Vertex AI 上游响应 → 内部标准响应
     */
    public LlmChatResponse toInternalResponse(VertexGenerateContentResponse vertexResp) {
        LlmChatResponse resp = new LlmChatResponse();
        resp.setModel("vertex"); // 上游可能不返回 model 字段

        if (vertexResp.getCandidates() != null && !vertexResp.getCandidates().isEmpty()) {
            VertexGenerateContentResponse.VertexCandidate firstCandidate = vertexResp.getCandidates().get(0);

            // 提取文本内容 + 函数调用
            StringBuilder textContent = new StringBuilder();
            List<LlmToolCall> toolCalls = new ArrayList<>();
            if (firstCandidate.getContent() != null && firstCandidate.getContent().getParts() != null) {
                for (var part : firstCandidate.getContent().getParts()) {
                    if (part.getText() != null) {
                        textContent.append(part.getText());
                    }
                    if (part.getFunctionCall() != null) {
                        LlmToolCall tc = mapFunctionCallToToolCall(part.getFunctionCall());
                        if (tc != null) {
                            toolCalls.add(tc);
                        }
                    }
                }
            }

            LlmChatResponse.Choice choice = new LlmChatResponse.Choice();
            choice.setIndex(firstCandidate.getIndex() != null ? firstCandidate.getIndex() : 0);
            choice.setFinishReason(mapFinishReason(firstCandidate.getFinishReason()));

            LlmChatResponse.Message msg = new LlmChatResponse.Message();
            msg.setRole("assistant");
            msg.setContent(textContent.toString());
            if (!toolCalls.isEmpty()) {
                msg.setToolCalls(toolCalls);
            }
            choice.setMessage(msg);

            resp.setChoices(List.of(choice));
        }

        // Usage
        if (vertexResp.getUsageMetadata() != null) {
            var usageMeta = vertexResp.getUsageMetadata();
            LlmChatResponse.Usage usage = new LlmChatResponse.Usage();
            usage.setPromptTokens(usageMeta.getPromptTokenCount() != null ? usageMeta.getPromptTokenCount() : 0);
            usage.setCompletionTokens(usageMeta.getCandidatesTokenCount() != null ? usageMeta.getCandidatesTokenCount() : 0);
            usage.setTotalTokens(usageMeta.getTotalTokenCount() != null ? usageMeta.getTotalTokenCount() : 0);
            // Gemini cachedContent 命中：promptTokenCount 已含缓存 tokens，cachedContentTokenCount 是其中命中的部分。
            // 映射到内部 cachedTokens，激活 BillingCalculator「新输入 vs 历史缓存」拆分计费，否则缓存命中按全价计。
            usage.setCachedTokens(usageMeta.getCachedContentTokenCount() != null ? usageMeta.getCachedContentTokenCount() : 0);
            // Gemini 2.5 思考/推理 tokens：candidatesTokenCount 已含思考 tokens，thoughtsTokenCount 是其中思考的部分。
            // 映射到内部 reasoningTokens，供推理价>0 时拆分计费；提取不到（null）则为 0（计费按无推理走输出价）。
            usage.setReasoningTokens(usageMeta.getThoughtsTokenCount() != null ? usageMeta.getThoughtsTokenCount() : 0);
            resp.setUsage(usage);
        }

        return resp;
    }

    /**
     * Vertex AI 上游流式事件 → 内部标准流式块
     * Vertex SSE 每行 data 都是一个完整的 VertexGenerateContentResponse JSON
     * delta 文本在 candidates[0].content.parts[0].text 中
     */
    public LlmStreamChunk toInternalStreamChunk(VertexGenerateContentResponse vertexResp) {
        LlmStreamChunk chunk = new LlmStreamChunk();

        if (vertexResp.getCandidates() != null && !vertexResp.getCandidates().isEmpty()) {
            var candidate = vertexResp.getCandidates().get(0);

            // 提取增量文本 + 函数调用
            if (candidate.getContent() != null && candidate.getContent().getParts() != null) {
                for (var part : candidate.getContent().getParts()) {
                    if (part.getText() != null) {
                        chunk.setDeltaContent(part.getText());
                    }
                    if (part.getFunctionCall() != null) {
                        LlmToolCall tc = mapFunctionCallToToolCall(part.getFunctionCall());
                        if (tc != null) {
                            // Gemini functionCall 是完整对象，单个事件通常只含一个调用，index 从 0 起。
                            // 必须显式设置：若为 null，StreamingConverter 虽已兜底，但源头置 0 更严谨，
                            // 避免多调用场景下 index 丢失导致客户端（Claude Agent SDK）按 index 拼装失败而丢弃工具调用。
                            chunk.setToolCallIndex(0);
                            chunk.setToolCallId(tc.getId() != null ? tc.getId() : tc.getName());
                            chunk.setToolCallName(tc.getName());
                            chunk.setToolCallArgumentsDelta(tc.getArguments());
                        }
                    }
                }
            }

            // 检查是否结束
            if (candidate.getFinishReason() != null && !candidate.getFinishReason().isEmpty()) {
                chunk.setFinished(true);
                chunk.setFinishReason(mapFinishReason(candidate.getFinishReason()));
            }
        }

        if (chunk.getDeltaContent() == null) {
            chunk.setDeltaContent("");
        }

        // Token 统计（最后一个 chunk 会带完整的 usageMetadata，中间 chunk 可能只有 trafficType）
        if (vertexResp.getUsageMetadata() != null) {
            var usageMeta = vertexResp.getUsageMetadata();
            // 只有包含完整 token 统计时才设置 usage
            if (usageMeta.getPromptTokenCount() != null
                    || usageMeta.getCandidatesTokenCount() != null
                    || usageMeta.getTotalTokenCount() != null) {
                chunk.setUsage(LlmUsage.builder()
                        .promptTokens(usageMeta.getPromptTokenCount() != null ? usageMeta.getPromptTokenCount() : 0)
                        .completionTokens(usageMeta.getCandidatesTokenCount() != null ? usageMeta.getCandidatesTokenCount() : 0)
                        .totalTokens(usageMeta.getTotalTokenCount() != null ? usageMeta.getTotalTokenCount() : 0)
                        // Gemini cachedContent 命中 → cachedTokens，激活「新输入 vs 历史缓存」拆分计费
                        .cachedTokens(usageMeta.getCachedContentTokenCount() != null ? usageMeta.getCachedContentTokenCount() : 0)
                        // Gemini 2.5 思考/推理 → reasoningTokens（推理价>0 时拆分计费，提取不到为 0）
                        .reasoningTokens(usageMeta.getThoughtsTokenCount() != null ? usageMeta.getThoughtsTokenCount() : 0)
                        .build());
            }
        }

        return chunk;
    }

    /**
     * Gemini 请求 → 内部标准请求
     * 将 Gemini 格式的请求体转换为内部标准格式
     */
    public LlmChatRequest toInternalRequest(VertexGenerateContentRequest geminiReq, String modelPath) {
        LlmChatRequest internalReq = new LlmChatRequest();
        internalReq.setModel(modelPath);

        // ===== 透传字段：跨协议零遗漏 =====
        // Gemini functionDeclarations → 内部工具定义
        if (geminiReq.getTools() != null && !geminiReq.getTools().isEmpty()) {
            List<LlmToolDefinition> tools = new ArrayList<>();
            for (var tool : geminiReq.getTools()) {
                if (tool.getFunctionDeclarations() != null) {
                    for (var decl : tool.getFunctionDeclarations()) {
                        tools.add(LlmToolDefinition.builder()
                                .name(decl.getName())
                                .description(decl.getDescription())
                                .parameters(decl.getParameters())
                                .build());
                    }
                }
            }
            internalReq.setTools(tools.isEmpty() ? null : tools);
        }
        // Gemini toolConfig.functionCallingConfig → 内部 toolChoice
        if (geminiReq.getToolConfig() != null && geminiReq.getToolConfig().getFunctionCallingConfig() != null) {
            var cfg = geminiReq.getToolConfig().getFunctionCallingConfig();
            if ("ANY".equals(cfg.getMode())) {
                internalReq.setToolChoice(cfg.getAllowedFunctionNames() != null && cfg.getAllowedFunctionNames().size() == 1
                        ? java.util.Map.of("type", "tool", "name", cfg.getAllowedFunctionNames().get(0))
                        : java.util.Map.of("type", "any"));
            } else if ("NONE".equals(cfg.getMode())) {
                internalReq.setToolChoice(java.util.Map.of("type", "none"));
            } else {
                internalReq.setToolChoice(java.util.Map.of("type", "auto"));
            }
        }
        // Gemini thinkingConfig → 内部 thinking
        if (geminiReq.getGenerationConfig() != null && geminiReq.getGenerationConfig().getThinkingConfig() != null) {
            var tc = geminiReq.getGenerationConfig().getThinkingConfig();
            java.util.Map<String, Object> thinking = new java.util.LinkedHashMap<>();
            thinking.put("type", "adaptive");
            if (tc.getThinkingBudget() != null) {
                thinking.put("budget_tokens", tc.getThinkingBudget());
            }
            internalReq.setThinking(thinking);
        }
        // Gemini extraParams → 内部 extraParams
        if (geminiReq.getExtraParams() != null && !geminiReq.getExtraParams().isEmpty()) {
            internalReq.setExtraParams(new java.util.LinkedHashMap<>(geminiReq.getExtraParams()));
        }

        // 转换消息
        List<LlmMessage> messages = new ArrayList<>();

        // systemInstruction → system 消息
        if (geminiReq.getSystemInstruction() != null) {
            String systemText = extractTextFromVertexContent(geminiReq.getSystemInstruction());
            if (systemText != null && !systemText.isEmpty()) {
                messages.add(LlmMessage.builder()
                        .role("system")
                        .textContent(systemText)
                        .build());
            }
        }

        // contents → user/assistant 消息
        // 【关键】按 parts 解析而非仅抽 text：functionCall（assistant 历史工具调用）→ 内部 toolCalls、
        // functionResponse（user 工具结果）→ 内部 tool 消息，否则工具往返第二次请求时 Gemini
        // 收不到工具结果，无法继续生成最终答案。
        // Gemini functionCall/functionResponse 原生协议没有工具调用 id，只有 name；同名函数被多次调用
        // （并行调用/多轮重试）时若直接用 name 作 tool_use id 会撞车 → Anthropic 上游 400
        // "tool_use ids must be unique"。每个请求建一个注册表，按 name FIFO 配对生成唯一 id
        // （详见 ToolCallIdRegistry），保证 tool_use ↔ tool_result 成对且全局唯一。
        ToolCallIdRegistry toolIdRegistry = new ToolCallIdRegistry();
        if (geminiReq.getContents() != null) {
            if (geminiReq.getContents() instanceof List) {
                // 多轮对话，contents 是数组
                @SuppressWarnings("unchecked")
                List<Object> contentsList = (List<Object>) geminiReq.getContents();
                for (var content : contentsList) {
                    VertexGenerateContentRequest.VertexContent vertexContent = convertToVertexContent(content);
                    if (vertexContent != null) {
                        convertVertexContentToMessages(vertexContent, messages, toolIdRegistry);
                    }
                }
            } else {
                // 单轮对话，contents 是单个对象
                VertexGenerateContentRequest.VertexContent singleContent = convertToVertexContent(geminiReq.getContents());
                if (singleContent != null) {
                    convertVertexContentToMessages(singleContent, messages, toolIdRegistry);
                }
            }
        }

        // 客户端中断污染清洗：剥离 "[Tool use interrupted]" / "(no content)"，避免上游模型回显
        PollutionCleaner.clean(messages);

        internalReq.setMessages(messages);

        // 转换生成参数
        if (geminiReq.getGenerationConfig() != null) {
            var genConfig = geminiReq.getGenerationConfig();
            internalReq.setTemperature(genConfig.getTemperature());
            internalReq.setMaxTokens(genConfig.getMaxOutputTokens());
            internalReq.setTopP(genConfig.getTopP());
            internalReq.setTopK(genConfig.getTopK());
            internalReq.setStopSequences(genConfig.getStopSequences());
        }

        return internalReq;
    }

    /**
     * 内部标准响应 → Gemini 响应格式
     */
    public VertexGenerateContentResponse toVertexResponse(LlmChatResponse internalResp) {
        VertexGenerateContentResponse.VertexCandidate.VertexCandidateBuilder candidateBuilder =
                VertexGenerateContentResponse.VertexCandidate.builder();

        if (internalResp.getChoices() != null && !internalResp.getChoices().isEmpty()) {
            var choice = internalResp.getChoices().get(0);
            var msg = choice.getMessage();

            // 构建 content（文本 + 工具调用 functionCall）
            if (msg != null) {
                List<VertexGenerateContentResponse.VertexPart> parts = new ArrayList<>();
                if (msg.getContent() != null) {
                    parts.add(VertexGenerateContentResponse.VertexPart.builder()
                            .text(msg.getContent())
                            .build());
                }
                if (msg.getToolCalls() != null) {
                    for (var tc : msg.getToolCalls()) {
                        Map<String, Object> fc = new java.util.LinkedHashMap<>();
                        fc.put("name", tc.getName());
                        if (tc.getArguments() != null) {
                            try {
                                fc.put("args", objectMapper.readValue(tc.getArguments(), Object.class));
                            } catch (Exception e) {
                                log.warn("[VertexFormatConverter] 工具调用参数解析失败: {}", e.getMessage());
                                fc.put("args", tc.getArguments());
                            }
                        }
                        parts.add(VertexGenerateContentResponse.VertexPart.builder()
                                .functionCall(fc)
                                .build());
                    }
                }
                if (!parts.isEmpty()) {
                    candidateBuilder.content(VertexGenerateContentResponse.VertexContent.builder()
                            .role("model")
                            .parts(parts)
                            .build());
                }
            }

            // 设置 finish reason
            if (choice.getFinishReason() != null) {
                candidateBuilder.finishReason(mapFinishReasonToVertex(choice.getFinishReason()));
            }

            candidateBuilder.index(choice.getIndex());
        }

        VertexGenerateContentResponse.VertexUsageMetadata.VertexUsageMetadataBuilder usageBuilder =
                VertexGenerateContentResponse.VertexUsageMetadata.builder();

        if (internalResp.getUsage() != null) {
            var usage = internalResp.getUsage();
            usageBuilder
                    .promptTokenCount(usage.getPromptTokens())
                    .candidatesTokenCount(usage.getCompletionTokens())
                    .totalTokenCount(usage.getTotalTokens())
                    // 内部缓存命中（OpenAI cached_tokens / Anthropic cache_read）→ Gemini cachedContentTokenCount
                    .cachedContentTokenCount(usage.getCachedTokens() > 0 ? usage.getCachedTokens() : null)
                    // 内部推理 tokens → Gemini thoughtsTokenCount
                    .thoughtsTokenCount(usage.getReasoningTokens() > 0 ? usage.getReasoningTokens() : null);
        }

        return VertexGenerateContentResponse.builder()
                .candidates(List.of(candidateBuilder.build()))
                .usageMetadata(usageBuilder.build())
                .build();
    }

    /**
     * 内部流式块 → Gemini 流式响应格式
     */
    public VertexGenerateContentResponse toVertexStreamChunk(LlmStreamChunk chunk) {
        VertexGenerateContentResponse.VertexCandidate.VertexCandidateBuilder candidateBuilder =
                VertexGenerateContentResponse.VertexCandidate.builder();

        // 构建 content（增量文本 + 工具调用 functionCall）
        // 【关键】内部 chunk 携带 toolCallName（上游 functionCall）时必须重建 functionCall part，
        // 否则客户端（Claude Desktop Gemini 模式）只收到 content=null 的空 candidate，
        // 工具调用被吞掉 → "触发 function call 但界面上没有任何显示"。
        List<VertexGenerateContentResponse.VertexPart> parts = new ArrayList<>();
        if (chunk.getDeltaContent() != null && !chunk.getDeltaContent().isEmpty()) {
            parts.add(VertexGenerateContentResponse.VertexPart.builder()
                    .text(chunk.getDeltaContent())
                    .build());
        }
        // 仅当 name 与参数同时存在才输出 functionCall：流式 tool_use 参数未完整时
        // 不得输出残缺的 {name}（客户端会报 "The required parameter X is missing"）。
        if (chunk.getToolCallName() != null && chunk.getToolCallArgumentsDelta() != null
                && !chunk.getToolCallArgumentsDelta().isEmpty()) {
            Map<String, Object> fc = new java.util.LinkedHashMap<>();
            fc.put("name", chunk.getToolCallName());
            try {
                fc.put("args", objectMapper.readValue(chunk.getToolCallArgumentsDelta(), Object.class));
            } catch (Exception e) {
                log.warn("[VertexFormatConverter] 工具调用参数解析失败: {}", e.getMessage());
                fc.put("args", chunk.getToolCallArgumentsDelta());
            }
            parts.add(VertexGenerateContentResponse.VertexPart.builder()
                    .functionCall(fc)
                    .build());
        }
        if (!parts.isEmpty()) {
            candidateBuilder.content(VertexGenerateContentResponse.VertexContent.builder()
                    .role("model")
                    .parts(parts)
                    .build());
        }

        // 设置 finish reason
        if (chunk.isFinished() && chunk.getFinishReason() != null) {
            candidateBuilder.finishReason(mapFinishReasonToVertex(chunk.getFinishReason()));
        }

        VertexGenerateContentResponse.VertexUsageMetadata.VertexUsageMetadataBuilder usageBuilder =
                VertexGenerateContentResponse.VertexUsageMetadata.builder();

        // Token 统计（最后一个 chunk）
        if (chunk.getUsage() != null) {
            usageBuilder
                    .promptTokenCount(chunk.getUsage().getPromptTokens())
                    .candidatesTokenCount(chunk.getUsage().getCompletionTokens())
                    .totalTokenCount(chunk.getUsage().getTotalTokens())
                    .cachedContentTokenCount(chunk.getUsage().getCachedTokens() > 0 ? chunk.getUsage().getCachedTokens() : null)
                    .thoughtsTokenCount(chunk.getUsage().getReasoningTokens() > 0 ? chunk.getUsage().getReasoningTokens() : null);
        }

        return VertexGenerateContentResponse.builder()
                .candidates(List.of(candidateBuilder.build()))
                .usageMetadata(usageBuilder.build())
                .build();
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 从内部消息中提取文本
     * 优先使用 textContent，否则从 contents 列表拼接
     */
    private String extractText(LlmMessage msg) {
        String text = msg.getTextContent();
        if (text == null && msg.getContents() != null && !msg.getContents().isEmpty()) {
            text = msg.getContents().stream()
                    .filter(c -> "text".equals(c.getType()))
                    .map(LlmContent::getText)
                    .filter(t -> t != null && !t.isEmpty())
                    .collect(Collectors.joining("\n"));
        }
        return text;
    }

    /**
     * 内部角色 → Vertex AI 角色
     * user → user, assistant → model
     */
    private String mapRoleToVertex(String internalRole) {
        if ("assistant".equals(internalRole)) {
            return "model";
        }
        return "user";
    }

    /**
     * Vertex finishReason → 内部 finish_reason
     */
    private String mapFinishReason(String vertexReason) {
        if (vertexReason == null) return "stop";
        return switch (vertexReason) {
            case "STOP" -> "stop";
            case "MAX_TOKENS" -> "length";
            case "SAFETY" -> "content_filter";
            case "RECITATION" -> "content_filter";
            default -> "stop";
        };
    }

    /**
     * 内部 finish_reason → Vertex finishReason
     */
    private String mapFinishReasonToVertex(String internalReason) {
        if (internalReason == null) return null;
        return switch (internalReason) {
            case "stop" -> "STOP";
            // Gemini 原生 functionCall 回合结束同样用 STOP（非 FUNCTION_CALL），
            // 与网关自身透传上游 Gemini 的 STOP 一致，客户端按 parts 识别工具调用。
            case "tool_calls" -> "STOP";
            case "length" -> "MAX_TOKENS";
            case "content_filter" -> "SAFETY";
            default -> "OTHER";
        };
    }

    /**
     * Vertex 角色 → 内部角色
     * model → assistant, user → user
     */
    private String mapRoleToInternal(String vertexRole) {
        if ("model".equals(vertexRole)) {
            return "assistant";
        }
        return "user";
    }

    /**
     * Gemini functionCall 部分 → 内部 LlmToolCall
     * Gemini 返回: {"name": "xxx", "args": {...}}
     * 内部: {id, type=function, name, arguments=JSON字符串}
     */
    @SuppressWarnings("unchecked")
    private LlmToolCall mapFunctionCallToToolCall(Object functionCall) {
        return mapFunctionCallToToolCall(functionCall, null);
    }

    /**
     * Gemini functionCall part → 内部工具调用。
     *
     * id 的取值取决于是否传入注册表：
     * - 无注册表（上游 Vertex→内部路径，id 只作占位、随输出协议丢弃或重生成）：直接用 name 作 id，保持旧行为
     * - 有注册表（客户端 Vertex→内部，后续转 Anthropic 上游）：由注册表按 name FIFO 分配唯一 id，
     *   避免同名函数多次调用时 tool_use id 撞车（Anthropic 上游 400 "tool_use ids must be unique"）
     */
    @SuppressWarnings("unchecked")
    private LlmToolCall mapFunctionCallToToolCall(Object functionCall, ToolCallIdRegistry toolIdRegistry) {
        if (functionCall == null) {
            return null;
        }
        if (functionCall instanceof Map<?, ?> map) {
            String name = map.get("name") instanceof String ? (String) map.get("name") : null;
            Object args = map.get("args");
            String argsJson;
            if (args == null) {
                argsJson = "{}";
            } else if (args instanceof String) {
                argsJson = (String) args;
            } else {
                try {
                    argsJson = objectMapper.writeValueAsString(args);
                } catch (Exception e) {
                    log.warn("[VertexFormatConverter] 函数调用参数序列化失败: {}", e.getMessage());
                    argsJson = "{}";
                }
            }
            return LlmToolCall.builder()
                    .id(toolIdRegistry != null ? toolIdRegistry.registerCall(name) : name)
                    .type("function")
                    .name(name)
                    .arguments(argsJson)
                    .build();
        }
        return null;
    }

    /**
     * 递归清洗 JSON Schema，删除 Gemini 不认识的方言键。
     *
     * Anthropic input_schema（JSON Schema draft-2020-12）与 OpenAI function.parameters
     * 常携带 $schema / propertyNames / const / exclusiveMinimum 等键，Gemini 严格校验不接受，
     * 直接透传会被 400 拒绝（见运行时日志 "Unknown name \"const\" ... any_of[1]"）。
     *
     * 采用深度递归：任意键名下的 Map 值（如 {value: {any_of: [...]}}）和
     * anyOf/oneOf/allOf 数组里的每个分支 schema 都会被继续清洗，保证 const 等方言键
     * 不残留在任何嵌套层级。properties 的属性名单独保留（属性名可能是 const 之类的字面量，
     * 只清洗属性值的 schema），enum 等标量/字符串数组元素不受影响。
     *
     * @param schema 原始 JSON Schema（可为 null）
     * @return 清洗后的副本；null 原样返回
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> sanitizeJsonSchema(Map<String, Object> schema) {
        if (schema == null) {
            return null;
        }
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : schema.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            // 方言键丢弃：命名黑名单 + 所有 $ 前缀的引用/方言键
            if (GEMINI_SCHEMA_BLOCKED.contains(key) || key.startsWith("$")) {
                continue;
            }
            if ("properties".equals(key) && value instanceof Map) {
                // properties 的 key 是属性名，必须原样保留（属性名可能恰好叫 const/$ref 之类），
                // 只递归清洗每个属性的 schema
                Map<String, Object> cleanedProps = new java.util.LinkedHashMap<>();
                for (Map.Entry<String, Object> propEntry : ((Map<String, Object>) value).entrySet()) {
                    cleanedProps.put(propEntry.getKey(), sanitizeJsonSchemaValue(propEntry.getValue()));
                }
                result.put(key, cleanedProps);
            } else {
                // 其余任意值深度清洗（Map / List<Map> / 标量）
                result.put(key, sanitizeJsonSchemaValue(value));
            }
        }
        return result;
    }

    /**
     * 递归清洗单个 JSON Schema 值：Map 继续清洗，List 逐个清洗（覆盖 anyOf/oneOf/allOf 数组），
     * 标量/字符串数组（enum、required 等）原样返回。
     */
    @SuppressWarnings("unchecked")
    private Object sanitizeJsonSchemaValue(Object value) {
        if (value instanceof Map) {
            return sanitizeJsonSchema((Map<String, Object>) value);
        }
        if (value instanceof List) {
            List<Object> list = (List<Object>) value;
            List<Object> cleaned = new ArrayList<>(list.size());
            for (Object item : list) {
                cleaned.add(sanitizeJsonSchemaValue(item));
            }
            return cleaned;
        }
        return value;
    }

    /**
     * 从 VertexContent 中提取文本
     */
    private String extractTextFromVertexContent(VertexGenerateContentRequest.VertexContent content) {
        if (content == null || content.getParts() == null || content.getParts().isEmpty()) {
            return null;
        }
        StringBuilder text = new StringBuilder();
        for (var part : content.getParts()) {
            if (part.getText() != null) {
                text.append(part.getText());
            }
        }
        return text.length() > 0 ? text.toString() : null;
    }

    /**
     * 将累积的 functionResponse parts 合并为一个 user content 写入 contents，并清空缓冲。
     * 同一 functionCall 回合的多个函数结果必须在一个 user content 中（Gemini 数量校验）。
     */
    private void flushFunctionResponses(List<VertexGenerateContentRequest.VertexPart> pending,
            List<VertexGenerateContentRequest.VertexContent> contents) {
        if (pending == null || pending.isEmpty()) {
            return;
        }
        contents.add(VertexGenerateContentRequest.VertexContent.builder()
                .role("user")
                .parts(new ArrayList<>(pending))
                .build());
        pending.clear();
    }

    /**
     * Vertex contents 单条内容 → 内部消息。
     * 支持三种 part 类型（可与文本共存）：
     * - functionCall（assistant 历史工具调用）→ 内部 toolCalls
     * - functionResponse（user 工具结果）→ 独立 tool 角色消息（带 name）
     * - text → 文本消息
     */
    @SuppressWarnings("unchecked")
    private void convertVertexContentToMessages(VertexGenerateContentRequest.VertexContent vertexContent,
            List<LlmMessage> out, ToolCallIdRegistry toolIdRegistry) {
        if (vertexContent == null || vertexContent.getParts() == null || vertexContent.getParts().isEmpty()) {
            return;
        }
        String role = mapRoleToInternal(vertexContent.getRole()); // "model"→assistant，其余→user
        List<VertexGenerateContentRequest.VertexPart> parts = vertexContent.getParts();

        StringBuilder text = new StringBuilder();
        List<LlmToolCall> toolCalls = new ArrayList<>();
        List<LlmMessage> toolResults = new ArrayList<>();

        for (var part : parts) {
            if (part.getFunctionCall() != null) {
                LlmToolCall tc = mapFunctionCallToToolCall(part.getFunctionCall(), toolIdRegistry);
                if (tc != null) {
                    toolCalls.add(tc);
                }
            } else if (part.getFunctionResponse() != null) {
                LlmMessage toolMsg = mapFunctionResponseToToolMessage(part.getFunctionResponse(), toolIdRegistry);
                if (toolMsg != null) {
                    toolResults.add(toolMsg);
                }
            } else if (part.getText() != null) {
                text.append(part.getText());
            }
        }

        // tool 结果消息先于同条 user 消息的文本输出，保证上游 functionCall→functionResponse 相邻
        out.addAll(toolResults);
        if (!toolCalls.isEmpty() || text.length() > 0) {
            out.add(LlmMessage.builder()
                    .role(toolCalls.isEmpty() ? role : "assistant")
                    .textContent(text.toString())
                    .toolCalls(toolCalls.isEmpty() ? null : toolCalls)
                    .build());
        }
    }

    /**
     * Gemini functionResponse part → 内部 tool 消息
     * Gemini 格式: {"name": "xxx", "response": {"result": "..."}}
     */
    @SuppressWarnings("unchecked")
    private LlmMessage mapFunctionResponseToToolMessage(Object functionResponse, ToolCallIdRegistry toolIdRegistry) {
        if (!(functionResponse instanceof Map<?, ?> map)) {
            return null;
        }
        String name = map.get("name") instanceof String ? (String) map.get("name") : null;
        Object response = map.get("response");
        String output;
        // Gemini functionResponse 的 response 载荷无统一约定：Claude Desktop 发 {"content": "..."}，
        // 其它客户端可能发 {"result": "..."} 或纯字符串。优先取 content/result 文本，其余序列化。
        Object textValue = null;
        if (response instanceof Map<?, ?> respMap) {
            textValue = respMap.get("content");
            if (textValue == null) {
                textValue = respMap.get("result");
            }
        } else {
            textValue = response;
        }
        if (textValue instanceof String s) {
            output = s;
        } else if (textValue != null) {
            try {
                output = objectMapper.writeValueAsString(textValue);
            } catch (Exception e) {
                output = textValue.toString();
            }
        } else {
            output = "";
        }
        // toolCallId 必须与对应 functionCall 转换出的 id 一致（注册表按 name FIFO 配对），
        // 否则转 Anthropic 上游时 tool_result.tool_use_id 为空串 → 400
        // "tool_result.tool_use_id: String should match pattern '^[a-zA-Z0-9_-]+$'"
        // 或与已有 tool_use id 对不上/重复 → 400 "tool_use ids must be unique"。
        return LlmMessage.builder()
                .role("tool")
                .name(name)
                .toolCallId(toolIdRegistry != null ? toolIdRegistry.takeResponseId(name) : name)
                .textContent(output)
                .build();
    }

    /**
     * 按请求隔离的 Gemini functionCall/functionResponse id 注册表。
     *
     * Gemini 原生协议的工具调用没有 id 字段，只有 name，网关历史上直接用 name 充当 tool_use id；
     * 同名函数被多次调用（并行调用/多轮重试，如两次 Bash）时 id 撞车，Anthropic 上游校验
     * "tool_use ids must be unique" 直接 400，整轮工具往返静默断裂。本注册表：
     * - registerCall(name)：为第 n 次同名调用生成唯一 id = sanitize(name)_n，并按 name 压入队列
     * - takeResponseId(name)：functionResponse 从该 name 的队列取队首 id（FIFO）。
     *   与 Gemini 的配对语义一致（同名 functionResponse 按出现顺序对应同名 functionCall），
     *   保证 tool_use ↔ tool_result 成对且全局唯一。
     *
     * 因为 VertexFormatConverter 是 Spring 单例（并发请求共享实例），注册表必须在每个请求
     * 的 toInternalRequest 里 new 一个，作为参数传入，不能放实例字段。
     */
    private static class ToolCallIdRegistry {
        private final Map<String, ArrayDeque<String>> pendingIds = new HashMap<>();

        /** functionCall 登记：生成并压入该 name 的队列，返回本次调用的唯一 id */
        String registerCall(String name) {
            ArrayDeque<String> queue = pendingIds.computeIfAbsent(name, k -> new ArrayDeque<>());
            String id = sanitizeToolId(name) + "_" + (queue.size() + 1);
            queue.add(id);
            return id;
        }

        /** functionResponse 认领：从该 name 的队列取队首 id（FIFO）；无待配对调用时回退为纯 name */
        String takeResponseId(String name) {
            ArrayDeque<String> queue = pendingIds.get(name);
            if (queue != null && !queue.isEmpty()) {
                return queue.poll();
            }
            return sanitizeToolId(name);
        }

        /** Anthropic tool_use/tool_use_id 只允许 [a-zA-Z0-9_-]，MCP 工具名可能含 . 等字符，转 _ 保证不破坏匹配 */
        private static String sanitizeToolId(String name) {
            return name == null ? "" : name.replaceAll("[^a-zA-Z0-9_-]", "_");
        }
    }
}
