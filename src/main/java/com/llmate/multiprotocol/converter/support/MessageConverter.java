package com.llmate.multiprotocol.converter.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmContent;
import com.llmate.multiprotocol.dto.LlmMessage;
import com.llmate.multiprotocol.dto.LlmToolCall;
import com.llmate.multiprotocol.dto.openai.OpenAiMessage;
import com.llmate.multiprotocol.dto.openai.ContentPart;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * 消息结构转换辅助组件
 * 负责 OpenAI 消息 → 内部标准消息的非阻塞转换（支持异步图片下载）
 */
@Component
@Log4j2
public class MessageConverter {

    private final WebClient webClient;
    private final ObjectMapper objectMapper;

    public MessageConverter(ObjectMapper objectMapper) {
        // 使用共享图片下载客户端（WebClientUtils.imageDownloadClient()）：
        // - ConnectionProvider.newConnection() 禁用连接池，规避国内服务器 NAT 断连
        // - followRedirect(true)：图床 URL 302 跳 CDN 必须跟随，否则空 body → base64 空串
        // - 响应超时 5 分钟（SystemConstants.HTTP_TIMEOUT_IMAGE_DOWNLOAD_SECONDS）
        // 与 Anthropic/Responses 入站的 URL 图片下载共用同一客户端，行为一致。
        this.webClient = WebClientUtils.imageDownloadClient();
        // 注入 Spring 单例 ObjectMapper，不各自 new 一份
        this.objectMapper = objectMapper;
    }

    /**
     * OpenAI 格式消息体向网关内部统一标准消息体的非阻塞转换
     *
     * 覆盖 OpenAI 全消息类型：
     * - system → 内部 system
     * - user 文本 / 多模态 → 内部 textContent / contents
     * - assistant + tool_calls（OpenAI 标准下 content 为 null）→ 内部 toolCalls（供上游还原 Gemini functionCall / Anthropic tool_use）
     * - tool（函数执行结果）→ 内部 toolCallId + name + textContent（供上游还原 Gemini functionResponse / Anthropic tool_result）
     */
    public Mono<List<LlmMessage>> openAiToInternal(List<OpenAiMessage> messages) {
        log.info("[MessageConverter] 开始转换 {} 条消息", messages != null ? messages.size() : 0);
        return Flux.fromIterable(messages)
                .flatMapSequential(msg -> {
                    log.debug("[MessageConverter] 转换消息: role={}, content类型={}", msg.getRole(),
                            msg.getContent() != null ? msg.getContent().getClass().getSimpleName() : "null");
                    if ("system".equals(msg.getRole())) {
                        String content = msg.getContent() != null ? msg.getContent().toString() : "";
                        log.debug("[MessageConverter] 转换system消息: {}", content.substring(0, Math.min(50, content.length())));
                        return Mono.just(LlmMessage.system(content));
                    }
                    if ("assistant".equals(msg.getRole()) && msg.getToolCalls() != null && !msg.getToolCalls().isEmpty()) {
                        // 【兼容修复】OpenAI 标准：assistant 消息带 tool_calls 时 content 为 null（工具调用不输出文本）。
                        // 之前的 else 分支对 null content 调 toString() 直接 NPE —— Claude Desktop 工具调用往返时必现。
                        // 提取 tool_calls → 内部 LlmToolCall，供上游还原 functionCall / tool_use。
                        List<LlmToolCall> toolCalls = msg.getToolCalls().stream()
                                .map(tc -> LlmToolCall.builder()
                                        .id(tc.getId())
                                        .type(tc.getType() != null ? tc.getType() : "function")
                                        .name(tc.getFunction() != null ? tc.getFunction().getName() : null)
                                        .arguments(tc.getFunction() != null ? tc.getFunction().getArguments() : null)
                                        .build())
                                .toList();
                        String text = msg.getContent() != null ? msg.getContent().toString() : null;
                        log.debug("[MessageConverter] 转换assistant工具调用消息: toolCalls={}",
                                toolCalls.size());
                        return Mono.just(LlmMessage.builder()
                                .role("assistant")
                                .textContent(text)
                                .toolCalls(toolCalls)
                                .build());
                    }
                    if ("tool".equals(msg.getRole())) {
                        // OpenAI tool 角色消息（函数执行结果）：content 可能是 String 或多模态 parts 数组。
                        // 统一提取文本，关联 tool_call_id + name，供上游还原 functionResponse / tool_result。
                        String text = extractTextContent(msg.getContent());
                        log.debug("[MessageConverter] 转换tool结果消息: toolCallId={}, name={}", msg.getToolCallId(), msg.getName());
                        return Mono.just(LlmMessage.builder()
                                .role("tool")
                                .textContent(text)
                                .toolCallId(msg.getToolCallId())
                                .name(msg.getName())
                                .build());
                    }
                    if (msg.getContent() instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> rawParts = (List<Map<String, Object>>) msg.getContent();
                        log.debug("[MessageConverter] 转换多模态消息: {} 个parts", rawParts.size());
                        List<ContentPart> parts = rawParts.stream()
                                .map(map -> objectMapper.convertValue(map, ContentPart.class))
                                .toList();
                        return Flux.fromIterable(parts)
                                .flatMapSequential(this::convertContentPartAsync)
                                .collectList()
                                .map(contents -> LlmMessage.builder().role(msg.getRole()).contents(contents).build());
                    } else {
                        // 兜底：content 可能为 null（防御），避免 NPE
                        String text = msg.getContent() != null ? msg.getContent().toString() : "";
                        log.debug("[MessageConverter] 转换文本消息: {}", text.substring(0, Math.min(50, text.length())));
                        return Mono.just(LlmMessage.builder().role(msg.getRole()).textContent(text).build());
                    }
                })
                .collectList()
                .map(list -> {
                    // 客户端中断污染清洗：剥离 "[Tool use interrupted]" / "(no content)"
                    PollutionCleaner.clean(list);
                    return list;
                })
                .doOnNext(list -> log.info("[MessageConverter] 消息转换完成: {} 条", list.size()));
    }

    /**
     * 从 OpenAI tool 角色消息的 content（可能是 String 或多模态 parts 数组）提取纯文本。
     * OpenAI 新版 tool 结果 content 可能为 [{"type":"text","text":"..."}] 结构。
     */
    private String extractTextContent(Object content) {
        if (content == null) {
            return "";
        }
        if (content instanceof String) {
            return (String) content;
        }
        if (content instanceof List<?> parts) {
            StringBuilder sb = new StringBuilder();
            for (Object part : parts) {
                if (part instanceof Map<?, ?> map) {
                    Object text = map.get("text");
                    if (text != null) {
                        sb.append(text);
                    }
                }
            }
            return sb.toString();
        }
        return content.toString();
    }

    /**
     * 内容块 → 内部 LlmContent。
     * 支持三种块：text、image_url（OpenAI 标准）、image（Anthropic 风格块，部分客户端对 chat 入口也发这种）。
     * 图片统一经 {@link #convertImageUrl}：data URI 直接解析、http URL 异步下载转 base64。
     */
    private Mono<LlmContent> convertContentPartAsync(ContentPart part) {
        if ("text".equals(part.getType())) {
            log.debug("[MessageConverter] 转换文本part: {}", part.getText() != null ? part.getText().substring(0, Math.min(30, part.getText().length())) : "null");
            return Mono.just(LlmContent.text(part.getText()));
        }
        if ("image_url".equals(part.getType()) && part.getImageUrl() != null && part.getImageUrl().getUrl() != null) {
            return convertImageUrl(part.getImageUrl().getUrl());
        }
        if ("image".equals(part.getType())) {
            return convertAnthropicStyleImage(part);
        }
        log.error("[MessageConverter] 不支持的内容类型: {}", part.getType());
        return Mono.error(new UnsupportedOperationException("不支持的内容类型: " + part.getType()));
    }

    /**
     * 图片 URL（data URI 或 http 链接）→ LlmContent.image。
     * data URI 直接解析；http URL 纯异步下载转 base64，下载前 percent-decode 防二次编码坑
     * （%XX → %25XX，见 WebClientUtils.decodeImageUrl），共享客户端跟随重定向。
     */
    private Mono<LlmContent> convertImageUrl(String url) {
        log.info("[MessageConverter] 转换图片: url={}", url.substring(0, Math.min(50, url.length())));
        if (url.startsWith("data:")) {
            // Base64 Data URI 直接解析
            String[] parts = url.split(",");
            String mimeType = parts[0].substring(parts[0].indexOf(":") + 1, parts[0].indexOf(";"));
            log.debug("[MessageConverter] 解析Base64图片: mimeType={}", mimeType);
            return Mono.just(LlmContent.image(parts[1], mimeType));
        }
        // 核心修复：纯异步非阻塞流式下载网络图片，避免工作线程阻塞。
        // 下载前先 percent-decode：客户端 URL 常已含 %XX 编码，WebClient UriBuilder 会二次编码成
        // %25XX → 404/502（见 WebClientUtils.decodeImageUrl）。
        String decodedUrl = WebClientUtils.decodeImageUrl(url);
        log.info("[MessageConverter] 异步下载网络图片: {}", decodedUrl);
        return webClient.get().uri(decodedUrl)
                .retrieve()
                .bodyToMono(byte[].class)
                .map(bytes -> Base64.getEncoder().encodeToString(bytes))
                .map(base64 -> LlmContent.image(base64, WebClientUtils.detectImageMimeType(decodedUrl)))
                .doOnNext(content -> log.info("[MessageConverter] 图片下载完成: mimeType={}", content.getMimeType()))
                .doOnError(e -> log.error("[MessageConverter] 图片下载失败: {}", decodedUrl, e))
                .onErrorMap(e -> new LlmGatewayException(LlmErrorCode.IMAGE_DOWNLOAD_FAILED, url, e));
    }

    /**
     * Anthropic 风格 image 块（source.type ∈ base64 / url / file）→ LlmContent.image。
     * chat 入口兼容部分客户端直接发这种块；file（Files API 引用）暂不支持，明确 400，
     * 与 Anthropic 入站 source.type=file 行为一致。
     */
    private Mono<LlmContent> convertAnthropicStyleImage(ContentPart part) {
        Map<String, Object> source = part.getSource();
        if (source == null) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "image 块缺少 source"));
        }
        String sourceType = source.get("type") instanceof String s ? s : null;
        if ("base64".equals(sourceType)) {
            String data = source.get("data") instanceof String s ? s : null;
            String mediaType = source.get("media_type") instanceof String s ? s : null;
            if (data != null && !data.isEmpty()) {
                return Mono.just(LlmContent.image(data, mediaType));
            }
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "image source.base64 缺少 data"));
        }
        if ("url".equals(sourceType)) {
            String url = source.get("url") instanceof String s ? s : null;
            if (url != null && !url.isEmpty()) {
                return convertImageUrl(url);
            }
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "image source.url 缺少 url"));
        }
        return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "image source.type 暂不支持: " + sourceType));
    }
}
