package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.config.GatewayErrorResponseBuilder;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.converter.OpenAiProtocolConverter;
import com.llmate.multiprotocol.dto.openai.OpenAiChatRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiChatResponse;
import com.llmate.multiprotocol.engine.LlmGateway;
import com.llmate.multiprotocol.engine.ProtocolManager;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/v1")
@RequireApiKey
@Log4j2
public class OpenAiCompatibleController {
    private final OpenAiProtocolConverter converter;
    private final LlmGateway gateway;
    private final ProtocolManager protocolManager;
    private final GatewayErrorResponseBuilder errorBuilder;

    public OpenAiCompatibleController(OpenAiProtocolConverter converter, LlmGateway gateway, ProtocolManager protocolManager,
                                      GatewayErrorResponseBuilder errorBuilder) {
        this.converter = converter;
        this.gateway = gateway;
        this.protocolManager = protocolManager;
        this.errorBuilder = errorBuilder;
    }

    /**
     * 聊天补全统一入口：流式 / 非流式由 body 的 stream 字段决定，而非 Accept 头
     *
     * 注意：不能拆成两个靠 produces 区分的 @PostMapping —— Spring 会按客户端 Accept 头路由，
     * 客户端（如 Postman 默认 Accept）不带 text/event-stream 时，stream:true 的请求会被误路由到
     * 非流式方法。正确做法与 Anthropic/Responses 控制器一致：单端点 + 读取 body 的 stream 字段分流。
     */
    @PostMapping(value = "/chat/completions", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public Mono<ResponseEntity<?>> chatCompletions(@RequestBody OpenAiChatRequest request, ServerWebExchange exchange) {
        log.info("[OpenAI-Controller] 收到请求: model={}, stream={}, messages={}",
                request.getModel(), request.getStream(),
                request.getMessages() != null ? request.getMessages().size() : 0);
        protocolManager.bindProtocol(exchange, ProtocolType.OPENAI_CHAT_COMPLETIONS);

        boolean isStream = Boolean.TRUE.equals(request.getStream());
        if (isStream) {
            log.info("[OpenAI-Controller] 进入流式处理流程");
            String maskedModel = request.getModel();
            return Mono.just(ResponseEntity.ok()
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .header("Cache-Control", "no-cache, no-store, must-revalidate")
                    .header("Pragma", "no-cache")
                    .header("Expires", "0")
                    .header("X-Accel-Buffering", "no")
                    .header("Connection", "keep-alive")
                    .body(handleStreamingRequest(request, maskedModel, exchange)));
        } else {
            log.info("[OpenAI-Controller] 进入非流式处理流程");
            return handleBlockingRequest(request, exchange)
                    .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp));
        }
    }

    private Mono<OpenAiChatResponse> handleBlockingRequest(OpenAiChatRequest request, ServerWebExchange exchange) {
        log.info("[OpenAI-Controller] 开始非流式请求处理");
        return converter.toInternalRequest(request)
                .flatMap(internalRequest -> gateway.chat(internalRequest, exchange))
                .map(converter::toExternalResponse)
                .doOnSuccess(resp -> log.info("[OpenAI-Controller] 非流式响应完成: id={}, model={}", resp.getId(), resp.getModel()))
                .doOnError(e -> log.error("[OpenAI-Controller] 非流式请求处理失败", e));
    }

    private Flux<ServerSentEvent<Object>> handleStreamingRequest(OpenAiChatRequest request, String maskedModelName, ServerWebExchange exchange) {
        log.info("[OpenAI-Controller] 开始流式请求处理");
        return converter.toInternalRequest(request)
                .flatMapMany(internalRequest -> gateway.chatStream(internalRequest, exchange))
                .transform(stream -> converter.toExternalStream(stream, request, maskedModelName))
                .doOnNext(sse -> log.debug("[OpenAI-Controller] 流式输出chunk: {}", sse.data()))
                .doOnComplete(() -> log.info("[OpenAI-Controller] 流式输出完成"))
                .doOnError(e -> log.error("[OpenAI-Controller] 流式请求处理失败", e))
                .onErrorResume(e -> catchStreamErrorAndEmit(e, exchange));
    }

    private Flux<ServerSentEvent<Object>> catchStreamErrorAndEmit(Throwable e, ServerWebExchange exchange) {
        log.error("[OpenAI-Controller] 流式错误处理", e);
        // OpenAI 协议错误事件后附 [DONE] 结束标记（保持现状）；message 统一脱敏 + 附 request_id
        return errorBuilder.streamErrorEvents(e, exchange, true);
    }
}

