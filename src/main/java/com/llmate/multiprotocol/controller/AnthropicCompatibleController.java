package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.config.GatewayErrorResponseBuilder;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.converter.AnthropicProtocolConverter;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesRequest;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesResponse;
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

/**
 * Anthropic 协议兼容控制器
 * 拦截并处理所有来自 Anthropic Claude SDK (如 /v1/messages) 的请求
 */
@RestController
@RequestMapping("/v1")
@RequireApiKey
@Log4j2
public class AnthropicCompatibleController {

    private final AnthropicProtocolConverter converter;
    private final LlmGateway gateway;
    private final ProtocolManager protocolManager;
    private final GatewayErrorResponseBuilder errorBuilder;

    public AnthropicCompatibleController(AnthropicProtocolConverter converter,
                                        LlmGateway gateway,
                                        ProtocolManager protocolManager,
                                        GatewayErrorResponseBuilder errorBuilder) {
        this.converter = converter;
        this.gateway = gateway;
        this.protocolManager = protocolManager;
        this.errorBuilder = errorBuilder;
    }

    @PostMapping(value = "/messages", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public Mono<ResponseEntity<?>> messages(
            @RequestBody AnthropicMessagesRequest request,
            @RequestHeader(value = "x-api-key", required = false) String apiKey,
            ServerWebExchange exchange) {

        // 1. 在异步响应式上下文中绑定协议状态
        protocolManager.bindProtocol(exchange, ProtocolType.ANTHROPIC_MESSAGES);

        boolean isStream = Boolean.TRUE.equals(request.getStream());
        log.info("[Anthropic-Controller] 收到请求: model={}, stream={}, messages={}",
                request.getModel(), request.getStream(),
                request.getMessages() != null ? request.getMessages().size() : 0);

        if (isStream) {
            log.info("[Anthropic-Controller] 进入流式处理流程");
            return Mono.just(ResponseEntity.ok()
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .header("Cache-Control", "no-cache, no-store, must-revalidate")
                    .header("Pragma", "no-cache")
                    .header("Expires", "0")
                    .header("X-Accel-Buffering", "no")
                    .header("Connection", "keep-alive")
                    .body(handleStreamingRequest(request, exchange)));
        } else {
            log.info("[Anthropic-Controller] 进入非流式处理流程");
            return handleBlockingRequest(request, exchange)
                    .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp));
        }
    }

    /**
     * 阻塞/非流式请求处理
     */
    private Mono<AnthropicMessagesResponse> handleBlockingRequest(AnthropicMessagesRequest request, ServerWebExchange exchange) {
        log.info("[Anthropic-Controller] 开始非流式请求处理");
        return converter.toInternalRequest(request)
                .doOnNext(req -> log.info("[Anthropic-Controller] 内部请求已构建: model={}, messages={}",
                        req.getModel(), req.getMessages() != null ? req.getMessages().size() : 0))
                .flatMap(internalRequest -> gateway.chat(internalRequest, exchange))
                .doOnNext(resp -> log.info("[Anthropic-Controller] 收到内部响应: id={}, model={}", resp.getId(), resp.getModel()))
                .map(converter::toExternalResponse)
                .doOnNext(extResp -> log.info("[Anthropic-Controller] 非流式响应完成: id={}, model={}", extResp.getId(), extResp.getModel()))
                .doOnError(e -> log.error("[Anthropic-Controller] 非流式请求处理失败", e));
    }

    /**
     * 流式请求处理
     */
    private Flux<ServerSentEvent<Object>> handleStreamingRequest(AnthropicMessagesRequest request, ServerWebExchange exchange) {
        log.info("[Anthropic-Controller] 开始流式请求处理");
        String maskedModelName = request.getModel();
        return converter.toInternalRequest(request)
                .doOnNext(req -> log.info("[Anthropic-Controller] 内部请求已构建: model={}, messages={}",
                        req.getModel(), req.getMessages() != null ? req.getMessages().size() : 0))
                .flatMapMany(internalRequest -> gateway.chatStream(internalRequest, exchange))
                .doOnNext(chunk -> log.info("[Anthropic-Controller] 收到内部chunk: deltaContent={}, finished={}",
                        chunk.getDeltaContent(), chunk.isFinished()))
                .transform(stream -> converter.toExternalStream(stream, request, maskedModelName))
                .doOnNext(sse -> log.debug("[Anthropic-Controller] 流式输出SSE: event={}, data={}", sse.event(), sse.data()))
                .doOnComplete(() -> log.info("[Anthropic-Controller] 流式输出完成"))
                .doOnError(e -> log.error("[Anthropic-Controller] 流式请求处理失败", e))
                .onErrorResume(e -> {
                    log.error("[Anthropic-Controller] 流式错误处理", e);
                    return errorBuilder.streamErrorEvents(e, exchange, false);
                });
    }
}
