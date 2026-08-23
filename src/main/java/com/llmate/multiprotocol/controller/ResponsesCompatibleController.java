package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.config.GatewayErrorResponseBuilder;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.converter.ResponsesProtocolConverter;
import com.llmate.multiprotocol.dto.openai.OpenAiResponsesRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiResponsesResponse;
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
 * OpenAI Responses API 兼容控制器
 * 拦截并处理 /v1/responses 请求（用于 o1 等推理模型）
 */
@RestController
@RequestMapping("/v1")
@RequireApiKey
@Log4j2
public class ResponsesCompatibleController {

    private final ResponsesProtocolConverter converter;
    private final LlmGateway gateway;
    private final ProtocolManager protocolManager;
    private final GatewayErrorResponseBuilder errorBuilder;

    public ResponsesCompatibleController(ResponsesProtocolConverter converter,
                                        LlmGateway gateway,
                                        ProtocolManager protocolManager,
                                        GatewayErrorResponseBuilder errorBuilder) {
        this.converter = converter;
        this.gateway = gateway;
        this.protocolManager = protocolManager;
        this.errorBuilder = errorBuilder;
    }

    @PostMapping(value = "/responses", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public Mono<ResponseEntity<?>> responses(
            @RequestBody OpenAiResponsesRequest request,
            ServerWebExchange exchange) {

        log.info("[Responses-Controller] 收到请求: model={}, stream={}, input={}",
                request.getModel(), request.getStream(),
                request.getInput() != null ? request.getInput().getClass().getSimpleName() : "null");

        // 绑定协议状态（应该绑定 OPENAI_RESPONSES 而非 CHAT_COMPLETIONS）
        protocolManager.bindProtocol(exchange, ProtocolType.OPENAI_RESPONSES);

        // 与其他接口一致：只有显式 stream=true 才走流式，省略或 stream=false 都按非流式处理
        boolean isStreamRequest = Boolean.TRUE.equals(request.getStream());
        String maskedModel = request.getModel();

        if (isStreamRequest) {
            log.info("[Responses-Controller] 进入流式处理流程");
            return Mono.just(ResponseEntity.ok()
                    .contentType(MediaType.TEXT_EVENT_STREAM)
                    .header("Cache-Control", "no-cache, no-store, must-revalidate")
                    .header("Pragma", "no-cache")
                    .header("Expires", "0")
                    .header("X-Accel-Buffering", "no")
                    .header("Connection", "keep-alive")
                    .body(handleStreamingRequest(request, maskedModel, exchange)));
        } else {
            log.info("[Responses-Controller] 进入非流式处理流程");
            return handleBlockingRequest(request, exchange)
                    .map(resp -> {
                        log.info("[Responses-Controller] 非流式响应完成: id={}, status={}", resp.getId(), resp.getStatus());
                        return ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp);
                    });
        }
    }

    private Mono<OpenAiResponsesResponse> handleBlockingRequest(OpenAiResponsesRequest request, ServerWebExchange exchange) {
        log.info("[Responses-Controller] 开始非流式请求处理");
        return converter.toInternalRequest(request)
                .doOnNext(req -> log.info("[Responses-Controller] 内部请求已构建: model={}, messages={}",
                        req.getModel(), req.getMessages() != null ? req.getMessages().size() : 0))
                .flatMap(internalRequest -> gateway.chat(internalRequest, exchange))
                .map(converter::toExternalResponse)
                .doOnError(e -> log.error("[Responses-Controller] 非流式请求处理失败", e));
    }

    private Flux<ServerSentEvent<Object>> handleStreamingRequest(OpenAiResponsesRequest request, String maskedModelName, ServerWebExchange exchange) {
        log.info("[Responses-Controller] 开始流式请求处理");
        return converter.toInternalRequest(request)
                .doOnNext(req -> log.info("[Responses-Controller] 内部请求已构建: model={}, messages={}",
                        req.getModel(), req.getMessages() != null ? req.getMessages().size() : 0))
                .flatMapMany(internalRequest -> gateway.chatStream(internalRequest, exchange))
                .transform(stream -> converter.toExternalStream(stream, request, maskedModelName))
                .doOnNext(sse -> log.debug("[Responses-Controller] 流式输出chunk: {}", sse.data()))
                .doOnComplete(() -> log.info("[Responses-Controller] 流式输出完成"))
                .doOnError(e -> log.error("[Responses-Controller] 流式请求处理失败", e))
                .onErrorResume(e -> {
                    log.error("[Responses-Controller] 流式错误处理", e);
                    return errorBuilder.streamErrorEvents(e, exchange, false);
                });
    }
}
