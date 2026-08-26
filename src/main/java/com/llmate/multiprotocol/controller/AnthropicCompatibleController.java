package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.config.GatewayErrorResponseBuilder;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.converter.AnthropicProtocolConverter;
import com.llmate.multiprotocol.dto.PreparedStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesRequest;
import com.llmate.multiprotocol.dto.anthropic.AnthropicMessagesResponse;
import com.llmate.multiprotocol.dto.anthropic.CountTokensResponse;
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
            String maskedModel = request.getModel();
            // 预飞先行：模型路由 / 价格查询 / 余额预占 在 SSE 响应头发出【之前】完成。
            // 余额不足等 setup 错误直接在 Mono 上失败 → GlobalExceptionHandler 返回普通 HTTP 错误，
            // 而不是 200 + SSE error 事件；只有流已提交后的失败才走 SSE error。
            return converter.toInternalRequest(request)
                    .flatMap(internalRequest -> gateway.prepareStream(internalRequest, exchange))
                    .map(prepared -> ResponseEntity.ok()
                            .contentType(MediaType.TEXT_EVENT_STREAM)
                            .header("Cache-Control", "no-cache, no-store, must-revalidate")
                            .header("Pragma", "no-cache")
                            .header("Expires", "0")
                            .header("X-Accel-Buffering", "no")
                            .header("Connection", "keep-alive")
                            .body(toSseStream(prepared, request, maskedModel, exchange)));
        } else {
            log.info("[Anthropic-Controller] 进入非流式处理流程");
            return handleBlockingRequest(request, exchange)
                    .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp));
        }
    }

    /**
     * Claude Code 的 POST /v1/messages/count_tokens：估算输入 tokens。
     * 网关此前未实现该接口 → 404 → Claude Code 视为失败高频重试产生大量 429。
     * 新增后把原始请求体透传上游，返回 {"input_tokens": N}；纯估算不参与计费。
     */
    @PostMapping(value = "/messages/count_tokens", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<CountTokensResponse>> countTokens(
            @RequestBody JsonNode request,
            ServerWebExchange exchange) {

        protocolManager.bindProtocol(exchange, ProtocolType.ANTHROPIC_MESSAGES);
        String model = request != null && request.path("model").isTextual() ? request.path("model").asText() : null;
        log.info("[Anthropic-Controller] 收到 count_tokens 请求: model={}", model);
        return gateway.countTokens(model, request, exchange)
                .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp));
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
     * 流式输出（SSE 响应已提交后）：把预飞好的上游流转成 Anthropic SSE 事件。
     * 此处的失败（上游中途报错）已进入 SSE 流，统一转流内 error 事件；
     * setup 阶段（余额预占等）的错误已被 prepareStream 挡在响应提交前，不会走到这里。
     */
    private Flux<ServerSentEvent<Object>> toSseStream(PreparedStream prepared, AnthropicMessagesRequest request, String maskedModelName, ServerWebExchange exchange) {
        log.info("[Anthropic-Controller] 开始流式输出");
        return prepared.getFlux()
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
