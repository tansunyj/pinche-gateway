package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.config.GatewayErrorResponseBuilder;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.converter.upstream.VertexFormatConverter;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentRequest;
import com.llmate.multiprotocol.dto.vertex.VertexGenerateContentResponse;
import com.llmate.multiprotocol.engine.LlmGateway;
import com.llmate.multiprotocol.engine.ProtocolManager;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Vertex AI / Gemini 企业版 API 兼容 Controller
 *
 * 统一拦截 /v1beta/models/** 下的所有请求，根据路径后缀自动识别流式与非流式
 */
@RestController
@RequestMapping("/v1beta/models")
@RequireApiKey
@Log4j2
public class VertexCompatibleController {

    private final LlmGateway gateway;
    private final ProtocolManager protocolManager;
    private final VertexFormatConverter vertexFormatConverter;
    private final GatewayErrorResponseBuilder errorBuilder;

    public VertexCompatibleController(
            LlmGateway gateway,
            ProtocolManager protocolManager,
            VertexFormatConverter vertexFormatConverter,
            GatewayErrorResponseBuilder errorBuilder) {
        this.gateway = gateway;
        this.protocolManager = protocolManager;
        this.vertexFormatConverter = vertexFormatConverter;
        this.errorBuilder = errorBuilder;
    }

    /**
     * 统一入口：拦截 /v1beta/models 下的所有 POST 请求
     * 通过内部对路径后缀（generateContent vs streamGenerateContent）的判断来自动分流
     */
    @PostMapping(value = "/**", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_EVENT_STREAM_VALUE})
    public Object handleVertexRequest(
            @RequestBody VertexGenerateContentRequest request,
            ServerWebExchange exchange) {

        String rawPath = exchange.getRequest().getURI().getPath();
        if (rawPath == null) {
            return Mono.error(new IllegalArgumentException("Request path cannot be null"));
        }

        // 1. 判断是否为流式请求
        boolean isStream = rawPath.endsWith("/streamGenerateContent") || rawPath.endsWith(":streamGenerateContent");
        // 2. 判断是否为非流式请求
        boolean isNormal = rawPath.endsWith("/generateContent") || rawPath.endsWith(":generateContent");

        if (!isStream && !isNormal) {
            return Mono.error(new IllegalArgumentException("Invalid Vertex API path: " + rawPath));
        }

        // 提取模型路径
        String suffix = isStream
                ? (rawPath.endsWith(":streamGenerateContent") ? ":streamGenerateContent" : "/streamGenerateContent")
                : (rawPath.endsWith(":generateContent") ? ":generateContent" : "/generateContent");

        String modelPath = extractModelPath(rawPath, suffix);
        if (modelPath == null || modelPath.isEmpty()) {
            return Mono.error(new IllegalArgumentException("Failed to extract modelPath from: " + rawPath));
        }

        protocolManager.bindProtocol(exchange, ProtocolType.GOOGLE_GEMINI);
        LlmChatRequest internalReq = vertexFormatConverter.toInternalRequest(request, modelPath);

        // 分流处理
        if (isStream) {
            log.info("[Vertex-Controller] 识别为【流式】请求: modelPath={}", modelPath);

            // 直接通过 ServerWebExchange 设置流式响应头，确保不会被网关或Nginx缓存
            org.springframework.http.server.reactive.ServerHttpResponse response = exchange.getResponse();
            response.getHeaders().set(HttpHeaders.CONTENT_TYPE, MediaType.TEXT_EVENT_STREAM_VALUE);
            response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-cache, no-store, must-revalidate");
            response.getHeaders().set("Pragma", "no-cache");
            response.getHeaders().set("Expires", "0");
            response.getHeaders().set("X-Accel-Buffering", "no");
            response.getHeaders().set(HttpHeaders.CONNECTION, "keep-alive");

            // 直接返回 Flux，WebFlux 会以 Chunked 方式实时向客户端推送数据
            return handleStreamingRequest(internalReq, modelPath, exchange);
        } else {
            log.info("[Vertex-Controller] 识别为【非流式】请求: modelPath={}", modelPath);
            return handleBlockingRequest(internalReq, modelPath, exchange);
        }
    }

    /**
     * 阻塞/非流式请求处理
     */
    private Mono<ResponseEntity<VertexGenerateContentResponse>> handleBlockingRequest(LlmChatRequest internalReq, String modelPath, ServerWebExchange exchange) {
        log.info("[Vertex-Controller] 开始非流式请求处理");
        return gateway.chat(internalReq, exchange)
                .map(resp -> {
                    VertexGenerateContentResponse vertexResp = vertexFormatConverter.toVertexResponse(resp);
                    return ResponseEntity.ok(vertexResp);
                })
                .doOnSuccess(resp -> log.info("[Vertex-Controller] 非流式响应完成: modelPath={}", modelPath))
                .doOnError(e -> log.error("[Vertex-Controller] 非流式请求失败: modelPath={}", modelPath, e));
    }

    /**
     * 流式请求处理
     * 直接返回 Flux<ServerSentEvent<Object>>，由 WebFlux 驱动按需流式下发
     */
    private Flux<ServerSentEvent<Object>> handleStreamingRequest(LlmChatRequest internalReq, String modelPath, ServerWebExchange exchange) {
        log.info("[Vertex-Controller] 开始流式请求处理");
        return gateway.chatStream(internalReq, exchange)
                .doOnSubscribe(sub -> log.info("[Vertex-Controller] 开始订阅 gateway 流"))
                .doOnNext(chunk -> log.info("[Vertex-Controller] 收到内部chunk: deltaContent={}, finished={}",
                        chunk.getDeltaContent(), chunk.isFinished()))
                .map(chunk -> {
                    VertexGenerateContentResponse vertexChunk = vertexFormatConverter.toVertexStreamChunk(chunk);
                    return ServerSentEvent.builder()
                            .data(vertexChunk)
                            .build();
                })
                .concatWith(Flux.just(
                        ServerSentEvent.builder()
                                .data("[DONE]")
                                .build()
                ))
                .doOnNext(sse -> log.debug("[Vertex-Controller] 流式输出SSE: data={}", sse.data()))
                .doOnComplete(() -> log.info("[Vertex-Controller] 流式响应完成: modelPath={}", modelPath))
                .doOnError(e -> log.error("[Vertex-Controller] 流式请求失败: modelPath={}", modelPath, e))
                .onErrorResume(e -> {
                    log.error("[Vertex-Controller] 流式错误处理: modelPath={}", modelPath, e);
                    return errorBuilder.streamErrorEvents(e, exchange, false);
                });
    }

    /**
     * 从完整路径中精准剥离并提取出中间的 modelPath
     */
    private String extractModelPath(String rawPath, String suffix) {
        String basePrefix = "/v1beta/models/";
        if (rawPath.startsWith(basePrefix) && rawPath.endsWith(suffix)) {
            int startIndex = basePrefix.length();
            int endIndex = rawPath.length() - suffix.length();
            if (endIndex > startIndex) {
                return rawPath.substring(startIndex, endIndex);
            }
        }
        return null;
    }
}