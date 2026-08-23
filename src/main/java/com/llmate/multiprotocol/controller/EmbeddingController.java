package com.llmate.multiprotocol.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.dto.embedding.MultimodalEmbeddingRequest;
import com.llmate.multiprotocol.dto.embedding.TextEmbeddingRequest;
import com.llmate.multiprotocol.service.EmbeddingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 向量接口控制器（MVC 分层：仅 HTTP 层，业务逻辑在 {@link EmbeddingService}）
 *
 * - POST /v1/embeddings/text_embeddings        文本向量
 * - POST /v1/embeddings/multimodal_embeddings  多模态向量（文本+图片+视频）
 *
 * 响应为上游 OpenAI 兼容 JSON 透传。
 */
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Log4j2
@RequireApiKey
public class EmbeddingController {

    private final EmbeddingService embeddingService;

    /**
     * 文本向量
     */
    @PostMapping("/embeddings/text_embeddings")
    public Mono<JsonNode> textEmbeddings(@RequestBody TextEmbeddingRequest request, ServerWebExchange exchange) {
        log.info("[Embedding-Controller] 文本向量请求: model={}", request.getModel());
        return embeddingService.textEmbeddings(request, exchange);
    }

    /**
     * 多模态向量
     */
    @PostMapping("/embeddings/multimodal_embeddings")
    public Mono<JsonNode> multimodalEmbeddings(@RequestBody MultimodalEmbeddingRequest request, ServerWebExchange exchange) {
        log.info("[Embedding-Controller] 多模态向量请求: model={}, contents={}",
                request.getModel(),
                request.getInput() != null && request.getInput().getContents() != null
                        ? request.getInput().getContents().size() : 0);
        return embeddingService.multimodalEmbeddings(request, exchange);
    }
}
