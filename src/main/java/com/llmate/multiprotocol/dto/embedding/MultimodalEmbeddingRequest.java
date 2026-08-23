package com.llmate.multiprotocol.dto.embedding;

import lombok.Data;

import java.util.List;

/**
 * 多模态向量请求（照老项目 MultimodalEmbeddingController.MultimodalEmbeddingRequest）
 * POST /v1/embeddings/multimodal_embeddings
 * {
 *   "model": "qwen3-vl-embedding",
 *   "input": { "contents": [ { "text": "...", "image": "...", "video": "..." } ] },
 *   "parameters": { "enableFusion": true }
 * }
 */
@Data
public class MultimodalEmbeddingRequest {

    private String model;

    private MultimodalInput input;

    private MultimodalParameters parameters;

    @Data
    public static class MultimodalInput {
        private List<ContentItem> contents;
    }

    @Data
    public static class ContentItem {
        private String text;
        private String image;
        private String video;
    }

    @Data
    public static class MultimodalParameters {
        private Boolean enableFusion = true;
    }
}
