package com.llmate.multiprotocol.dto.embedding;

import lombok.Data;

import java.util.List;

/**
 * 文本向量请求（照老项目 EmbeddingController.EmbeddingRequest）
 * POST /v1/embeddings/text_embeddings
 * {
 *   "model": "text-embedding-v4"（可带渠道前缀，如 aliyun/text-embedding-v4）,
 *   "input": { "texts": ["文本1", "文本2"] }
 * }
 */
@Data
public class TextEmbeddingRequest {

    private String model;

    private TextInput input;

    @Data
    public static class TextInput {
        private List<String> texts;
    }
}
