package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 符合 OpenAI 标准的图像生成/编辑响应体
 * 同时复用为上游 OpenAI 兼容渠道的响应解析 DTO。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OpenAiImageResponse {

    private Long created;

    private List<ImageData> data;

    private OpenAiImageUsage usage;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ImageData {
        @JsonProperty("b64_json")
        private String b64Json;
        private String url;
        @JsonProperty("revised_prompt")
        private String revisedPrompt;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpenAiImageUsage {
        @JsonProperty("total_tokens")
        private Long totalTokens;
        @JsonProperty("input_tokens")
        private Long inputTokens;
        @JsonProperty("output_tokens")
        private Long outputTokens;
        @JsonProperty("input_tokens_details")
        private InputTokensDetails inputTokensDetails;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class InputTokensDetails {
        @JsonProperty("text_tokens")
        private Long textTokens;
        @JsonProperty("image_tokens")
        private Long imageTokens;
    }
}
