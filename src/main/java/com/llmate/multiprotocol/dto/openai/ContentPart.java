package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 符合 OpenAI 规范的多模态内容块定义
 * 对应请求中 "content": [ {"type": "text", "text": "..."}, {"type": "image_url", "image_url": {...}} ]
 * 兼容：部分客户端对 chat 入口直接发 Anthropic 风格 {"type": "image", "source": {...}} 块，见 {@link #source}。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ContentPart {

    /**
     * 内容类型: "text" (纯文本) / "image_url" (OpenAI 标准多模态图片) / "image" (Anthropic 风格块)
     */
    private String type;

    /**
     * 当 type 为 "text" 时，具体的文本内容
     */
    private String text;

    /**
     * 当 type 为 "image_url" 时，包含的图片链接详情对象
     */
    @JsonProperty("image_url")
    private ImageUrl imageUrl;

    /**
     * 当 type 为 "image"（Anthropic 风格块）时，包含的 source 结构：
     * {type: base64, media_type, data} 或 {type: url, url} 或 {type: file, file_id}（file 暂不支持）。
     * chat 入口兼容部分客户端直接发这种块；OpenAI 标准仍用 image_url。
     */
    @JsonProperty("source")
    private Map<String, Object> source;

    /**
     * 图片 URL 内部包装类
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ImageUrl {
        /**
         * 图片的远程网络 HTTP 链接，或者是 Base64 编码的 Data URI (例如 data:image/png;base64,xxx)
         */
        private String url;

        /**
         * 图片细节识别质量控规: "auto", "low", "high" (OpenAI 特有可选字段)
         */
        @Builder.Default
        private String detail = "auto";
    }
}