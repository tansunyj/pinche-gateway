package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 图像生成/编辑输出结果
 * b64Json 与 url 至少一个非空（取决于渠道与客户端请求的 response_format）。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmImage {

    /** base64 图片数据（b64_json） */
    private String b64Json;

    /** 图片 URL */
    private String url;

    /** 上游修订后的提示词（部分渠道返回） */
    private String revisedPrompt;

    /** 图片内容类型，如 image/png（Gemini inlineData 的 mimeType） */
    private String contentType;

    /** 结果序号（多图时从 0 开始） */
    private Integer index;
}
