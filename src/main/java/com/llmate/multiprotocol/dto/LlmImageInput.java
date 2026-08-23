package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 图像编辑的输入图
 * url 与 base64Data 二选一：
 * - url：可直接访问的图片地址（转发给上游或由上游抓取）
 * - base64Data + mimeType：图片字节的 base64（不含 data: 前缀），由适配器组装成 data URI
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmImageInput {

    /** 图片 URL（外部可访问） */
    private String url;

    /** 图片字节 base64（不含 data: 前缀） */
    private String base64Data;

    /** MIME 类型，如 image/png / image/jpeg */
    private String mimeType;
}
