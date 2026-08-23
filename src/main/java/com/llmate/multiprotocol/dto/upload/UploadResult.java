package com.llmate.multiprotocol.dto.upload;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 单个文件上传结果
 * 对应老项目响应中的 data 对象：{ url, oss_key, mime, size }
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UploadResult {

    /** OSS 签名 URL（临时有效） */
    private String url;

    /** OSS 对象 key */
    @JsonProperty("oss_key")
    private String ossKey;

    /** MIME 类型 */
    private String mime;

    /** 文件大小（字节） */
    private Long size;
}
