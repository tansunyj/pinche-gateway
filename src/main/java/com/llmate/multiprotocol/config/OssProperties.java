package com.llmate.multiprotocol.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * OSS 配置属性（前缀 oss）
 * 对应 application.yml 的 oss.* 配置块，照老项目 gateway.oss 迁移
 */
@Data
@Component
@ConfigurationProperties(prefix = "oss")
public class OssProperties {

    /** OSS 区域，如 oss-cn-shenzhen */
    private String region = "oss-cn-shenzhen";

    /** Bucket 名称 */
    private String bucket = "numspirit-media";

    /** AccessKey ID */
    private String accessKeyId;

    /** AccessKey Secret */
    private String accessKeySecret;

    /** CDN 基础地址（预留，签名 URL 目前直接用 OSS 原始域名） */
    private String cdnBaseUrl = "https://numspirit-media.oss-cn-shenzhen.aliyuncs.com";

    /** 签名 URL 有效期（秒） */
    private int signTtlSeconds = 3600;
}
