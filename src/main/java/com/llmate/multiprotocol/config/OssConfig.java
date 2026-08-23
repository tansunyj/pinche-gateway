package com.llmate.multiprotocol.config;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * OSS 客户端配置
 * 生产单例 {@link OSS} bean，上下文关闭时通过 destroyMethod="shutdown" 释放连接
 */
@Configuration
@Log4j2
public class OssConfig {

    @Bean(destroyMethod = "shutdown")
    public OSS ossClient(OssProperties props) {
        log.info("[OssConfig] 初始化 OSS 客户端: region={}, bucket={}", props.getRegion(), props.getBucket());
        return new OSSClientBuilder().build(
                "https://" + props.getRegion() + ".aliyuncs.com",
                props.getAccessKeyId(),
                props.getAccessKeySecret());
    }
}
