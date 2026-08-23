package com.llmate.multiprotocol.config;

import io.r2dbc.spi.ConnectionFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

/**
 * R2DBC 配置类
 */
@Configuration
@EnableR2dbcRepositories(basePackages = "com.llmate.multiprotocol.repository")
public class R2dbcConfig extends AbstractR2dbcConfiguration {

    @Override
    public ConnectionFactory connectionFactory() {
        // 使用 spring.r2dbc 配置，这里不需要额外配置
        return null;
    }
}
