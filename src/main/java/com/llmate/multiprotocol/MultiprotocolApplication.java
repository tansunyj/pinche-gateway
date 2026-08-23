package com.llmate.multiprotocol;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.util.TimeZone;

@SpringBootApplication
@ComponentScan(basePackages = {"com.llmate", "com.silievo.gateway"})
public class MultiprotocolApplication {

    public static void main(String[] args) {
        // 设置全局默认时区为东八区（Asia/Shanghai）
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        SpringApplication.run(MultiprotocolApplication.class, args);
    }
}
