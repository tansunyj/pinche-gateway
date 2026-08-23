package com.llmate.multiprotocol.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * API Key 认证注解
 * 标注在 Controller 方法上表示需要 API Key 认证
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
public @interface RequireApiKey {
    /**
     * 是否必需，默认为 true
     */
    boolean required() default true;
}
