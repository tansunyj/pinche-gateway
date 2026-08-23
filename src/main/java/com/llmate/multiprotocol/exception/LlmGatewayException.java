package com.llmate.multiprotocol.exception;

import lombok.Getter;

/**
 * LLM 网关统一业务异常
 * 携带 LlmErrorCode 错误码，由 GlobalExceptionHandler 统一捕获并按协议格式化
 *
 * 构造方式：
 * - new LlmGatewayException(LlmErrorCode.AUTH_FAILED)            // 用枚举默认文案
 * - new LlmGatewayException(LlmErrorCode.MODEL_NOT_FOUND, "gpt-4") // 用枚举模板 + 单参数自动格式化
 * - new LlmGatewayException(LlmErrorCode.PROVIDER_ERROR, "dashscope", "timeout") // 多参数
 * - new LlmGatewayException(LlmErrorCode.UPSTREAM_UNAVAILABLE, "message", ex)   // 带原始异常链
 */
@Getter
public class LlmGatewayException extends RuntimeException {

    /** 错误码枚举 */
    private final LlmErrorCode errorCode;

    /**
     * 使用枚举默认 message（无占位符）构造异常
     */
    public LlmGatewayException(LlmErrorCode errorCode) {
        super(errorCode.getMessageTemplate());
        this.errorCode = errorCode;
    }

    /**
     * 使用枚举模板 + 动态参数构造异常（自动格式化）。
     * 适用于枚举模板含 %s 占位符的场景。
     */
    public LlmGatewayException(LlmErrorCode errorCode, Object... args) {
        super(errorCode.format(args));
        this.errorCode = errorCode;
    }

    /**
     * 使用错误码 + 已拼接好的完整 message 构造异常（覆盖枚举默认提示）。
     * 适用于需要完全自定义文案、不依赖模板的场景。
     */
    public LlmGatewayException(LlmErrorCode errorCode, String detailMessage) {
        super(errorCode.format(detailMessage));
        this.errorCode = errorCode;
    }

    /**
     * 使用错误码 + 已拼接好的完整 message + 原始异常链构造异常
     */
    public LlmGatewayException(LlmErrorCode errorCode, String detailMessage, Throwable cause) {
        super(detailMessage, cause);
        this.errorCode = errorCode;
    }
}
