package com.llmate.multiprotocol.exception;

import lombok.Getter;

/**
 * LLM 网关统一错误码枚举
 * 每个枚举值包含 code（字符串标识）和 messageTemplate（提示语模板，支持 %s 占位符）
 * HTTP 状态码和协议错误类型的映射由 GlobalExceptionHandler 负责
 */
@Getter
public enum LlmErrorCode {

    // ========== 认证错误 (401) ==========
    AUTH_FAILED("auth_failed", "认证失败：%s"),
    AUTH_INVALID_KEY("auth_invalid_key", "API Key 无效：%s"),
    AUTH_KEY_EXPIRED("auth_key_expired", "API Key 已过期：%s"),
    AUTH_USER_DISABLED("auth_user_disabled", "用户账户已被禁用"),
    AUTH_MODEL_NO_PERMISSION("auth_model_no_permission", "No permission to access model: %s"),

    // ========== 余额错误 (402) ==========
    BALANCE_INSUFFICIENT("balance_insufficient", "Insufficient balance. Required: %s, Available: %s"),
    BALANCE_RESERVE_FAILED("balance_reserve_failed", "Failed to reserve balance"),

    // ========== 模型错误 (400/404) ==========
    MODEL_NOT_FOUND("model_not_found", "请求的模型不存在或未配置对应渠道：%s"),
    MODEL_NOT_SUPPORTED("model_not_supported", "Model %s does not support %s mode"),

    // ========== 任务错误 (404) ==========
    TASK_NOT_FOUND("task_not_found", "视频任务不存在：%s"),

    // ========== 渠道错误 (502) ==========
    CHANNEL_NOT_FOUND("channel_not_found", "Channel not found: %s"),
    CHANNEL_UNAVAILABLE("channel_unavailable", "All channels unavailable for model: %s"),
    CHANNEL_TOKEN_EXHAUSTED("channel_token_exhausted", "Channel token pool exhausted"),
    CHANNEL_NO_DEFAULT("channel_no_default", "No default channel configured for model: %s"),

    // ========== 上游错误 (502/503/504) ==========
    UPSTREAM_UNAVAILABLE("upstream_unavailable", "上游 LLM 服务不可达：%s"),
    PROVIDER_ERROR("provider_error", "Provider '%s' 调用失败：%s"),
    PROVIDER_TIMEOUT("provider_timeout", "Upstream provider timeout"),
    PROVIDER_INVALID_RESPONSE("provider_invalid_response", "Invalid response from provider"),
    SERVICE_UNAVAILABLE("service_unavailable", "服务未配置或不可用：%s"),

    // ========== 请求错误 (400) ==========
    INVALID_REQUEST("invalid_request", "请求参数不合法：%s"),
    RATE_LIMITED("rate_limit_exceeded", "请求频率超限，请稍后重试"),
    IMAGE_DOWNLOAD_FAILED("image_download_failed", "多模态图片远程下载失败：%s"),
    PRICE_NOT_CONFIGURED("price_not_configured", "模型未配置价格，已拒绝本次调用：%s（请联系管理员配置）"),

    // ========== 功能未启用 (400) ==========
    FEATURE_NOT_ENABLED("feature_not_enabled", "%s功能未启用"),

    // ========== 素材错误 (404/409/502) ==========
    MATERIAL_COLLECTION_NOT_FOUND("material_collection_not_found", "素材集合不存在"),
    MATERIAL_NOT_FOUND("material_not_found", "素材不存在"),
    MATERIAL_LIMIT_REACHED("material_limit_reached", "%s"),
    ARK_SERVICE_ERROR("ark_service_error", "方舟素材资产库调用失败：%s"),

    // ========== 计费错误 (500) ==========
    BILLING_PRICE_NOT_FOUND("billing_price_not_found", "Price config not found for model: %s"),
    BILLING_CALCULATION_ERROR("billing_calculation_error", "Billing calculation failed"),

    // ========== 系统错误 (500) ==========
    INTERNAL_ERROR("internal_error", "服务器内部错误");

    /** 错误码字符串标识 */
    private final String code;

    /** 默认错误提示语模板，支持 %s 占位符，由 LlmGatewayException 在构造时格式化 */
    private final String messageTemplate;

    LlmErrorCode(String code, String messageTemplate) {
        this.code = code;
        this.messageTemplate = messageTemplate;
    }

    /**
     * 用给定参数格式化模板。无占位符的枚举值（如 AUTH_FAILED、INTERNAL_ERROR）直接返回原文案。
     */
    public String format(Object... args) {
        return messageTemplate != null && messageTemplate.contains("%s")
                ? String.format(messageTemplate, args)
                : messageTemplate;
    }
}
