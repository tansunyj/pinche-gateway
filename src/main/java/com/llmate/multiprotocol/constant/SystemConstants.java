package com.llmate.multiprotocol.constant;

import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * 系统级常量
 * 与业务逻辑无关的底层系统常量
 */
@UtilityClass
public class SystemConstants {

    // ==================== WebFlux Context Key ====================
    public static final String CONTEXT_PROTOCOL_KEY = "LLMATE_PROTOCOL_TYPE";
    public static final String CONTEXT_USER_ID_KEY = "userId";
    public static final String CONTEXT_TOKEN_ID_KEY = "tokenId";
    public static final String CONTEXT_TOKEN_ENTITY_KEY = "tokenEntity";
    public static final String CONTEXT_REQUEST_ID_KEY = "requestId";

    // ==================== 负载均衡策略 ====================
    public static final String LB_STRATEGY_ROUND_ROBIN = "round_robin";
    public static final String LB_STRATEGY_RANDOM = "random";
    public static final String LB_STRATEGY_LEAST_USED = "least_used";
    public static final String LB_STRATEGY_DEFAULT = LB_STRATEGY_ROUND_ROBIN;

    // ==================== 状态码 ====================
    public static final int STATUS_ENABLED = 1;
    public static final int STATUS_DISABLED = 0;

    // ==================== 用户状态（pt_users.status 枚举） ====================
    public static final String USER_STATUS_ACTIVE = "ACTIVE";
    public static final String USER_STATUS_DISABLED = "DISABLED";

    // ==================== HTTP Header ====================
    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String HEADER_REQUEST_ID = "X-Request-Id";
    public static final String BEARER_PREFIX = "Bearer ";

    // ==================== HTTP 超时 ====================
    /**
     * 上游渠道调用响应超时：10 分钟。
     * 适用于所有 LLM/SSE/向量等上游调用。LLM 推理、SSE 长流、向量大输入都可能耗时较长，
     * 超时设置必须留足余量，太短会误杀正常慢请求（历史踩坑：图片/上游超时过短导致随机失败）。
     */
    public static final long HTTP_TIMEOUT_UPSTREAM_SECONDS = 600;

    /**
     * 外部图片 URL 下载转 base64 的响应超时：5 分钟。
     * 图床/OSS 大图下载可能较慢，统一用 5 分钟（与图片下载客户端一致）。
     */
    public static final long HTTP_TIMEOUT_IMAGE_DOWNLOAD_SECONDS = 300;

    /**
     * TLS 握手超时：60 秒。
     * 部分上游（如 api.vapeur.ai）TLS 握手偶发超过 Netty 默认 10s，过短会把正常请求误判为
     * SslHandshakeTimeoutException 失败。统一放宽到 60s。
     */
    public static final long HTTP_TIMEOUT_SSL_HANDSHAKE_SECONDS = 60;

    /**
     * TCP 建连超时：30 秒。
     * 上游网络抖动/慢启动时建连可能较慢，过短会误报连接失败。
     */
    public static final long HTTP_TIMEOUT_CONNECT_SECONDS = 30;

    // ==================== 模型可见性 ====================
    public static final String VISIBILITY_PUBLIC = "public";
    public static final String VISIBILITY_PRIVATE = "private";
    public static final String VISIBILITY_INTERNAL = "internal";

    // ==================== 模型类型 ====================
    public static final String MODEL_TYPE_CHAT = "chat";
    public static final String MODEL_TYPE_IMAGE = "image";
    public static final String MODEL_TYPE_VIDEO = "video";
    public static final String MODEL_TYPE_AUDIO = "audio";
    public static final String MODEL_TYPE_EMBEDDING = "embedding";
}
