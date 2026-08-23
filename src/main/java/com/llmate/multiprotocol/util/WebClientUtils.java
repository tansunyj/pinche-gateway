package com.llmate.multiprotocol.util;

import com.llmate.multiprotocol.constant.SystemConstants;
import io.netty.handler.ssl.SslContextBuilder;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.tcp.SslProvider;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * WebClient / HttpClient 统一构建工具
 *
 * 全项目所有 HTTP 出站调用（渠道适配器、SSE、向量、消息图片下载等）统一从这里构建，
 * 避免各处重复拼 ConnectionProvider.newConnection() 样板。
 *
 * 核心约定：全部使用 {@link ConnectionProvider#newConnection()} 禁用连接池，每次请求新建
 * TCP+TLS 连接。原因：国内生产服务器出站经阿里云 NAT，NAT 空闲超时后会断开池中旧连接，
 * Reactor Netty 默认 DefaultPooledConnectionProvider 不知情，拿出来复用 → Connection reset
 * by peer。newConnection() 无连接复用开销可接受（LLM/向量请求本身 latency 远大于握手）。
 */
public final class WebClientUtils {

    private WebClientUtils() {
    }

    /**
     * 构建禁用连接池的 HttpClient（NAT 断连规避）。
     *
     * @param responseTimeout 响应超时
     */
    public static HttpClient newConnHttpClient(Duration responseTimeout) {
        return HttpClient.create(ConnectionProvider.newConnection())
                .responseTimeout(responseTimeout);
    }

    /**
     * 构建禁用连接池的 HttpClient，并设置是否跟随重定向。
     * 图片 URL 下载必须跟随 301/302（图床会跳到 CDN），否则拿到空 body → base64 空串。
     */
    public static HttpClient newConnHttpClient(Duration responseTimeout, boolean followRedirect) {
        return newConnHttpClient(responseTimeout).followRedirect(followRedirect);
    }

    /**
     * 构建禁用连接池且放宽 SSL 握手超时的 HttpClient（Anthropic/Vertex 用）。
     * 部分上游（api.vapeur.ai）TLS 握手偶发超过 Netty 默认 10s，放宽到 30s 避免误判失败。
     */
    public static HttpClient newConnHttpClient(Duration responseTimeout, Duration sslHandshakeTimeout) {
        HttpClient client = newConnHttpClient(responseTimeout);
        try {
            client = client.secure(SslProvider.builder()
                    .sslContext(SslContextBuilder.forClient().build())
                    .handshakeTimeout(sslHandshakeTimeout)
                    .build());
        } catch (Exception e) {
            // 握手超时配置失败时退化默认，不阻塞构建
        }
        return client;
    }

    /**
     * 共享的图片 URL → base64 下载客户端（独立无鉴权，避免把渠道 key 发给图片主机）。
     * 跟随重定向 + 5 分钟响应超时。DashScope/Gemini/OpenAiImage 三个图像适配器共用。
     */
    private static final WebClient IMAGE_DOWNLOAD_CLIENT = WebClient.builder()
            .clientConnector(new ReactorClientHttpConnector(
                    newConnHttpClient(
                            Duration.ofSeconds(SystemConstants.HTTP_TIMEOUT_IMAGE_DOWNLOAD_SECONDS),
                            true)))
            .build();

    /**
     * 获取共享图片下载客户端。
     */
    public static WebClient imageDownloadClient() {
        return IMAGE_DOWNLOAD_CLIENT;
    }

    /**
     * 图片 URL 下载前的编码修复（生图参考图 / 对话图片 / 视频封面等所有 URL 下载共用）。
     *
     * <p>坑：客户端上传的 URL 常已含百分号编码（如文件名中文被编码成 {@code %XX}），
     * WebClient 的 UriBuilder 会对整个 URL 重新编码，把已有的 {@code %} 二次编码成 {@code %25}，
     * 导致图片地址 404/502（实测：{@code %25XX} 是 {@code %XX} 的双编码）。
     * 解决：下载前先对 URL 做一次 percent-decode（保留 {@code +}，避免 query 里的加号被解成空格），
     * 还原成未编码形态，再交给 UriBuilder 统一编码一次。
     *
     * @param url 客户端传入的图片 URL
     * @return 已还原为单次编码语义的 URL；null 原样返回
     */
    public static String decodeImageUrl(String url) {
        if (url == null) {
            return null;
        }
        try {
            // 先把 + 保护起来（URLDecoder 会把 + 解成空格，图片 query 中的 + 通常是有意义的加号）
            String guarded = url.replace("+", "%2B");
            return URLDecoder.decode(guarded, StandardCharsets.UTF_8);
        } catch (Exception e) {
            // 解码失败（罕见），原样透传，下载失败时由调用方兜底
            return url;
        }
    }

    /**
     * 根据 URL 扩展名推断图片 MIME 类型（下载兜底用）。
     * 上游未返回 Content-Type 时的保守推断；未知扩展名默认 image/jpeg。
     */
    public static String detectImageMimeType(String url) {
        if (url == null) {
            return "image/jpeg";
        }
        String lower = url.toLowerCase();
        if (lower.contains(".png")) {
            return "image/png";
        }
        if (lower.contains(".webp")) {
            return "image/webp";
        }
        if (lower.contains(".gif")) {
            return "image/gif";
        }
        if (lower.contains(".bmp")) {
            return "image/bmp";
        }
        if (lower.contains(".svg")) {
            return "image/svg+xml";
        }
        return "image/jpeg";
    }
}