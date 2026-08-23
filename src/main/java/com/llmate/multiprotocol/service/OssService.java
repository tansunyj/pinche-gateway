package com.llmate.multiprotocol.service;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GeneratePresignedUrlRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.llmate.multiprotocol.config.OssProperties;
import com.llmate.multiprotocol.dto.upload.UploadResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.YearMonth;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OSS 文件上传服务（MVC 分层：Controller 只做 HTTP，业务逻辑在此）
 *
 * 阿里云 OSS SDK 是阻塞调用，所有 OSS 操作统一包到 {@link Schedulers#boundedElastic()}，
 * 避免阻塞 Netty 事件循环线程。
 *
 * 照老项目 OssServiceImpl 迁移：
 * - OSS key 路径：users/{userId}/uploads/{YYYY-MM}/{uuid}.{ext}
 * - 返回签名 URL（TTL 由配置 oss.sign-ttl-seconds 决定，追加 response-content-disposition=inline）
 * - 上传不落库（老项目行为）
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class OssService {

    private final OSS ossClient;
    private final OssProperties ossProperties;

    // MIME 类型到扩展名的映射（照老项目）
    private static final Map<String, String> MIME_EXT_MAP = new ConcurrentHashMap<>();

    static {
        // 图片
        MIME_EXT_MAP.put("image/png", "png");
        MIME_EXT_MAP.put("image/jpeg", "jpg");
        MIME_EXT_MAP.put("image/jpg", "jpg");
        MIME_EXT_MAP.put("image/webp", "webp");
        MIME_EXT_MAP.put("image/gif", "gif");
        // 音频
        MIME_EXT_MAP.put("audio/mpeg", "mp3");
        MIME_EXT_MAP.put("audio/wav", "wav");
        MIME_EXT_MAP.put("audio/mp3", "mp3");
        MIME_EXT_MAP.put("audio/aac", "aac");
        MIME_EXT_MAP.put("audio/ogg", "ogg");
        MIME_EXT_MAP.put("audio/flac", "flac");
        MIME_EXT_MAP.put("audio/x-wav", "wav");
        MIME_EXT_MAP.put("audio/x-m4a", "m4a");
        // 视频
        MIME_EXT_MAP.put("video/mp4", "mp4");
        MIME_EXT_MAP.put("video/webm", "webm");
        MIME_EXT_MAP.put("video/quicktime", "mov");
    }

    /**
     * 上传字节数组到 OSS（供图像适配器把 HTTP 上传的 base64 图片转成 URL 传给上游渠道），
     * 返回签名 URL + ossKey。阻塞上传包在 boundedElastic 上执行。
     */
    public Mono<UploadResult> uploadBytes(byte[] data, String mime, Long userId) {
        if (data == null || data.length == 0) {
            return Mono.empty();
        }
        String ext = extFromMime(mime);
        String ossKey = buildKey(userId, ext);
        return Mono.fromCallable(() -> {
            ObjectMetadata meta = new ObjectMetadata();
            if (mime != null) {
                meta.setContentType(mime);
            }
            meta.setContentLength(data.length);
            try (InputStream in = new ByteArrayInputStream(data)) {
                ossClient.putObject(ossProperties.getBucket(), ossKey, in, meta);
            }
            log.info("[OssService] 上传字节成功: ossKey={}, size={}, mime={}", ossKey, data.length, mime);
            return UploadResult.builder()
                    .url(buildSignedUrl(ossKey, ossProperties.getSignTtlSeconds()))
                    .ossKey(ossKey)
                    .mime(mime)
                    .size((long) data.length)
                    .build();
        }).subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 生成 OSS 对象签名 URL（阻塞，boundedElastic 上执行，TTL 用配置默认值）
     */
    public Mono<String> getSignedUrl(String ossKey) {
        return Mono.fromCallable(() -> buildSignedUrl(ossKey, ossProperties.getSignTtlSeconds()))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 生成 OSS 对象签名 URL（自定义 TTL 秒，阻塞，boundedElastic 上执行）。
     * 素材上传用：方舟 CreateAsset 异步拉取 URL 需要更长有效期（24h），不能用默认 1h。
     */
    public Mono<String> getSignedUrl(String ossKey, int ttlSeconds) {
        return Mono.fromCallable(() -> buildSignedUrl(ossKey, ttlSeconds))
            .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 删除 OSS 对象（阻塞，boundedElastic 上执行）
     */
    public Mono<Void> deleteFile(String ossKey) {
        return Mono.fromRunnable(() -> {
            ossClient.deleteObject(ossProperties.getBucket(), ossKey);
            log.info("[OssService] 删除成功: ossKey={}", ossKey);
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    // ==================== 内部工具方法 ====================

    /**
     * 构建 OSS 对象 key：users/{userId}/uploads/{YYYY-MM}/{uuid}.{ext}
     */
    private String buildKey(Long userId, String ext) {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        String ym = YearMonth.now().toString();
        return String.format("users/%d/uploads/%s/%s.%s", userId, ym, uuid, ext);
    }

    /**
     * 根据 MIME 类型获取文件扩展名，未知类型 fallback 为 bin
     */
    private String extFromMime(String mime) {
        if (mime == null || mime.isEmpty()) {
            return "bin";
        }
        String ext = MIME_EXT_MAP.get(mime.toLowerCase());
        return ext != null ? ext : "bin";
    }

    /**
     * 生成签名 URL（TTL 秒 + response-content-disposition=inline）
     */
    private String buildSignedUrl(String ossKey, int ttlSeconds) {
        Date expiration = new Date(System.currentTimeMillis() + ttlSeconds * 1000L);
        GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(ossProperties.getBucket(), ossKey);
        request.setExpiration(expiration);
        request.setProcess("response-content-disposition=inline");
        return ossClient.generatePresignedUrl(request).toString();
    }
}
