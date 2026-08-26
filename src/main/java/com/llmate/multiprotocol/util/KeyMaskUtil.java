package com.llmate.multiprotocol.util;

/**
 * API Key 日志脱敏工具。
 *
 * 打印用户/渠道 API KEY 时只保留首尾若干字符，中间以星号遮罩，绝不打印完整 key，
 * 方便在日志中按 requestId/tokenId 定位问题又不泄露凭据。
 */
public final class KeyMaskUtil {

    /** 头部保留字符数（尽量带全 sk-xxx- 前缀，如 sk-silievo-） */
    private static final int HEAD_KEEP = 12;
    /** 尾部保留字符数 */
    private static final int TAIL_KEEP = 6;
    /** 长度不足该值时整段全遮（连 HEAD+TAIL+4 个星号都放不下就没必要露了） */
    private static final int MIN_MASKABLE = HEAD_KEEP + TAIL_KEEP + 4;

    private KeyMaskUtil() {
    }

    /**
     * 遮罩 API Key：首留 HEAD_KEEP 尾留 TAIL_KEEP 字符，中间以 **** 填充。
     * null / 空串 → "null"；过短 → "****"。
     * 例：sk-silievo-2c4b76e3bbe34eb31621066e222bd8fd → sk-silievo-2c4****2bd8fd
     */
    public static String mask(String key) {
        if (key == null || key.isEmpty()) {
            return "null";
        }
        if (key.length() <= MIN_MASKABLE) {
            return "****";
        }
        return key.substring(0, HEAD_KEEP) + "****" + key.substring(key.length() - TAIL_KEEP);
    }

    /**
     * 把「用户 API Key」与「渠道 API Key」的 ID + 遮罩详情拼成一行日志文本。
     * userKey/channelKey 为明文，内部只经 {@link #mask} 输出，绝不泄露完整 key。
     */
    public static String describeKeys(Long userTokenId, String userKey, Long channelTokenId, String channelKey) {
        return "userTokenId=" + userTokenId
            + ", userKey=" + mask(userKey)
            + ", channelTokenId=" + channelTokenId
            + ", channelKey=" + mask(channelKey);
    }
}
