package com.llmate.multiprotocol.util;

/**
 * URL 拼接工具类
 *
 * 统一处理 baseUrl 与 path 拼接的斜杠问题：
 * - baseUrl 结尾无斜杠 + path 开头无斜杠 → 中间补一个斜杠
 * - baseUrl 结尾有斜杠 + path 开头有斜杠 → 去掉重复斜杠
 * - baseUrl 结尾有斜杠 + path 开头无斜杠（或反之）→ 保持单斜杠
 *
 * 用法：
 *   join("https://api.deepseek.com", "v1/chat/completions")     → https://api.deepseek.com/v1/chat/completions
 *   join("https://api.deepseek.com/", "/v1/chat/completions")   → https://api.deepseek.com/v1/chat/completions
 *   join("https://api.vapeur.ai/claude", "v1/messages")         → https://api.vapeur.ai/claude/v1/messages
 */
public final class UrlUtils {

    private UrlUtils() {
    }

    /**
     * 拼接 baseUrl 与 path，保证结果中恰好只有一个斜杠衔接。
     * 任一段为 null/空则返回另一段原样。
     */
    public static String join(String baseUrl, String path) {
        if (baseUrl == null || baseUrl.isEmpty()) {
            return path != null ? path : "";
        }
        if (path == null || path.isEmpty()) {
            return baseUrl;
        }
        String base = baseUrl;
        String p = path;
        // 去掉 base 结尾的斜杠
        while (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        // 去掉 path 开头的斜杠
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        return base + "/" + p;
    }

    /**
     * 去掉 path 开头的所有斜杠，得到可作为 WebClient 相对路径使用的形式。
     * 相对路径不带前导斜杠时，WebClient 会基于 baseUrl 追加（不会覆盖 baseUrl 的路径部分）。
     */
    public static String stripLeadingSlash(String path) {
        if (path == null) {
            return null;
        }
        String p = path;
        while (p.startsWith("/")) {
            p = p.substring(1);
        }
        return p;
    }

    /**
     * 确保 URL 以单个斜杠结尾（用于 WebClient baseUrl，保证相对路径追加正确）。
     */
    public static String withTrailingSlash(String url) {
        if (url == null || url.isEmpty()) {
            return url;
        }
        return url.endsWith("/") ? url : url + "/";
    }
}
