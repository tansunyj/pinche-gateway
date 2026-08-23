package com.llmate.multiprotocol.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.BillingParams;
import com.llmate.multiprotocol.dto.UsageData;
import lombok.experimental.UtilityClass;
import lombok.extern.log4j.Log4j2;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 增强日志工具类
 *
 * 关键事件（计费 / 预占 / 扣减 / 释放 / 结算 / 请求入口 / 上游请求响应 / 错误）使用纯 ASCII
 * "====" 分隔线框起：开头一行 ==== 分隔线，中间为详细内容行（原样完整打印，不截断），
 * 结尾一行 ==== 分隔线。不用制表符方框（╔═╗║）：中文全角字符在终端占 2 列，按字符数补
 * padding 永远画不齐，且内容行过长还会与边框粘连。
 *
 * 约定：
 * - 内容行一律完整打印，绝不做任何截断/截取。
 * - 流式(SSE)响应体不套 ==== 框，仅完整打印原文（chunk 已由各适配器/控制器逐条打印）。
 */
@UtilityClass
@Log4j2
public class LogBox {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /** 分隔线（纯 ASCII，任何终端/日志文件都显示为直线） */
    private static final String SEP = "================================================================================";

    /**
     * 开启一个分隔线框：空白行 + 开头 ==== 分隔线 + 标题行。
     * 调用方随后追加内容行，最后用 {@link #logFrame} 收尾。
     */
    private static StringBuilder frameStart(String title) {
        StringBuilder sb = new StringBuilder();
        sb.append("\n").append(SEP).append("\n");
        sb.append("【").append(title).append("】\n");
        return sb;
    }

    /** 关闭分隔线框：追加结尾 ==== 分隔线并打印整块日志 */
    private static void logFrame(StringBuilder sb) {
        sb.append(SEP);
        log.info(sb.toString());
    }

    /**
     * 请求入口日志（HTTP 请求，==== 框起，脱敏请求头 + 请求大小 + 请求体完整打印）。
     *
     * @param headers   脱敏后的请求头 Map（authorization/x-api-key/api-key 已置 "***"），可为 null
     * @param sizeBytes 请求大小（UTF-8 字节），未知传 null
     */
    public static void logRequestEntry(String method, String path, String requestId, Long userId,
                                       Map<String, String> headers, Integer sizeBytes, Object body) {
        StringBuilder sb = frameStart("请求入口");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        sb.append("Method: ").append(method).append("\n");
        sb.append("Path: ").append(path).append("\n");
        appendHeaders(sb, "Request Headers", headers);
        sb.append("Request Size: ").append(sizeBytes != null ? sizeBytes : "N/A").append(" bytes\n");
        appendBody(sb, "Body", body);
        logFrame(sb);
    }

    /**
     * 请求响应日志。
     *
     * @param headers   真实响应头 Map，可为 null
     * @param sizeBytes 响应大小（字节），未知传 null
     * @param isStream  是否流式(SSE)响应：是 → 不加 ==== 框，完整打印 SSE 原文；
     *                  否 → ==== 框起，响应体完整打印。
     */
    public static void logRequestResponse(String requestId, Long userId, long durationMs,
                                          Map<String, String> headers, Integer sizeBytes,
                                          Object body, boolean isStream) {
        if (isStream) {
            // 流式响应：不用 ==== 框，完整打印 SSE 响应体原文
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("【请求响应】RequestId: ").append(requestId != null ? requestId : "N/A")
              .append(", UserId: ").append(userId != null ? userId : "N/A")
              .append(", Duration: ").append(durationMs).append("ms, Type: text/event-stream\n");
            appendHeaders(sb, "Response Headers", headers);
            sb.append("Response Size: ").append(sizeBytes != null ? sizeBytes : "N/A").append(" bytes\n");
            appendBody(sb, "Body", body);
            log.info(sb.toString());
        } else {
            StringBuilder sb = frameStart("请求响应");
            sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
            sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
            sb.append("Duration: ").append(durationMs).append("ms\n");
            appendHeaders(sb, "Response Headers", headers);
            sb.append("Response Size: ").append(sizeBytes != null ? sizeBytes : "N/A").append(" bytes\n");
            appendBody(sb, "Body", body);
            logFrame(sb);
        }
    }

    /**
     * 上游渠道请求日志（请求体完整打印）
     */
    public static void logUpstreamRequest(String provider, String uri, Object requestBody, String requestId, Long userId) {
        StringBuilder sb = frameStart("上游渠道请求");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        sb.append("Provider: ").append(provider).append("\n");
        sb.append("URI: ").append(uri).append("\n");
        appendBody(sb, "Request", requestBody);
        logFrame(sb);
    }

    /**
     * 上游渠道响应日志（响应体完整打印，不截断）
     */
    public static void logUpstreamResponse(String provider, Object responseBody, String requestId, Long userId) {
        StringBuilder sb = frameStart("上游渠道响应");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        sb.append("Provider: ").append(provider).append("\n");
        appendBody(sb, "Response", responseBody);
        logFrame(sb);
    }

    /**
     * 用量提取日志：打印从上游响应中提取出的各维度用量（tokens / 图片数量 / 图文 token / 视频时长 / 向量 / 字符数等）。
     *
     * 这是计费前的"提取用量"环节。所有请求类型（文本聊天 / 生图 / 生视频 / 向量 / TTS）在计算费用前
     * 都统一打印本块，保证日志标准一致：上游渠道响应 → 用量提取 → 计费明细 → 计费计算 → 余额扣减 → 结算。
     * 只打印 >0 的维度，避免全 0 刷屏。
     */
    public static void logUsageExtraction(String requestId, Long userId, String modelId, UsageData usage) {
        StringBuilder sb = frameStart("用量提取");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        sb.append("ModelId: ").append(modelId != null ? modelId : "N/A").append("\n");
        long inputTokens = usage.getInputTokens();
        long cacheHit = usage.getCachedInputTokens();
        long cacheMiss = usage.getCacheMissTokens();
        // 输入tokens = 缓存命中 + 缓存未命中（总输入包含二者），缺一个用另一个补算，与计费 calcInputTokens 同口径：
        if (cacheMiss <= 0 && inputTokens > 0) {
            cacheMiss = Math.max(0, inputTokens - cacheHit);
        }
        if (cacheHit <= 0 && cacheMiss > 0) {
            cacheHit = Math.max(0, inputTokens - cacheMiss);
        }
        sb.append("输入tokens: ").append(inputTokens).append("\n");
        // 缓存命中/未命中在输入tokens行内拆分展示（命中即原「缓存输入tokens」，不再单列重复账目）
        sb.append("    其中缓存命中 ").append(cacheHit).append(", 缓存未命中 ").append(cacheMiss).append("\n");
        sb.append("输出tokens: ").append(usage.getOutputTokens()).append("\n");
        sb.append("总tokens: ").append(usage.getTotalTokens()).append("\n");
        appendUsageDim(sb, "推理tokens", usage.getReasoningTokens());
        appendUsageDim(sb, "图片数量", usage.getImageCount());
        appendUsageDim(sb, "图片输入文本tokens", usage.getInputTextTokens());
        appendUsageDim(sb, "图片输入图片tokens", usage.getInputImageTokens());
        appendUsageDim(sb, "图片输出文本tokens", usage.getOutputTextTokens());
        appendUsageDim(sb, "图片输出图片tokens", usage.getOutputImageTokens());
        appendUsageDim(sb, "视频时长(s)", usage.getVideoSeconds());
        if (usage.getVideoSeconds() > 0) {
            sb.append("视频分辨率: ").append(usage.is1080p() ? "1080p" : "720p")
              .append(", 是否带输入图: ").append(usage.isHasInputImage()).append("\n");
        }
        appendUsageDim(sb, "音频时长(s)", usage.getAudioSeconds());
        appendUsageDim(sb, "文本向量tokens", usage.getTextTokensEmbedding());
        appendUsageDim(sb, "图片向量tokens", usage.getImageTokensEmbedding());
        appendUsageDim(sb, "通用向量tokens", usage.getVectorTokens());
        appendUsageDim(sb, "TTS字符数", usage.getCharacterCount());
        logFrame(sb);
    }

    /** 用量提取块：只追加 >0 的维度（long） */
    private static void appendUsageDim(StringBuilder sb, String name, long value) {
        if (value > 0) {
            sb.append(name).append(": ").append(value).append("\n");
        }
    }

    /** 用量提取块：只追加 >0 的维度（int） */
    private static void appendUsageDim(StringBuilder sb, String name, int value) {
        if (value > 0) {
            sb.append(name).append(": ").append(value).append("\n");
        }
    }

    /**
     * 计费明细日志：先以一行可读格式打印查询到的价格参数（每个维度价格取值，未配置显示未配置），
     * 再逐维度打印用量 × 单价计算过程，全程完整不截断。
     *
     * @param modelId 上游模型ID（如 deepseek-v4-flash），用于定位是哪条模型记录被计费
     * @param params 查询到的价格参数（BillingParams）
     */
    public static void logBillingDetail(String billingMode, String requestId, Long userId, String modelId, Object params, List<String> details) {
        StringBuilder sb = frameStart("计费明细");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        sb.append("ModelId: ").append(modelId != null ? modelId : "N/A").append("\n");
        sb.append("BillingMode: ").append(billingMode).append("\n");
        if (params instanceof BillingParams p) {
            sb.append(formatPriceParams(p)).append("\n");
        } else {
            appendBody(sb, "查询到的价格参数", params);
        }
        if (details != null && !details.isEmpty()) {
            sb.append("----- 逐维度计算明细 -----\n");
            for (String line : details) {
                sb.append(line).append("\n");
            }
        }
        logFrame(sb);
    }

    /**
     * 把 BillingParams 格式化为一行可读的价格展示：
     * 价格：inputPer1m=1 元/M，outputPer1m=2 元/M，...
     * 只显示已配置（非 null 且不为 0）的价格维度，未配置的不打印。
     */
    private static String formatPriceParams(BillingParams p) {
        StringBuilder sb = new StringBuilder("价格：");
        appendPriceIfConfigured(sb, "inputPer1m", p.getInputPer1m(), "元/M");
        appendPriceIfConfigured(sb, "outputPer1m", p.getOutputPer1m(), "元/M");
        appendPriceIfConfigured(sb, "reasoningPer1m", p.getReasoningPer1m(), "元/M");
        appendPriceIfConfigured(sb, "cacheHitPer1m", p.getCacheHitPer1m(), "元/M");
        appendPriceIfConfigured(sb, "imagePerCall", p.getImagePerCall(), "元/次");
        appendPriceIfConfigured(sb, "inputTextPer1m", p.getInputTextPer1m(), "元/M");
        appendPriceIfConfigured(sb, "inputImagePer1m", p.getInputImagePer1m(), "元/M");
        appendPriceIfConfigured(sb, "outputTextPer1m", p.getOutputTextPer1m(), "元/M");
        appendPriceIfConfigured(sb, "outputImagePer1m", p.getOutputImagePer1m(), "元/M");
        appendPriceIfConfigured(sb, "videoPerSecond720p", p.getVideoPerSecond720p(), "元/秒");
        appendPriceIfConfigured(sb, "videoPerSecond1080p", p.getVideoPerSecond1080p(), "元/秒");
        appendPriceIfConfigured(sb, "audioPerSecond", p.getAudioPerSecond(), "元/秒");
        appendPriceIfConfigured(sb, "textTokensPer1m", p.getTextTokensPer1m(), "元/M");
        appendPriceIfConfigured(sb, "imageTokensPer1m", p.getImageTokensPer1m(), "元/M");
        appendPriceIfConfigured(sb, "vectorTokensPer1m", p.getVectorTokensPer1m(), "元/M");
        appendPriceIfConfigured(sb, "charactersPer1k", p.getCharactersPer1k(), "元/1k");
        appendPriceIfConfigured(sb, "flatPrice", p.getFlatPrice(), "元/次");
        return sb.toString();
    }

    /** 追加单个价格维度：仅当值非 null 且 >0 时才打印 */
    private static void appendPriceIfConfigured(StringBuilder sb, String name, BigDecimal value, String unit) {
        if (value == null || value.compareTo(BigDecimal.ZERO) <= 0) {
            return; // 未配置或价格为 0，跳过
        }
        if (sb.length() > "价格：".length()) {
            sb.append("，");
        }
        sb.append(name).append("=")
          .append(value.stripTrailingZeros().toPlainString()).append(" ").append(unit);
    }

    /**
     * 计费计算日志：价格配置(Params)与结算结果(Result)完整打印
     */
    public static void logBillingCalculation(String billingMode, String requestId, Long userId, String modelId, Object params, Object result, long quota) {
        StringBuilder sb = frameStart("计费计算");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        sb.append("ModelId: ").append(modelId != null ? modelId : "N/A").append("\n");
        sb.append("BillingMode: ").append(billingMode).append("\n");
        appendBody(sb, "Params", params);
        appendBody(sb, "Result", result);
        sb.append("Calculated Quota: ").append(quota).append("\n");
        logFrame(sb);
    }

    /**
     * 余额预占日志。
     *
     * 预占语义：预占只"锁定额度"、不扣减用户余额 —— 用户余额 balance 始终不变；
     * 真正起作用的是"累计已预占 total"：当 余额 - 累计已预占 < 本次预占 时拒绝新请求，
     * 避免下游调用把 balance 扣成负数还继续放行。
     *
     * @param balance       用户实际余额（预占不修改它）
     * @param reservedBefore 本次预占前的累计已预占额度
     * @param amount        本次预占额度
     * @param available     真实可预占额度 = balance - reservedBefore
     * @param success       本次预占是否成功
     */
    public static void logBalanceReserve(Long userId, String requestId, long balance,
                                         long reservedBefore, long amount, long available, boolean success) {
        StringBuilder sb = frameStart("余额预占");
        sb.append("UserId: ").append(userId).append("\n");
        sb.append("RequestId: ").append(requestId).append("\n");
        sb.append("用户实际余额: ").append(balance).append("  (预占不扣减余额，保持不变)\n");
        sb.append("累计已预占(本次前): ").append(reservedBefore).append("\n");
        sb.append("本次预占额度: ").append(amount).append("\n");
        sb.append("可预占额度: ").append(available).append("  (= 实际余额 - 累计已预占)\n");
        sb.append("累计已预占(本次后): ").append(reservedBefore + amount).append("\n");
        sb.append("预占结果: ").append(success ? "成功" : "拒绝").append("\n");
        if (!success) {
            sb.append("拒绝原因: 可预占额度(").append(available)
              .append(") < 本次预占(").append(amount)
              .append(")，若允许将把余额扣成负数，故拒绝本次调用\n");
        }
        logFrame(sb);
    }

    /**
     * 余额扣减日志。
     * 结算时才真正扣减余额；同时释放该请求的预占额度。
     *
     * @param balanceBefore     扣减前余额
     * @param actualAmount      实际扣减额度（按真实用量）
     * @param newBalance        扣减后余额
     * @param releasedReserved  本次释放的该请求预占额度（归还给可用额度池）
     * @param totalReserved     释放后累计已预占额度
     */
    public static void logBalanceDeduct(Long userId, String requestId, long balanceBefore,
                                        long actualAmount, long newBalance, long releasedReserved, long totalReserved) {
        StringBuilder sb = frameStart("余额扣减");
        sb.append("UserId: ").append(userId).append("\n");
        sb.append("RequestId: ").append(requestId).append("\n");
        sb.append("扣减前余额: ").append(balanceBefore).append("\n");
        sb.append("实际扣减: ").append(actualAmount).append("  (结算按真实用量扣减)\n");
        sb.append("扣减后余额: ").append(newBalance).append("\n");
        sb.append("释放该请求预占: ").append(releasedReserved).append("  (归还累计已预占额度池)\n");
        sb.append("累计已预占(释放后): ").append(totalReserved).append("\n");
        sb.append("可预占额度: ").append(newBalance - totalReserved).append("  (= 扣减后余额 - 累计已预占)\n");
        logFrame(sb);
    }

    /**
     * 余额扣减日志（简化版，无 totalReserved 时兼容）
     */
    public static void logBalanceDeduct(Long userId, String requestId, long balanceBefore,
                                        long actualAmount, long newBalance, long releasedReserved) {
        logBalanceDeduct(userId, requestId, balanceBefore, actualAmount, newBalance, releasedReserved, 0);
    }

    /**
     * 余额 DB 更新日志：结算扣减后同步 pt_users.balance。
     * 扣减采用相对 SQL `balance = balance - :amount`（原子、防并发丢更新），不是绝对值直写。
     */
    public static void logBalanceDbUpdate(Long userId, String requestId, long deductedAmount, long oldBalance, long newBalance) {
        StringBuilder sb = frameStart("余额DB更新");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId).append("\n");
        sb.append("本次扣减: ").append(deductedAmount).append("\n");
        sb.append("扣减前余额: ").append(oldBalance).append("\n");
        sb.append("扣减后余额: ").append(newBalance).append("  (相对扣减，等于 DB 原值 - 本次扣减)\n");
        sb.append("UPDATE SQL: UPDATE pt_users SET balance = balance - ").append(deductedAmount)
          .append(" WHERE id = ").append(userId).append("\n");
        logFrame(sb);
    }

    /**
     * 余额释放日志：请求失败或无需扣费时，把预占额度归还给可用额度池，余额不变
     */
    public static void logBalanceRelease(Long userId, String requestId, long releasedAmount) {
        StringBuilder sb = frameStart("余额释放");
        sb.append("UserId: ").append(userId).append("\n");
        sb.append("RequestId: ").append(requestId).append("\n");
        sb.append("Released Amount: ").append(releasedAmount)
          .append("  (归还累计已预占额度池，余额不变)\n");
        logFrame(sb);
    }

    /**
     * 结算/请求日志记录（实体完整 JSON 打印）
     */
    public static void logSettlement(String requestId, Long userId, Object settlementData) {
        StringBuilder sb = frameStart("结算记录");
        sb.append("RequestId: ").append(requestId != null ? requestId : "N/A").append("\n");
        sb.append("UserId: ").append(userId != null ? userId : "N/A").append("\n");
        appendBody(sb, "Data", settlementData);
        logFrame(sb);
    }

    /**
     * 错误日志
     */
    public static void logError(String title, String error, Object details) {
        StringBuilder sb = frameStart("错误" + title);
        sb.append("Error: ").append(error).append("\n");
        if (details != null) {
            appendBody(sb, "Details", details);
        }
        logFrame(sb);
    }

    // ==================== 内部工具方法 ====================

    /**
     * 追加 Header 内容行：逐行 "  Key: Value" 打印；空/null 打 "(none)"。
     */
    private static void appendHeaders(StringBuilder sb, String label, Map<String, String> headers) {
        sb.append(label).append(":\n");
        if (headers == null || headers.isEmpty()) {
            sb.append("  (none)\n");
            return;
        }
        headers.forEach((key, value) -> sb.append("  ").append(key).append(": ").append(value).append("\n"));
    }

    /**
     * 追加 Body 内容行：String 原样完整打印；对象序列化为 JSON 完整打印。不做任何截断。
     */
    private static void appendBody(StringBuilder sb, String label, Object body) {
        sb.append(label).append(":\n");
        if (body == null) {
            sb.append("null\n");
            return;
        }
        if (body instanceof String) {
            sb.append((String) body).append("\n");
            return;
        }
        try {
            sb.append(OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(body)).append("\n");
        } catch (Exception e) {
            // 序列化失败兜底：原样 toString
            sb.append(body).append("\n");
        }
    }
}
