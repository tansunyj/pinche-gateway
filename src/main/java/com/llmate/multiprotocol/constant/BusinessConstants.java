package com.llmate.multiprotocol.constant;

import lombok.experimental.UtilityClass;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Set;

/**
 * 业务常量
 * 与业务逻辑相关的常量：计费、额度、计费模式等
 */
@UtilityClass
public class BusinessConstants {

    // ==================== 额度换算 ====================
    public static final BigDecimal QUOTA_PER_USD = new BigDecimal("100000");
    public static final BigDecimal DEFAULT_EXCHANGE_RATE = new BigDecimal("7.25");

    // ==================== 余额预占 ====================
    /**
     * 单次请求预占额度的固定值
     * 预占用于在请求执行期间锁定余额，防止并发超支。
     * 使用固定较小值而非按价格估算，避免预占「100万token」的极端上限
     * 掏空可用余额（如 deepseek 旧逻辑预占 300000，实际消费仅 233）。
     */
    public static final long DEFAULT_RESERVE_QUOTA = 5000;

    /**
     * 生图单次预占额度：20000（0.2 元）
     */
    public static final long DEFAULT_RESERVE_QUOTA_IMAGE = 20000;

    /**
     * 生视频单次预占额度：400000（4 元）
     * 固定一口价，不做按价格/时长的复杂估算，避免 video_token 取最高档单价（如 51 元/M）
     * 预占 510 万额度，把只有几元余额的用户全部挡掉
     */
    public static final long DEFAULT_RESERVE_QUOTA_VIDEO = 400000;

    // ==================== 计费模式 ====================
    public static final String BILLING_MODE_TOKEN = "token";
    public static final String BILLING_MODE_IMAGE = "image";
    public static final String BILLING_MODE_IMAGE_TOKEN = "image_token";
    public static final String BILLING_MODE_VIDEO_SECOND = "video_second";
    public static final String BILLING_MODE_VIDEO_TOKEN = "video_token";
    public static final String BILLING_MODE_TTS = "tts";
    public static final String BILLING_MODE_EMBEDDING = "embedding";
    public static final String BILLING_MODE_FLAT = "flat";
    public static final String BILLING_MODE_ASR = "asr";

    // ==================== 价格计算 ====================
    public static final BigDecimal TOKEN_PER_1M = new BigDecimal("1000000");
    public static final BigDecimal CHARS_PER_1K = new BigDecimal("1000");
    public static final int COST_SCALE = 10;
    public static final RoundingMode COST_ROUNDING_MODE = RoundingMode.HALF_UP;
    /**
     * 额度换算取整方式：向上取整（RoundingMode.UP = 正数取 ceil）。
     * 用户明确要求：价格计算必须向上取整、不能遗漏数值 ——
     * 保证任何非零费用至少扣 1 额度，向量等小额计费不因取整归零而变成"不计费"。
     * 不要改为 HALF_UP/DOWN 等可能把小数归零的取整方式。
     */
    public static final RoundingMode QUOTA_ROUNDING_MODE = RoundingMode.UP;

    // ==================== 上游API路径 ====================
    public static final String UPSTREAM_PATH_CHAT = "v1/chat/completions";
    public static final String UPSTREAM_PATH_MESSAGES = "v1/messages";
    public static final String UPSTREAM_PATH_IMAGE_GENERATIONS = "v1/images/generations";
    public static final String UPSTREAM_PATH_IMAGE_EDITS = "v1/images/edits";
    public static final String UPSTREAM_PATH_VIDEO_GENERATIONS = "v1/videos/generations";
    public static final String UPSTREAM_PATH_EMBEDDINGS = "v1/embeddings";
    public static final String UPSTREAM_PATH_TTS = "v1/audio/speech";

    // ==================== 向量上游API路径（照老项目 silievo 向量服务）====================
    public static final String UPSTREAM_PATH_TEXT_EMBEDDING = "/api/v1/services/embeddings/text-embedding/text-embedding";
    public static final String UPSTREAM_PATH_MULTIMODAL_EMBEDDING = "/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding";

    // ==================== 生视频 ====================

    // 视频生成模式（与 video_generation_tasks.video_mode 枚举一致）
    public static final String VIDEO_MODE_TEXT2VIDEO = "text2video";
    public static final String VIDEO_MODE_IMAGE2VIDEO = "image2video";
    public static final String VIDEO_MODE_REFERENCE2VIDEO = "reference2video";
    public static final String VIDEO_MODE_FIRST_LAST_FRAME = "first_last_frame";
    public static final String VIDEO_MODE_VIDEO2VIDEO = "video2video";

    /** 合法 video_mode 枚举值集合（落库校验用：mode 字段只在白名单内才采纳，否则忽略走自动推断） */
    public static final Set<String> VALID_VIDEO_MODES = Set.of(
            VIDEO_MODE_TEXT2VIDEO, VIDEO_MODE_IMAGE2VIDEO, VIDEO_MODE_REFERENCE2VIDEO,
            VIDEO_MODE_FIRST_LAST_FRAME, VIDEO_MODE_VIDEO2VIDEO);

    // 视频任务状态（本地 DB 存储）
    public static final String TASK_STATUS_PENDING = "PENDING";
    public static final String TASK_STATUS_PROCESSING = "PROCESSING";
    public static final String TASK_STATUS_SUCCEEDED = "SUCCEEDED";
    public static final String TASK_STATUS_FAILED = "FAILED";
    public static final String TASK_STATUS_TIMEOUT = "TIMEOUT";

    // 视频任务计费状态
    public static final String BILLING_STATUS_PENDING = "pending";
    public static final String BILLING_STATUS_BILLED = "billed";
    public static final String BILLING_STATUS_OVERDUE = "overdue";

    // 上游视频 API 路径
    public static final String UPSTREAM_PATH_DASHSCOPE_VIDEO_SUBMIT = "api/v1/services/aigc/video-generation/video-synthesis";
    public static final String UPSTREAM_PATH_DASHSCOPE_VIDEO_QUERY = "api/v1/tasks/";
    public static final String UPSTREAM_PATH_SEEDANCE_VIDEO_SUBMIT = "api/v3/contents/generations/tasks";
    public static final String UPSTREAM_PATH_SEEDANCE_VIDEO_QUERY = "api/v3/contents/generations/tasks/";

    // 视频任务超时阈值（分钟）与轮询并发
    public static final long VIDEO_TASK_TIMEOUT_MINUTES = 60;
    public static final int VIDEO_POLL_CONCURRENCY = 8;
    public static final int VIDEO_ACTIVE_TASK_LIMIT = 100;

    // ==================== ASR 语音转写（异步任务） ====================

    /**
     * ASR 异步任务超时阈值（分钟）：780 = 12h 最长音频 + 1h 缓冲。
     * 百炼异步转写最长支持 12h 音频（qwen3-asr-flash-filetrans 等），若按视频的 60min 阈值，
     * 合法处理中的长音频任务会被误标 TIMEOUT 并释放预占，故必须放宽。
     */
    public static final long AUDIO_TASK_TIMEOUT_MINUTES = 780L;

    /** ASR 异步任务轮询并发数（查询是短阻塞 HTTP，不宜过高占线程池） */
    public static final int AUDIO_POLL_CONCURRENCY = 4;

    /** ASR 异步任务单轮最多拉取条数 */
    public static final int AUDIO_ACTIVE_TASK_LIMIT = 100;
}
