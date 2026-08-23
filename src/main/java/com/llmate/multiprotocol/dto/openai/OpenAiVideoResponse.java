package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * OpenAI 兼容的视频生成响应体
 *
 * - 提交（POST /v1/videos/generations）：{id, object:"video.generation", created, model, status:"PENDING"}
 * - 查询（GET /v1/videos/generations/{taskId}）：{id, object, created, model, status, data:[{url,cover_url}], usage, error}
 * 对齐 OpenAI 异步任务形态（如 images 的异步变体），status 取值 PENDING/PROCESSING/SUCCEEDED/FAILED/TIMEOUT。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiVideoResponse {

    /** 任务ID（上游 task_id） */
    private String id;

    /** 固定 "video.generation" */
    private String object;

    private Long created;

    /** 客户端原始模型ID（含渠道前缀） */
    private String model;

    private String status;

    /** 生成结果（SUCCEEDED 时有值） */
    private List<VideoData> data;

    private OpenAiVideoUsage usage;

    private OpenAiVideoError error;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VideoData {
        private String url;
        @JsonProperty("cover_url")
        private String coverUrl;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpenAiVideoUsage {
        @JsonProperty("prompt_tokens")
        private Long promptTokens;
        @JsonProperty("completion_tokens")
        private Long completionTokens;
        @JsonProperty("total_tokens")
        private Long totalTokens;
        /** 实际扣减额度（金额消耗，结算后由 BillingService.markBilled 回写 quota_consumed） */
        @JsonProperty("quota_consumed")
        private Long quotaConsumed;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class OpenAiVideoError {
        private String code;
        private String message;
        /** 请求ID：便于客户端报障时在网关日志定位该任务请求 */
        @JsonProperty("request_id")
        private String requestId;
    }
}
