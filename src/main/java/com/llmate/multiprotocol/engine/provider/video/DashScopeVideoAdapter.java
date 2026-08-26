package com.llmate.multiprotocol.engine.provider.video;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.BusinessConstants;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmImageInput;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.LlmVideoParams;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.VideoTaskStatus;
import com.llmate.multiprotocol.dto.VideoTaskSubmitResult;
import com.llmate.multiprotocol.engine.provider.AbstractProviderAdapter;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UrlUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 阿里云百炼 DashScope 视频渠道 Provider（happyhorse / wan2.7 系列）
 *
 * 异步任务模式：
 * - 提交：POST video-synthesis + X-DashScope-Async: enable → {output:{task_id, task_status:PENDING}}
 * - 轮询：GET /api/v1/tasks/{taskId}（返回 SSE data: {...}，需剥壳）→ task_status/video_url/cover_url
 *
 * 端点路径默认 api/v1/services/aigc/video-generation/video-synthesis，
 * 可用 DB 配置（model_channel_configs / model_templates）的 endpointPath 覆盖。
 */
@Log4j2
public class DashScopeVideoAdapter extends AbstractProviderAdapter {

    private static final String SUBMIT_PATH = BusinessConstants.UPSTREAM_PATH_DASHSCOPE_VIDEO_SUBMIT;
    private static final String QUERY_PATH = BusinessConstants.UPSTREAM_PATH_DASHSCOPE_VIDEO_QUERY;
    private static final String ASYNC_HEADER = "X-DashScope-Async";
    private static final String ASYNC_ENABLE = "enable";

    private final String providerName;
    private final String providerAlias;

    public DashScopeVideoAdapter(String baseUrl, String apiKey, String name, String alias, ObjectMapper objectMapper,
                                 List<String> apiKeys, List<Long> tokenIds) {
        super(baseUrl, WebClient.builder().defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey), objectMapper, apiKeys, tokenIds);
        this.providerName = name;
        this.providerAlias = alias;
    }

    @Override
    public String getProviderAlias() {
        return providerAlias;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    // ==================== 文本聊天（视频渠道不支持） ====================

    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest request) {
        throw new UnsupportedOperationException("视频渠道不支持文本聊天: " + getProviderName());
    }

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest request) {
        throw new UnsupportedOperationException("视频渠道不支持文本流式聊天: " + getProviderName());
    }

    // ==================== 视频任务提交 ====================

    @Override
    public Mono<VideoTaskSubmitResult> generateVideo(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        ModelEndpointConfig effective = effectiveEndpoint(endpointConfig);
        Map<String, Object> body = buildSubmitBody(request);

        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            String url = effective.getFullUrl();

            // 上游请求日志（body 经 maskForLog 脱敏，base64 大段变占位符）
            LogBox.logUpstreamRequest(getProviderName(), url, maskForLog(body), requestId, userId);
            // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
            logUpstreamKeys(request, "生视频");

            // 生视频提交必须带 X-DashScope-Async: enable（异步任务头），doPostBlocking 不支持额外 header，
            // 这里用 mutate 后独立 WebClient 发送，保留 Bearer 认证 + 大响应缓冲。
            return webClient.mutate().build().post()
                    .uri(url)
                    .header(ASYNC_HEADER, ASYNC_ENABLE)
                    .bodyValue(body)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .doOnNext(resp -> LogBox.logUpstreamResponse(getProviderName(), maskForLog(resp), requestId, userId))
                    .map(this::parseSubmitResponse)
                    .doOnError(e -> logError("视频任务提交", e))
                    .onErrorResume(e -> Mono.error(translateUpstreamError(e)));
        });
    }

    /**
     * 构建 DashScope 视频合成请求体：
     * {"model":..., "input":{"prompt","negative_prompt","audio_url","media":[...]},
     *  "parameters":{"resolution":"720P","ratio":"16:9","duration":5,"prompt_extend":true,"watermark":false}}
     */
    private Map<String, Object> buildSubmitBody(LlmChatRequest request) {
        LlmVideoParams p = request.getVideoParams();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", request.getModel());

        Map<String, Object> input = new LinkedHashMap<>();
        input.put("prompt", p != null && p.getPrompt() != null ? p.getPrompt() : "");
        if (p != null) {
            if (p.getNegativePrompt() != null && !p.getNegativePrompt().isEmpty()) {
                input.put("negative_prompt", p.getNegativePrompt());
            }
            if (p.getAudioUrl() != null && !p.getAudioUrl().isEmpty()) {
                input.put("audio_url", p.getAudioUrl());
            }
            List<Map<String, Object>> media = buildMedia(p);
            if (media != null && !media.isEmpty()) {
                input.put("media", media);
            }
        }
        body.put("input", input);

        Map<String, Object> parameters = new LinkedHashMap<>();
        String resolution = p != null && p.getResolution() != null ? p.getResolution().toUpperCase() : "720P";
        parameters.put("resolution", resolution);
        String ratio = p != null && p.getAspectRatio() != null ? p.getAspectRatio() : "16:9";
        parameters.put("ratio", ratio);
        int duration = p != null && p.getDuration() != null ? p.getDuration() : 5;
        parameters.put("duration", duration);

        String model = request.getModel();
        boolean isWan = model != null && model.startsWith("wan2.7");
        if (isWan) {
            parameters.put("prompt_extend", p != null && p.getPromptExtend() != null ? p.getPromptExtend() : true);
            parameters.put("watermark", p != null && p.getWatermark() != null ? p.getWatermark() : false);
        }
        body.put("parameters", parameters);
        return body;
    }

    /**
     * 构建 media 列表（图生视频/参考生视频）：
     * - media 字段显式提供 → 直接透传
     * - 否则按 images 数量推断：1 张 → first_frame（i2v），≥2 张 → reference_image 各一张（r2v）
     */
    private List<Map<String, Object>> buildMedia(LlmVideoParams p) {
        if (p.getMedia() != null && !p.getMedia().isEmpty()) {
            List<Map<String, Object>> media = new ArrayList<>();
            for (LlmVideoParams.VideoMedia m : p.getMedia()) {
                Map<String, Object> item = new LinkedHashMap<>();
                if (m.getType() != null) {
                    item.put("type", m.getType());
                }
                if (m.getUrl() != null) {
                    item.put("url", m.getUrl());
                }
                if (m.getReferenceVoice() != null) {
                    item.put("reference_voice", m.getReferenceVoice());
                }
                media.add(item);
            }
            return media;
        }

        if (p.getImages() != null && !p.getImages().isEmpty()) {
            List<Map<String, Object>> media = new ArrayList<>();
            String type = p.getImages().size() == 1 ? "first_frame" : "reference_image";
            for (LlmImageInput in : p.getImages()) {
                String url = in.getUrl() != null ? in.getUrl()
                        : (in.getBase64Data() != null
                            ? "data:" + (in.getMimeType() != null ? in.getMimeType() : "image/png") + ";base64," + in.getBase64Data()
                            : null);
                if (url != null) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    item.put("type", type);
                    item.put("url", url);
                    media.add(item);
                }
            }
            return media.isEmpty() ? null : media;
        }
        return null;
    }

    /**
     * DashScope 提交响应 → VideoTaskSubmitResult
     * {output:{task_id, task_status:PENDING}, request_id}
     */
    private VideoTaskSubmitResult parseSubmitResponse(JsonNode root) {
        String taskId = root.path("output").path("task_id").asText(null);
        if (taskId == null || taskId.isEmpty()) {
            throw new IllegalStateException("DashScope 视频提交响应缺少 output.task_id: " + root);
        }
        String status = root.path("output").path("task_status").asText(BusinessConstants.TASK_STATUS_PENDING);
        return VideoTaskSubmitResult.builder()
                .taskId(taskId)
                .status(status)
                .build();
    }

    // ==================== 视频任务状态查询 ====================

    @Override
    public Mono<VideoTaskStatus> queryVideoTask(String taskId) {
        return queryVideoTask(taskId, null);
    }

    @Override
    public Mono<VideoTaskStatus> queryVideoTask(String taskId, ModelEndpointConfig endpointConfig) {
        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            String relativePath = UrlUtils.stripLeadingSlash(QUERY_PATH + taskId);
            String queryUrl = UrlUtils.join(baseUrl, QUERY_PATH + taskId);
            log.info("[{}] 查询视频任务状态: taskId={}, url={}", getProviderName(), taskId, queryUrl);

            return webClient.get()
                    .uri(relativePath)
                    .retrieve()
                    .bodyToMono(String.class)
                    .map(this::stripSsePrefix)
                    .map(this::parseQueryResponse)
                    .doOnNext(st -> LogBox.logUpstreamResponse(getProviderName(), st, requestId, userId))
                    .doOnError(e -> logError("视频任务查询", e))
                    .onErrorResume(e -> Mono.error(translateUpstreamError(e)));
        });
    }

    /**
     * DashScope 任务查询响应（SSE data: {...}）→ VideoTaskStatus
     * task_status: PENDING/PROCESSING/SUCCEEDED/FAILED/RUNNING
     */
    private VideoTaskStatus parseQueryResponse(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode output = root.path("output");
            if (output.isMissingNode() || output.isNull()) {
                output = root;
            }

            String status = firstText(output, "task_status").orElse(null);
            if (status == null) {
                status = firstText(root, "status").orElse(null);
            }

            // 归一化：终态 SUCCEEDED/FAILED，其余一律 RUNNING（PENDING/PROCESSING/RUNNING 都算进行中）
            String normalized;
            if (status == null) {
                normalized = "RUNNING";
            } else if (status.equalsIgnoreCase("SUCCEEDED") || status.equalsIgnoreCase("FAILED")) {
                normalized = status.toUpperCase();
            } else {
                normalized = "RUNNING";
            }

            VideoTaskStatus.VideoTaskStatusBuilder b = VideoTaskStatus.builder().status(normalized);
            if ("SUCCEEDED".equals(normalized)) {
                b.videoUrl(extractVideoUrl(output));
                b.coverUrl(firstText(output, "cover_url", "cover_image_url").orElse(null));
            }
            if ("FAILED".equals(normalized)) {
                JsonNode err = output.path("error");
                b.errorCode(firstText(err, "code").orElse("GENERATION_FAILED"));
                b.errorMessage(firstText(err, "message")
                        .orElse(firstText(output, "message").orElse("视频生成失败")));
            }
            return b.build();
        } catch (Exception e) {
            log.warn("[{}] 解析视频任务状态失败: json={}, err={}", getProviderName(), json, e.getMessage());
            return VideoTaskStatus.builder().status("RUNNING").build();
        }
    }

    /**
     * 从 output 提取视频 URL：video_url → result → results[0].url → output_urls[0]
     */
    private String extractVideoUrl(JsonNode output) {
        String url = firstText(output, "video_url").orElse(null);
        if (url == null) {
            url = firstText(output, "result").orElse(null);
        }
        if (url == null) {
            JsonNode results = output.path("results");
            if (results.isArray() && !results.isEmpty()) {
                url = results.get(0).path("url").asText(null);
            }
        }
        if (url == null) {
            JsonNode outputUrls = output.path("output_urls");
            if (outputUrls.isArray() && !outputUrls.isEmpty()) {
                url = outputUrls.get(0).asText(null);
            }
        }
        return url;
    }

    private Optional<String> firstText(JsonNode node, String... keys) {
        if (node == null || node.isMissingNode()) {
            return Optional.empty();
        }
        for (String k : keys) {
            JsonNode v = node.get(k);
            if (v != null && v.isTextual() && !v.asText().isEmpty()) {
                return Optional.of(v.asText());
            }
        }
        return Optional.empty();
    }

    /**
     * 剥掉 SSE 的 data: 前缀，取首个 { 到末尾 } 的 JSON 片段
     * DashScope 任务查询返回 SSE 格式（data: {...}），需剥壳后解析。
     */
    private String stripSsePrefix(String response) {
        if (response == null || response.isEmpty()) {
            return response;
        }
        String trimmed = response.trim();
        int jsonStart = trimmed.indexOf('{');
        if (jsonStart >= 0) {
            int lastBrace = trimmed.lastIndexOf('}');
            if (lastBrace > jsonStart) {
                return trimmed.substring(jsonStart, lastBrace + 1);
            }
        }
        return trimmed;
    }

    /**
     * 生效端点配置：DB 配置缺失或命中通用默认（chat/completions / 空）时，
     * 用本渠道默认路径构建。
     */
    private ModelEndpointConfig effectiveEndpoint(ModelEndpointConfig ec) {
        if (ec != null && ec.getEndpointPath() != null && !ec.getEndpointPath().isBlank()
                && !ec.getEndpointPath().equals("chat/completions")
                && !ec.getEndpointPath().equals(SUBMIT_PATH)) {
            return ec;
        }
        return ModelEndpointConfig.builder()
                .baseUrl(baseUrl)
                .endpointPath(SUBMIT_PATH)
                .httpMethod("POST")
                .build();
    }
}
