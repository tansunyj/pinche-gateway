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

/**
 * 火山引擎 Seedance 2.0 视频渠道 Provider
 *
 * 异步任务模式（ark contents/generations API）：
 * - 提交：POST api/v3/contents/generations/tasks → {id: cgt-xxx, status: "queued"}
 * - 轮询：GET api/v3/contents/generations/tasks/{id} → status(succeeded/failed) + content.video_url + usage.completion_tokens
 *
 * 计费：按 token（completion_tokens），resolution + 是否带输入图决定单价档位。
 */
@Log4j2
public class VolcengineVideoAdapter extends AbstractProviderAdapter {

    private static final String SUBMIT_PATH = BusinessConstants.UPSTREAM_PATH_SEEDANCE_VIDEO_SUBMIT;
    private static final String QUERY_PATH = BusinessConstants.UPSTREAM_PATH_SEEDANCE_VIDEO_QUERY;

    // seedance 模型名 → 火山官方模型ID
    private static final Map<String, String> OFFICIAL_MODELS = Map.of(
            "seedance-2-0", "doubao-seedance-2-0-260128",
            "seedance-2-0-fast", "doubao-seedance-2-0-fast-250615",
            "seedance-2-0-mini", "doubao-seedance-2-0-mini-250615");

    private final String providerName;
    private final String providerAlias;

    public VolcengineVideoAdapter(String baseUrl, String apiKey, String name, String alias, ObjectMapper objectMapper,
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
        Map<String, Object> body = buildSubmitBody(request);

        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);
            String relativePath = UrlUtils.stripLeadingSlash(SUBMIT_PATH);
            String url = UrlUtils.join(baseUrl, SUBMIT_PATH);

            LogBox.logUpstreamRequest(getProviderName(), url, maskForLog(body), requestId, userId);

            return webClient.post()
                    .uri(relativePath)
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
     * 构建 Seedance 请求体：
     * {"model":"doubao-seedance-2-0-260128",
     *  "content":[{"type":"text","text":...},{"type":"image_url","image_url":{"url":...},"role":"first_frame"},...],
     *  "ratio":"16:9","duration":5,"generate_audio":true,"watermark":false,"resolution":"720p"}
     */
    private Map<String, Object> buildSubmitBody(LlmChatRequest request) {
        LlmVideoParams p = request.getVideoParams();
        Map<String, Object> body = new LinkedHashMap<>();

        String model = request.getModel();
        body.put("model", officialModelId(model));

        List<Map<String, Object>> content = new ArrayList<>();

        // 1. 文本提示词
        if (p != null && p.getPrompt() != null && !p.getPrompt().isEmpty()) {
            Map<String, Object> text = new LinkedHashMap<>();
            text.put("type", "text");
            text.put("text", p.getPrompt());
            content.add(text);
        }

        // 2. 参考图片：按图片数量自动判模式（1 首帧 / 2 首尾帧 / ≥3 多模态参考）
        List<LlmImageInput> images = p != null ? p.getImages() : null;
        boolean isMini = isMiniModel(model);
        if (images != null && !images.isEmpty()) {
            if (images.size() == 1) {
                content.add(imageContent(images.get(0), "first_frame"));
            } else if (images.size() == 2) {
                content.add(imageContent(images.get(0), "first_frame"));
                content.add(imageContent(images.get(1), "last_frame"));
            } else {
                int count = 0;
                for (LlmImageInput in : images) {
                    if (count >= 9) {
                        break;
                    }
                    content.add(imageContent(in, "reference_image"));
                    count++;
                }
            }
        }

        // 3. 参考视频（非 mini，最多 3 个）
        if (!isMini && p != null && p.getReferenceVideos() != null) {
            int count = 0;
            for (String vurl : p.getReferenceVideos()) {
                if (count >= 3 || vurl == null || vurl.isEmpty()) {
                    break;
                }
                Map<String, Object> vc = new LinkedHashMap<>();
                vc.put("type", "video_url");
                Map<String, Object> vu = new LinkedHashMap<>();
                vu.put("url", vurl);
                vc.put("video_url", vu);
                vc.put("role", "reference_video");
                content.add(vc);
                count++;
            }
        }

        // 4. 参考音频（非 mini，最多 3 个 + audioUrl）
        if (!isMini && p != null) {
            List<String> audios = new ArrayList<>();
            if (p.getReferenceAudios() != null) {
                audios.addAll(p.getReferenceAudios());
            }
            if (p.getAudioUrl() != null && !p.getAudioUrl().isEmpty() && !audios.contains(p.getAudioUrl())) {
                audios.add(p.getAudioUrl());
            }
            int count = 0;
            for (String aurl : audios) {
                if (count >= 3 || aurl == null || aurl.isEmpty()) {
                    break;
                }
                Map<String, Object> ac = new LinkedHashMap<>();
                ac.put("type", "audio_url");
                Map<String, Object> au = new LinkedHashMap<>();
                au.put("url", aurl);
                ac.put("audio_url", au);
                ac.put("role", "reference_audio");
                content.add(ac);
                count++;
            }
        }

        body.put("content", content);

        String ratio = p != null && p.getAspectRatio() != null ? p.getAspectRatio() : "16:9";
        body.put("ratio", ratio);

        int duration = p != null && p.getDuration() != null ? p.getDuration() : 5;
        if (duration < 4) {
            duration = 4;
        }
        if (duration > 15) {
            duration = 15;
        }
        body.put("duration", duration);

        body.put("generate_audio", p != null && p.getGenerateAudio() != null ? p.getGenerateAudio() : true);
        body.put("watermark", p != null && p.getWatermark() != null ? p.getWatermark() : false);

        // mini 模型仅支持 480p/720p
        String resolution = p != null && p.getResolution() != null ? p.getResolution().toLowerCase() : "720p";
        if (isMini && !"480p".equals(resolution)) {
            resolution = "720p";
        }
        body.put("resolution", resolution);

        return body;
    }

    private Map<String, Object> imageContent(LlmImageInput in, String role) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("type", "image_url");
        Map<String, Object> iu = new LinkedHashMap<>();
        String url = in.getUrl() != null ? in.getUrl()
                : (in.getBase64Data() != null
                    ? "data:" + (in.getMimeType() != null ? in.getMimeType() : "image/png") + ";base64," + in.getBase64Data()
                    : "");
        iu.put("url", url);
        item.put("image_url", iu);
        item.put("role", role);
        return item;
    }

    /**
     * Seedance 提交响应 → VideoTaskSubmitResult
     * {id: cgt-xxx, status: "queued"}
     */
    private VideoTaskSubmitResult parseSubmitResponse(JsonNode root) {
        String taskId = root.path("id").asText(null);
        if (taskId == null || taskId.isEmpty()) {
            throw new IllegalStateException("Seedance 视频提交响应缺少 id: " + root);
        }
        return VideoTaskSubmitResult.builder()
                .taskId(taskId)
                .status(BusinessConstants.TASK_STATUS_PENDING)
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
                    .bodyToMono(JsonNode.class)
                    .doOnNext(resp -> LogBox.logUpstreamResponse(getProviderName(), maskForLog(resp), requestId, userId))
                    .map(this::parseQueryResponse)
                    .doOnError(e -> logError("视频任务查询", e))
                    .onErrorResume(e -> Mono.error(translateUpstreamError(e)));
        });
    }

    /**
     * Seedance 任务查询响应 → VideoTaskStatus
     * status: queued/running/cancelled/succeeded/failed/expired；content.video_url / last_frame_url；usage.completion_tokens
     */
    private VideoTaskStatus parseQueryResponse(JsonNode root) {
        String status = root.path("status").asText(null);
        String normalized;
        if (status == null) {
            normalized = "RUNNING";
        } else if (status.equalsIgnoreCase("succeeded")) {
            normalized = "SUCCEEDED";
        } else if (status.equalsIgnoreCase("failed")) {
            normalized = "FAILED";
        } else {
            // queued/running/cancelled/expired 都算进行中（cancelled/expired 由本地轮询/超时处理）
            normalized = "RUNNING";
        }

        VideoTaskStatus.VideoTaskStatusBuilder b = VideoTaskStatus.builder().status(normalized);
        if ("SUCCEEDED".equals(normalized)) {
            JsonNode content = root.path("content");
            b.videoUrl(content.path("video_url").asText(null));
            b.coverUrl(content.path("last_frame_url").asText(null));
            JsonNode usage = root.path("usage");
            if (usage.has("completion_tokens")) {
                b.completionTokens(usage.path("completion_tokens").asLong());
            }
        }
        if ("FAILED".equals(normalized)) {
            JsonNode err = root.path("error");
            b.errorCode(err.path("code").asText(null));
            b.errorMessage(err.path("message").asText("视频生成失败"));
        }
        return b.build();
    }

    /**
     * seedance-* → doubao-seedance-* 官方模型ID；未映射则原样透传
     */
    private String officialModelId(String model) {
        if (model == null) {
            return model;
        }
        return OFFICIAL_MODELS.getOrDefault(model, model);
    }

    private boolean isMiniModel(String model) {
        return model != null && model.contains("mini");
    }
}
