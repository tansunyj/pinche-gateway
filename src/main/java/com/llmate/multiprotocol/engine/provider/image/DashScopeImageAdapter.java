package com.llmate.multiprotocol.engine.provider.image;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmImage;
import com.llmate.multiprotocol.dto.LlmImageInput;
import com.llmate.multiprotocol.dto.LlmImageParams;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.engine.provider.AbstractProviderAdapter;
import com.llmate.multiprotocol.service.OssService;
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 阿里云百炼 DashScope 图像渠道 Provider（qwen-image 系）
 *
 * 走单接口 multimodal-generation（生成与编辑都路由到这里，参数不同）：
 * - 生图：content 只含 text
 * - 编辑：content 追加 {image: dataUri 或 url}（把输入图带进同一个请求体）
 *
 * 端点路径：默认 api/v1/services/aigc/multimodal-generation/generation，
 * 可用 DB 配置（model_channel_configs / model_templates）的 endpointPath 覆盖。
 */
@Log4j2
public class DashScopeImageAdapter extends AbstractProviderAdapter {

    private static final String DEFAULT_PATH = "api/v1/services/aigc/multimodal-generation/generation";

    /**
     * 图片 URL → base64 的下载客户端（独立无鉴权 WebClient，避免把渠道 key 发给图片主机）。
     * 统一由 WebClientUtils 提供（newConnection 禁连接池 + 跟随重定向 + 5 分钟超时）。
     */
    private static final WebClient IMAGE_DOWNLOAD_CLIENT = WebClientUtils.imageDownloadClient();

    private final String providerName;
    private final String providerAlias;
    private final OssService ossService;

    public DashScopeImageAdapter(String baseUrl, String apiKey, String name, String alias, ObjectMapper objectMapper,
                                 OssService ossService, List<String> apiKeys, List<Long> tokenIds) {
        super(baseUrl, WebClient.builder().defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey), objectMapper, apiKeys, tokenIds);
        this.providerName = name;
        this.providerAlias = alias;
        // 用于把 HTTP 上传的 base64 图片转成 OSS URL（DashScope 渠道接口需要图片地址）
        this.ossService = ossService;
    }

    @Override
    public String getProviderAlias() {
        return providerAlias;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    // ==================== 文本聊天（图像渠道不支持） ====================

    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest request) {
        throw new UnsupportedOperationException("图像渠道不支持文本聊天: " + getProviderName());
    }

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest request) {
        throw new UnsupportedOperationException("图像渠道不支持文本流式聊天: " + getProviderName());
    }

    @Override
    public Mono<LlmChatResponse> generateImage(LlmChatRequest request) {
        return generateImage(request, null);
    }

    @Override
    public Mono<LlmChatResponse> generateImage(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        return doGenerate(request, endpointConfig, false);
    }

    @Override
    public Mono<LlmChatResponse> editImage(LlmChatRequest request) {
        return editImage(request, null);
    }

    @Override
    public Mono<LlmChatResponse> editImage(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        return doGenerate(request, endpointConfig, true);
    }

    /**
     * 单接口生成/编辑（edit=true 或携带参考图时在 content 追加输入图）。
     *
     * ⚠️ DashScope 渠道不区分生图/编辑，两者都走同一个 multimodal-generation 接口，
     * 区别只在 content：编辑或生图带参考图时把输入图带进 content。输入图若是 HTTP
     * 上传的二进制（内部只有 base64、无 url），必须先上传 OSS 换成图片地址再传给
     * 渠道接口（DashScope 需要图片地址；上传失败回退 data URI，不阻塞主链路）。
     */
    private Mono<LlmChatResponse> doGenerate(LlmChatRequest request, ModelEndpointConfig ec, boolean edit) {
        LlmImageParams p = request.getImageParams();
        ModelEndpointConfig effective = effectiveEndpoint(ec);

        return buildContentParts(p, edit)
            .map(contentParts -> {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("model", request.getModel());
                Map<String, Object> message = new LinkedHashMap<>();
                message.put("role", "user");
                message.put("content", contentParts);
                Map<String, Object> input = new LinkedHashMap<>();
                input.put("messages", List.of(message));
                body.put("input", input);

                // OpenAI 标准入口参数适配到 DashScope parameters（qwen-image 原生支持 n/size），
                // quality/style/response_format 等 DashScope 不支持的参数不发送，避免上游 400。
                Map<String, Object> parameters = new LinkedHashMap<>();
                if (p != null) {
                    if (p.getN() != null) {
                        parameters.put("n", p.getN());
                    }
                    if (p.getSize() != null) {
                        parameters.put("size", sizeToDashScope(p.getSize()));
                    }
                    if (p.getExtraParams() != null) {
                        parameters.putAll(p.getExtraParams());
                    }
                }
                if (!parameters.isEmpty()) {
                    body.put("parameters", parameters);
                }
                return body;
            })
            .doOnNext(body -> log.info("[{}] 请求地址: POST {}", getProviderName(), effective.getFullUrl()))
            // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
            .doOnNext(body -> logUpstreamKeys(request, edit ? "图像编辑" : "图像生成"))
            .flatMap(body -> doPostBlocking(DEFAULT_PATH, body, JsonNode.class, json -> parseResponse(json, request), effective))
            .flatMap(resp -> maybeDownloadToBase64(resp, request))
            .doOnNext(resp -> log.info("[{}] 图像{}完成: images={}", getProviderName(),
                edit ? "编辑" : "生成", resp.getImages() != null ? resp.getImages().size() : 0))
            .doOnError(e -> logError(edit ? "图像编辑" : "图像生成", e));
    }

    /**
     * 构建 content 列表，与 DashScope 官网编辑示例格式完全一致：图片在前、文本在后。
     *
     * ⚠️ 顺序有语义：qwen-image 的编辑提示（如"把图2的涂鸦喷绘在图1的汽车上"）按
     * content 数组里【图片出现的顺序】编号（图1、图2…），所以输入图必须排在文本之前，
     * 且图片之间保持客户端传入的相对顺序。生图（无输入图）content 只含 text。
     * 生图请求若客户端通过 images 数组携带参考图（多图编辑/合并场景），同样带进 content——
     * 否则参考图被静默丢弃（与 Gemini 同款修复）。
     *
     * 输入图有 url 直接用；只有 base64（HTTP 二进制上传）的上传 OSS 换 url。
     */
    private Mono<List<Map<String, Object>>> buildContentParts(LlmImageParams p, boolean edit) {
        // 编辑（edit=true）必须带输入图；生图请求若客户端通过 images 数组携带了参考图
        // （DashScope 多图编辑/合并场景），同样要带进 content，否则参考图被静默丢弃。
        boolean hasImages = p != null && p.getImages() != null && !p.getImages().isEmpty();
        if ((edit || hasImages) && p != null && p.getImages() != null && !p.getImages().isEmpty()) {
            List<Mono<Map<String, Object>>> imgPartMonos = new ArrayList<>();
            for (LlmImageInput in : p.getImages()) {
                if (in.getUrl() != null) {
                    Map<String, Object> part = new LinkedHashMap<>();
                    part.put("image", in.getUrl());
                    imgPartMonos.add(Mono.just(part));
                } else if (in.getBase64Data() != null) {
                    imgPartMonos.add(uploadImageToUrl(in));
                }
            }
            return Flux.concat(imgPartMonos).collectList().map(imgParts -> {
                List<Map<String, Object>> all = new ArrayList<>(imgParts);
                if (p.getPrompt() != null) {
                    Map<String, Object> textPart = new LinkedHashMap<>();
                    textPart.put("text", p.getPrompt());
                    all.add(textPart);
                }
                return all;
            });
        }
        // 生图：只含文本
        List<Map<String, Object>> parts = new ArrayList<>();
        if (p != null && p.getPrompt() != null) {
            Map<String, Object> textPart = new LinkedHashMap<>();
            textPart.put("text", p.getPrompt());
            parts.add(textPart);
        }
        return Mono.just(parts);
    }

    /**
     * 把只有 base64 的输入图上传 OSS 换签名 URL（渠道接口需要图片地址）。
     * 上传失败回退 data URI，保证请求不被 OSS 故障阻塞。
     */
    private Mono<Map<String, Object>> uploadImageToUrl(LlmImageInput in) {
        String mime = in.getMimeType() != null ? in.getMimeType() : "image/png";
        byte[] data = Base64.getDecoder().decode(in.getBase64Data());
        return Mono.deferContextual(ctxView -> {
            Long userId = ctxView.getOrDefault("userId", null);
            return ossService.uploadBytes(data, mime, userId)
                .map(result -> {
                    log.info("[{}] 输入图已上传 OSS: url={}", getProviderName(), result.getUrl());
                    Map<String, Object> part = new LinkedHashMap<>();
                    part.put("image", result.getUrl());
                    return part;
                })
                .onErrorResume(e -> {
                    log.warn("[{}] 输入图上传 OSS 失败，回退 data URI: err={}", getProviderName(), e.getMessage());
                    Map<String, Object> part = new LinkedHashMap<>();
                    part.put("image", "data:" + mime + ";base64," + in.getBase64Data());
                    return Mono.just(part);
                });
        });
    }

    /**
     * 客户端要求 b64_json 而上游只返回 URL 时，下载并转 base64
     */
    private Mono<LlmChatResponse> maybeDownloadToBase64(LlmChatResponse resp, LlmChatRequest request) {
        LlmImageParams p = request.getImageParams();
        boolean wantB64 = p != null && "b64_json".equalsIgnoreCase(p.getOutputFormat());
        if (!wantB64 || resp.getImages() == null || resp.getImages().isEmpty()) {
            return Mono.just(resp);
        }
        boolean hasUrlOnly = resp.getImages().stream()
            .anyMatch(img -> img.getB64Json() == null && img.getUrl() != null);
        if (!hasUrlOnly) {
            return Mono.just(resp);
        }

        return Flux.fromIterable(resp.getImages())
            .flatMap(img -> {
                if (img.getB64Json() != null || img.getUrl() == null) {
                    return Mono.just(img);
                }
                // 用无鉴权的独立 WebClient 下载 OSS/CDN 图片，避免把 DashScope 的 Bearer key 发给 OSS 主机
                return IMAGE_DOWNLOAD_CLIENT.get().uri(img.getUrl()).retrieve().bodyToMono(byte[].class)
                    .map(bytes -> {
                        img.setB64Json(Base64.getEncoder().encodeToString(bytes));
                        return img;
                    })
                    .onErrorResume(e -> {
                        log.warn("[{}] 下载图片转 base64 失败，保留 url: url={}, err={}",
                            getProviderName(), img.getUrl(), e.getMessage());
                        return Mono.just(img);
                    });
            })
            .collectList()
            .map(images -> resp);
    }

    /**
     * DashScope multimodal-generation 响应 → 内部标准响应
     * {output:{choices:[{message:{content:[{image:url}]}}]}, usage:{image_count,width,height}, request_id}
     */
    private LlmChatResponse parseResponse(JsonNode root, LlmChatRequest request) {
        LlmChatResponse resp = new LlmChatResponse();
        resp.setId(root.has("request_id") ? root.get("request_id").asText() : "img-" + System.currentTimeMillis());

        List<LlmImage> images = new ArrayList<>();
        JsonNode output = root.path("output");
        JsonNode choices = output.path("choices");
        if (choices.isArray()) {
            for (JsonNode choice : choices) {
                JsonNode content = choice.path("message").path("content");
                if (content.isArray()) {
                    for (JsonNode c : content) {
                        if (c.has("image")) {
                            images.add(LlmImage.builder()
                                .url(c.get("image").asText())
                                .index(images.size())
                                .build());
                        }
                    }
                }
            }
        }
        resp.setImages(images);

        LlmChatResponse.Usage usage = new LlmChatResponse.Usage();
        JsonNode usageNode = root.path("usage");
        int imageCount = usageNode.path("image_count").asInt(0);
        usage.setImageCount(imageCount > 0 ? imageCount : images.size());

        // DashScope 也返回 input_tokens/output_tokens/total_tokens（qwen-image 实测返回），
        // 必须填充，否则对外响应 usage 全 0、与 Azure/OpenAI 的图像接口格式不一致。
        usage.setPromptTokens(usageNode.path("input_tokens").asInt(0));
        usage.setCompletionTokens(usageNode.path("output_tokens").asInt(0));
        usage.setTotalTokens(usageNode.path("total_tokens").asInt(0));
        // 输入 token 归入 text 维度（生图输入即文本 prompt，语义准确；编辑的图文混合无法细分，
        // 按聚合值近似），这样 input_tokens_details 块也能正常输出，三渠道响应结构一致。
        usage.setInputTextTokens(usage.getPromptTokens());

        resp.setUsage(usage);
        return resp;
    }

    /**
     * OpenAI 尺寸（1024x1024）→ DashScope 尺寸（1024*1024）
     * DashScope qwen-image 的 size 用 * 分隔，OpenAI 用 x。
     */
    private String sizeToDashScope(String size) {
        return size != null ? size.replace('x', '*') : null;
    }

    /**
     * 生效端点配置：DB 配置缺失或命中通用默认（chat/completions）时，
     * 用本渠道默认路径构建（doPostBlocking 内部优先使用 endpointConfig.getFullUrl()）。
     */
    private ModelEndpointConfig effectiveEndpoint(ModelEndpointConfig ec) {
        if (ec != null && ec.getEndpointPath() != null && !ec.getEndpointPath().isBlank()
                && !ec.getEndpointPath().equals(DEFAULT_PATH)
                && !ec.getEndpointPath().equals("chat/completions")) {
            return ec;
        }
        return ModelEndpointConfig.builder()
                .baseUrl(baseUrl)
                .endpointPath(DEFAULT_PATH)
                .httpMethod("POST")
                .build();
    }
}
