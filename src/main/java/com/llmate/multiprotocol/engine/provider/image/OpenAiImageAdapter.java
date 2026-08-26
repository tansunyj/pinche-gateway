package com.llmate.multiprotocol.engine.provider.image;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmImage;
import com.llmate.multiprotocol.dto.LlmImageInput;
import com.llmate.multiprotocol.dto.LlmImageParams;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.openai.OpenAiImageResponse;
import com.llmate.multiprotocol.engine.provider.AbstractProviderAdapter;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.MultipartBodyBuilder;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * OpenAI / Azure 图像渠道 Provider
 *
 * 兼容 OpenAI images/generations（JSON）与 images/edits（multipart）两个端点，
 * 分别对应内部请求类型 IMAGE_GENERATION / IMAGE_EDIT。Azure 部署路径由渠道 base_url 承载
 * （如 https://xxx.cognitiveservices.azure.com/openai/deployments/gpt-image-2），
 * 路径为 images/generations、images/edits。
 *
 * 端点解析：DB 配置的 endpointPath（model_channel_configs / model_templates）优先；
 * 编辑命中"生图默认路径"时切换为编辑路径（两操作路径不同）。
 */
@Log4j2
public class OpenAiImageAdapter extends AbstractProviderAdapter {

    private static final String GEN_PATH = "v1/images/generations";
    private static final String EDIT_PATH = "v1/images/edits";
    /**
     * 图片 URL → base64 的下载客户端（独立无鉴权 WebClient，避免把渠道 key 发给图片主机）。
     * 必须跟随重定向（301/302）：picsum 等图床会把 picsum.photos 302 跳到 CDN，
     * 不跟随会拿到空 body → base64 空串 → 上游报 "Missing required parameter: 'image'"。
     * 统一由 WebClientUtils 提供（newConnection 禁连接池 + 跟随重定向 + 5 分钟超时）。
     */
    private static final WebClient REDIRECT_FOLLOWING_CLIENT = WebClientUtils.imageDownloadClient();

    private final String providerName;
    private final String providerAlias;

    public OpenAiImageAdapter(String baseUrl, String apiKey, String name, String alias, ObjectMapper objectMapper,
                              List<String> apiKeys, List<Long> tokenIds) {
        // 双头认证（与 AzureProviderAdapter 一致）：api-key 供 Azure，Bearer 供 OpenAI 原生。
        // 同时发送互不影响——OpenAI 忽略未知头，Azure 使用 api-key 头。
        super(baseUrl, WebClient.builder()
                .defaultHeader("api-key", apiKey)
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey), objectMapper, apiKeys, tokenIds);
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

    // ==================== 文本聊天（图像渠道不支持） ====================

    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest request) {
        throw new UnsupportedOperationException("图像渠道不支持文本聊天: " + getProviderName());
    }

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest request) {
        throw new UnsupportedOperationException("图像渠道不支持文本流式聊天: " + getProviderName());
    }

    // ==================== 图像生成 ====================

    @Override
    public Mono<LlmChatResponse> generateImage(LlmChatRequest request) {
        return generateImage(request, null);
    }

    @Override
    public Mono<LlmChatResponse> generateImage(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        // DB 配置的 endpointPath 可能含 {model} 占位符（如 /openai/deployments/{model}/images/generations），
        // 必须先替换为上游模型名，否则 WebClient 展开 URI 模板报 "Not enough variable values available to expand 'model'"。
        ModelEndpointConfig effective = resolveModelPlaceholder(endpointConfig, request.getModel());
        Map<String, Object> body = buildGenerationBody(request);
        logRequest("图像生成", fullUrl(effective, GEN_PATH), body);
        // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
        logUpstreamKeys(request, "图像生成");

        // doPostBlocking 内部优先使用 endpointConfig.getFullUrl()（DB 解析结果），
        // 无配置时退回 uri=GEN_PATH 相对 baseUrl 拼接
        return doPostBlocking(GEN_PATH, body, OpenAiImageResponse.class, this::toInternalResponse, effective)
            .doOnNext(resp -> log.info("[{}] 图像生成完成: images={}", getProviderName(),
                resp.getImages() != null ? resp.getImages().size() : 0))
            .doOnError(e -> logError("图像生成", e));
    }

    // ==================== 图像编辑（multipart） ====================

    @Override
    public Mono<LlmChatResponse> editImage(LlmChatRequest request) {
        return editImage(request, null);
    }

    @Override
    public Mono<LlmChatResponse> editImage(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        // DB 配置的 endpointPath 可能含 {model} 占位符，先替换再切换为编辑路径
        ModelEndpointConfig modelReplaced = resolveModelPlaceholder(endpointConfig, request.getModel());
        ModelEndpointConfig effective = editEndpointConfig(modelReplaced);
        LlmImageParams p = request.getImageParams();
        boolean isAzure = baseUrl != null && baseUrl.contains("azure.com");

        // Azure/OpenAI multipart 需要图片字节：只有 url 的输入图先下载转 base64（反应式），再组 multipart
        List<LlmImageInput> inputs = p != null ? p.getImages() : null;
        return resolveInputImages(inputs)
            .flatMap(resolved -> {
                MultipartBodyBuilder mb = buildEditMultipart(request, p, resolved, isAzure);
                String logSummary = buildMultipartLogSummary(request, resolved, p, isAzure);
                logRequest("图像编辑", effective.getFullUrl(), logSummary);
                // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
                logUpstreamKeys(request, "图像编辑");
                return postMultipart(mb, effective, logSummary);
            })
            .doOnNext(resp -> log.info("[{}] 图像编辑完成: images={}", getProviderName(),
                resp.getImages() != null ? resp.getImages().size() : 0))
            .doOnError(e -> logError("图像编辑", e));
    }

    /**
     * 构建编辑 multipart：Azure 只发 image(单数)/mask/prompt（不发 model/n/size），与 curl 一致；
     * 原生 OpenAI 发 image[]（数组）+ model + prompt。输入图用已解析（base64 补齐）的列表。
     * 可选编辑参数（size/quality/output_format/n）在请求携带时透传（JSON 编辑格式会带，
     * multipart 存量请求不带 → 天然不拼入，不影响既有行为）。
     */
    private MultipartBodyBuilder buildEditMultipart(LlmChatRequest request, LlmImageParams p,
                                                    List<LlmImageInput> resolved, boolean isAzure) {
        MultipartBodyBuilder mb = new MultipartBodyBuilder();
        if (p == null) {
            return mb;
        }
        if (isAzure) {
            if (p.getPrompt() != null) mb.part("prompt", p.getPrompt());
            if (resolved != null) {
                int idx = 0;
                for (LlmImageInput in : resolved) {
                    byte[] data = toBytes(in);
                    String mime = in.getMimeType() != null ? in.getMimeType() : "image/png";
                    mb.part("image", asResource(data, "image_" + idx + "." + extOf(mime)), MediaType.parseMediaType(mime));
                    idx++;
                }
            }
            addMaskPart(mb, p);
        } else {
            if (p.getPrompt() != null) mb.part("prompt", p.getPrompt());
            mb.part("model", request.getModel());
            if (resolved != null) {
                int idx = 0;
                for (LlmImageInput in : resolved) {
                    byte[] data = toBytes(in);
                    String mime = in.getMimeType() != null ? in.getMimeType() : "image/png";
                    mb.part("image[]", asResource(data, "image_" + idx + "." + extOf(mime)), MediaType.parseMediaType(mime));
                    idx++;
                }
            }
            addMaskPart(mb, p);
        }
        addEditParams(mb, p);
        return mb;
    }

    /**
     * 可选编辑参数透传（仅请求携带时拼入 form 字段）：
     * size / quality / output_format / n 由 JSON 编辑格式（OpenAiImageRequest）显式传入，
     * multipart 存量请求不填这些字段 → 不影响既有 Azure/OpenAI 编辑行为。
     */
    private void addEditParams(MultipartBodyBuilder mb, LlmImageParams p) {
        if (p.getN() != null) mb.part("n", p.getN());
        if (p.getSize() != null) mb.part("size", p.getSize());
        if (p.getQuality() != null) mb.part("quality", p.getQuality());
        if (p.getOutputFormat() != null) mb.part("output_format", p.getOutputFormat());
    }

    private void addMaskPart(MultipartBodyBuilder mb, LlmImageParams p) {
        if (p.getMask() != null && p.getMask().getBase64Data() != null) {
            byte[] data = toBytes(p.getMask());
            String mime = p.getMask().getMimeType() != null ? p.getMask().getMimeType() : "image/png";
            mb.part("mask", asResource(data, "mask." + extOf(mime)), MediaType.parseMediaType(mime));
        }
    }

    /**
     * 解析编辑输入图：已有 base64 直接用；只有 url 的下载转 base64（multipart 需要字节）。
     * 保证切换模型（含 Azure/OpenAI）后，url 引用的输入图也能直接调用。
     */
    private Mono<List<LlmImageInput>> resolveInputImages(List<LlmImageInput> images) {
        if (images == null || images.isEmpty()) {
            return Mono.just(images != null ? images : Collections.emptyList());
        }
        List<Mono<LlmImageInput>> monos = new ArrayList<>();
        for (LlmImageInput in : images) {
            if (in.getBase64Data() != null) {
                monos.add(Mono.just(in));
            } else if (in.getUrl() != null) {
                monos.add(downloadUrlToInput(in));
            } else {
                monos.add(Mono.error(new IllegalStateException("[图像编辑] 图片缺少 base64 和 url，无法提交 multipart")));
            }
        }
        return Flux.concat(monos).collectList();
    }

    private Mono<LlmImageInput> downloadUrlToInput(LlmImageInput in) {
        return REDIRECT_FOLLOWING_CLIENT.get().uri(in.getUrl()).retrieve().toEntity(byte[].class)
            .map(resp -> {
                LlmImageInput resolved = new LlmImageInput();
                resolved.setUrl(in.getUrl());
                // 顺带从响应 Content-Type 取真实 mime（如 image/jpeg），避免 multipart 把 jpeg 标成 png
                MediaType ct = resp.getHeaders().getContentType();
                resolved.setMimeType(ct != null ? ct.toString() : in.getMimeType());
                resolved.setBase64Data(Base64.getEncoder().encodeToString(Objects.requireNonNull(resp.getBody())));
                return resolved;
            })
            .onErrorResume(e -> {
                log.warn("[{}] 下载图片 URL 转 base64 失败: url={}, err={}", getProviderName(), in.getUrl(), e.getMessage());
                return Mono.error(new IllegalStateException("[图像编辑] 无法下载图片 URL: " + in.getUrl()));
            });
    }

    /**
     * 编辑端点配置：将路径中的 images/generations → images/edits，修正 Azure 域名，并升级 api-version。
     *
     * 三个必要修正（缺一即 404/400）：
     * 1. 域名：Azure 的 images/edits 端点在 {name}.cognitiveservices.azure.com 上，
     *    不在 {name}.openai.azure.com（生图域名）上。
     * 2. 路径：images/generations → images/edits。
     * 3. api-version：2024-02-01 只有 generations 路由，没有 edits 路由，
     *    必须升级到支持图像编辑的版本（2025-04-01-preview）。
     */
    private ModelEndpointConfig editEndpointConfig(ModelEndpointConfig ec) {
        if (ec != null && ec.getEndpointPath() != null && !ec.getEndpointPath().isBlank()) {
            String original = ec.getEndpointPath();
            String editPath = original.replace("images/generations", "images/edits");
            if (!editPath.equals(original)) {
                // Azure 图像编辑需要比 2024-02-01 更新的 api-version
                editPath = editPath.replace("api-version=2024-02-01", "api-version=2025-04-01-preview");
                String editBaseUrl = ec.getBaseUrl() != null
                        ? ec.getBaseUrl().replace(".openai.azure.com", ".cognitiveservices.azure.com")
                        : baseUrl;
                return ModelEndpointConfig.builder()
                        .baseUrl(editBaseUrl)
                        .endpointPath(editPath)
                        .httpMethod(ec.getHttpMethod())
                        .build();
            }
        }
        String editBaseUrl = baseUrl != null
                ? baseUrl.replace(".openai.azure.com", ".cognitiveservices.azure.com")
                : baseUrl;
        return ModelEndpointConfig.builder()
                .baseUrl(editBaseUrl)
                .endpointPath(EDIT_PATH + "?api-version=2025-04-01-preview")
                .httpMethod("POST")
                .build();
    }

    /**
     * 生成编辑请求的日志摘要：明确标注 multipart/form-data 及各 form 字段名 + 文件体积，
     * 让日志一眼看出是 form-data 而非 JSON。
     */
    private String buildMultipartLogSummary(LlmChatRequest request, List<LlmImageInput> resolved,
                                            LlmImageParams p, boolean isAzure) {
        StringBuilder sb = new StringBuilder();
        sb.append("multipart/form-data [");
        if (!isAzure) {
            sb.append("model=").append(request.getModel()).append(", ");
        }
        if (resolved != null) {
            int i = 0;
            for (LlmImageInput in : resolved) {
                String mime = in.getMimeType() != null ? in.getMimeType() : "image/png";
                sb.append(isAzure ? "image" : "image[]")
                  .append("=image_").append(i).append('.').append(extOf(mime))
                  .append("(base64 ").append(in.getBase64Data() != null ? in.getBase64Data().length() : 0).append(" chars), ");
                i++;
            }
        }
        if (p != null) {
            if (p.getMask() != null && p.getMask().getBase64Data() != null) {
                String mime = p.getMask().getMimeType() != null ? p.getMask().getMimeType() : "image/png";
                sb.append("mask=mask.").append(extOf(mime)).append("(base64 ")
                  .append(p.getMask().getBase64Data().length()).append(" chars), ");
            }
            if (p.getPrompt() != null) {
                sb.append("prompt=\"").append(p.getPrompt()).append("\"");
            }
        }
        if (sb.charAt(sb.length() - 1) == ' ' && sb.length() > 2 && sb.charAt(sb.length() - 2) == ',') {
            sb.setLength(sb.length() - 2);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * multipart 提交（OpenAI edits 端点），响应解析为内部标准响应
     */
    private Mono<LlmChatResponse> postMultipart(MultipartBodyBuilder mb, ModelEndpointConfig effective, Object logSummary) {
        return Mono.deferContextual(ctxView -> {
            String requestId = ctxView.getOrDefault("requestId", "N/A");
            Long userId = ctxView.getOrDefault("userId", null);

            String fullUrl = effective.getFullUrl();
            LogBox.logUpstreamRequest(getProviderName(), fullUrl, logSummary, requestId, userId);

            return webClient.post()
                    .uri(fullUrl)
                    .body(BodyInserters.fromMultipartData(mb.build()))
                    .retrieve()
                    .bodyToMono(OpenAiImageResponse.class)
                    // 同步打印上游响应日志（maskForLog 脱敏后体积很小，不阻塞 Netty；保证日志顺序确定）
                    .doOnNext(resp -> LogBox.logUpstreamResponse(getProviderName(), maskForLog(resp), requestId, userId))
                    .map(this::toInternalResponse)
                    .doOnError(e -> logError("图像编辑", e));
        });
    }

    // ==================== 工具方法 ====================

    /**
     * 构建生成请求体（OpenAI JSON 格式），剔除 null 字段
     */
    private Map<String, Object> buildGenerationBody(LlmChatRequest request) {
        LlmImageParams p = request.getImageParams();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("model", request.getModel());
        if (p != null) {
            body.put("prompt", p.getPrompt());
            body.put("n", p.getN());
            body.put("size", p.getSize());
            body.put("quality", p.getQuality());
            body.put("style", p.getStyle());
            body.put("output_format", p.getOutputFormat());
            body.put("output_compression", p.getOutputCompression());
            body.put("background", p.getBackground());
            body.put("moderation", p.getModeration());
            body.put("seed", p.getSeed());
            body.put("user", p.getUser());
        }
        body.entrySet().removeIf(e -> e.getValue() == null);
        return body;
    }

    /**
     * OpenAI 上游响应 → 内部标准响应
     */
    private LlmChatResponse toInternalResponse(OpenAiImageResponse upstream) {
        LlmChatResponse resp = new LlmChatResponse();
        resp.setId("img-" + System.currentTimeMillis());

        List<LlmImage> images = new ArrayList<>();
        if (upstream.getData() != null) {
            int idx = 0;
            for (OpenAiImageResponse.ImageData d : upstream.getData()) {
                images.add(LlmImage.builder()
                    .b64Json(d.getB64Json())
                    .url(d.getUrl())
                    .revisedPrompt(d.getRevisedPrompt())
                    .index(idx++)
                    .build());
            }
        }
        resp.setImages(images);

        LlmChatResponse.Usage usage = new LlmChatResponse.Usage();
        usage.setImageCount(images.size());
        if (upstream.getUsage() != null) {
            OpenAiImageResponse.OpenAiImageUsage u = upstream.getUsage();
            usage.setPromptTokens(u.getInputTokens() != null ? u.getInputTokens().intValue() : 0);
            usage.setCompletionTokens(u.getOutputTokens() != null ? u.getOutputTokens().intValue() : 0);
            usage.setTotalTokens(u.getTotalTokens() != null ? u.getTotalTokens().intValue() : 0);
            if (u.getInputTokensDetails() != null) {
                usage.setInputTextTokens(u.getInputTokensDetails().getTextTokens() != null
                    ? u.getInputTokensDetails().getTextTokens() : 0);
                usage.setInputImageTokens(u.getInputTokensDetails().getImageTokens() != null
                    ? u.getInputTokensDetails().getImageTokens() : 0);
            }
            // 关键：gpt-image 的输出 token 即生成的图片 token。
            // 不填充 outputImageTokens 会导致 image_token 计费的 output_image 维度为 0（费用恒为 0）。
            int outTokens = u.getOutputTokens() != null ? u.getOutputTokens().intValue() : 0;
            usage.setOutputImageTokens(outTokens);
        }
        resp.setUsage(usage);
        return resp;
    }

    /**
     * 图片输入 → 字节数组（multipart 需要本地字节；仅支持 base64 输入）
     */
    private byte[] toBytes(LlmImageInput in) {
        if (in.getBase64Data() != null) {
            return Base64.getDecoder().decode(in.getBase64Data());
        }
        throw new IllegalStateException("[图像编辑] 图片缺少 base64 数据，无法提交 multipart");
    }

    /**
     * 字节数组 → 带文件名的 ByteArrayResource（multipart 文件分片必需）
     */
    private Resource asResource(byte[] bytes, String filename) {
        return new ByteArrayResource(bytes) {
            @Override
            public String getFilename() {
                return filename;
            }
        };
    }

    private String extOf(String mime) {
        if (mime == null) {
            return "png";
        }
        return switch (mime.toLowerCase()) {
            case "image/jpeg", "image/jpg" -> "jpg";
            case "image/webp" -> "webp";
            case "image/gif" -> "gif";
            default -> "png";
        };
    }

    /**
     * 解析目标完整 URL：优先 DB 配置（baseUrl + endpointPath），否则 baseUrl + 操作默认路径
     */
    private String fullUrl(ModelEndpointConfig ec, String defaultPath) {
        if (ec != null && ec.getFullUrl() != null && !ec.getFullUrl().isEmpty()) {
            return ec.getFullUrl();
        }
        return baseUrl + (defaultPath.startsWith("/") ? defaultPath.substring(1) : defaultPath);
    }

    /**
     * 替换端点路径中的 {model} 占位符为上游模型名。
     *
     * DB 配置的 endpointPath 可能使用 {model} 占位符（如
     * /openai/deployments/{model}/images/generations），让一个模板复用于多个模型部署。
     * 但 WebClient.uri(String) 会把 {model} 当作 URI 模板变量尝试展开，
     * 未提供变量值时抛 IllegalArgumentException:
     * "Not enough variable values available to expand 'model'"。
     *
     * 若路径不含 {model}，replace 是 no-op，原样保留（兼容 dev 库硬编码模型名的模板）。
     */
    private ModelEndpointConfig resolveModelPlaceholder(ModelEndpointConfig ec, String model) {
        if (ec != null && ec.getEndpointPath() != null && !ec.getEndpointPath().isBlank()
                && ec.getEndpointPath().contains("{model}")) {
            String path = ec.getEndpointPath().replace("{model}", model);
            return ModelEndpointConfig.builder()
                    .baseUrl(ec.getBaseUrl())
                    .endpointPath(path)
                    .httpMethod(ec.getHttpMethod())
                    .build();
        }
        return ec;
    }
}
