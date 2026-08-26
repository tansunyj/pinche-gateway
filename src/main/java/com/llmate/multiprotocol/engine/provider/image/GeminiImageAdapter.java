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
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Gemini 图像渠道 Provider（nano banana / vapeur 等 Gemini 兼容图像端点）
 *
 * 走单接口 generateContent（生成与编辑都路由到这里，参数不同）：
 * - 生图：contents.parts 只含 {text}
 * - 编辑：contents.parts 追加 {inlineData:{mimeType,data}} 或 {fileData:{fileUri,mimeType}}（输入图）
 * - 生图带参考图（客户端通过 images 数组传多张，如 Gemini 多区域替换/合并场景）：
 *   与编辑一致，把输入图一并追加进 parts —— 否则参考图会被静默丢弃，模型看不到。
 *
 * 端点路径：默认 gemini/v1beta/models/{model}:generateContent（{model} 用上游模型名替换），
 * 可用 DB 配置的 endpointPath 覆盖。
 */
@Log4j2
public class GeminiImageAdapter extends AbstractProviderAdapter {

    private static final String DEFAULT_PATH = "gemini/v1beta/models/{model}:generateContent";
    /**
     * 图片 URL → base64 的下载客户端（独立无鉴权 WebClient，避免把渠道 key 发给图片主机）。
     * 必须跟随重定向（301/302）：图床会把 URL 302 跳到 CDN，不跟随会拿到空 body → base64 空串。
     * 统一由 WebClientUtils 提供（newConnection 禁连接池 + 跟随重定向 + 5 分钟超时）。
     */
    private static final WebClient REDIRECT_FOLLOWING_CLIENT = WebClientUtils.imageDownloadClient();

    private final String providerName;
    private final String providerAlias;

    public GeminiImageAdapter(String baseUrl, String apiKey, String name, String alias, ObjectMapper objectMapper,
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
     * 单接口生成/编辑（edit=true 或携带参考图时在 parts 追加输入图）。
     * parts 构建为响应式：URL 输入图需下载转 base64（异步），base64 输入图直接内联。
     */
    private Mono<LlmChatResponse> doGenerate(LlmChatRequest request, ModelEndpointConfig ec, boolean edit) {
        LlmImageParams p = request.getImageParams();
        // 默认路径含 {model} 占位符，需先替换为上游模型名
        String resolvedDefault = DEFAULT_PATH.replace("{model}", request.getModel());
        ModelEndpointConfig effective = effectiveEndpoint(ec, resolvedDefault, request.getModel());

        return buildContents(request, p, edit)
            .map(contents -> {
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("model", request.getModel());
                body.put("contents", contents);
                if (p != null && p.getExtraParams() != null) {
                    if (p.getExtraParams().containsKey("generationConfig")) {
                        body.put("generationConfig", p.getExtraParams().get("generationConfig"));
                    }
                    if (p.getExtraParams().containsKey("systemInstruction")) {
                        body.put("systemInstruction", p.getExtraParams().get("systemInstruction"));
                    }
                }
                return body;
            })
            .doOnNext(body -> log.info("[{}] 请求地址: POST {}", getProviderName(), effective.getFullUrl()))
            // 调用上游接口前打印本次使用的 用户/渠道 API Key（ID + 首尾遮罩），便于排查
            .doOnNext(body -> logUpstreamKeys(request, edit ? "图像编辑" : "图像生成"))
            .flatMap(body -> doPostBlocking(resolvedDefault, body, JsonNode.class, json -> parseResponse(json, request), effective))
            .doOnNext(resp -> log.info("[{}] 图像{}完成: images={}", getProviderName(),
                edit ? "编辑" : "生成", resp.getImages() != null ? resp.getImages().size() : 0))
            .doOnError(e -> logError(edit ? "图像编辑" : "图像生成", e));
    }

    /**
     * 构建 contents：文本在前，输入图在后（parts 顺序即图片编号，与 nano banana 直连请求一致）。
     * 输入图 base64 直接用 inlineData；URL 需下载转 base64（Gemini fileUri 不认普通 http 地址）。
     */
    private Mono<Map<String, Object>> buildContents(LlmChatRequest request, LlmImageParams p, boolean edit) {
        List<Map<String, Object>> textParts = new ArrayList<>();
        if (p != null && p.getPrompt() != null) {
            Map<String, Object> textPart = new LinkedHashMap<>();
            textPart.put("text", p.getPrompt());
            textParts.add(textPart);
        }

        // 编辑（edit=true）必须带输入图；生图请求若客户端通过 images 数组携带了参考图
        // （Gemini 多区域替换/合并场景），同样要拼进 parts，否则参考图被静默丢弃。
        boolean hasImages = p != null && p.getImages() != null && !p.getImages().isEmpty();
        if (!(edit || hasImages) || p == null || p.getImages() == null) {
            return Mono.just(wrapContents(textParts));
        }

        List<Mono<Map<String, Object>>> imgPartMonos = new ArrayList<>();
        for (LlmImageInput in : p.getImages()) {
            if (in.getBase64Data() != null) {
                imgPartMonos.add(Mono.just(inlineDataPart(in.getMimeType(), in.getBase64Data())));
            } else if (in.getUrl() != null) {
                imgPartMonos.add(downloadUrlToInlineData(in));
            }
        }
        return Flux.concat(imgPartMonos).collectList()
            .map(imgParts -> {
                List<Map<String, Object>> all = new ArrayList<>(textParts);
                all.addAll(imgParts);
                return wrapContents(all);
            });
    }

    private Map<String, Object> wrapContents(List<Map<String, Object>> parts) {
        Map<String, Object> contents = new LinkedHashMap<>();
        contents.put("role", "user");
        contents.put("parts", parts);
        return contents;
    }

    private Map<String, Object> inlineDataPart(String mime, String data) {
        Map<String, Object> inlineData = new LinkedHashMap<>();
        inlineData.put("mimeType", mime != null && !mime.isBlank() ? mime : "image/png");
        inlineData.put("data", data);
        Map<String, Object> part = new LinkedHashMap<>();
        part.put("inlineData", inlineData);
        return part;
    }

    /**
     * 下载图片 URL 转 base64 → inlineData part。
     * Gemini/vapeur 的 fileUri 只认 GCS / Files API URI，普通 http 地址必须转 base64 走 inlineData。
     */
    private Mono<Map<String, Object>> downloadUrlToInlineData(LlmImageInput in) {
        return REDIRECT_FOLLOWING_CLIENT.get().uri(in.getUrl()).retrieve().toEntity(byte[].class)
            .map(resp -> {
                // 顺带从响应 Content-Type 取真实 mime（如 image/jpeg），避免把 jpeg 标成 png
                MediaType ct = resp.getHeaders().getContentType();
                String mime = ct != null ? ct.toString() : in.getMimeType();
                return inlineDataPart(mime, Base64.getEncoder().encodeToString(Objects.requireNonNull(resp.getBody())));
            })
            .onErrorResume(e -> {
                log.warn("[{}] 下载图片 URL 转 base64 失败: url={}, err={}", getProviderName(), in.getUrl(), e.getMessage());
                return Mono.error(new IllegalStateException("[图像编辑] 无法下载图片 URL: " + in.getUrl()));
            });
    }

    /**
     * Gemini generateContent 响应 → 内部标准响应
     * {candidates:[{content:{parts:[{inlineData:{mimeType,data}}]}}], usageMetadata:{...}, responseId}
     */
    private LlmChatResponse parseResponse(JsonNode root, LlmChatRequest request) {
        LlmChatResponse resp = new LlmChatResponse();
        resp.setId(root.has("responseId") ? root.get("responseId").asText() : "img-" + System.currentTimeMillis());

        List<LlmImage> images = new ArrayList<>();
        JsonNode candidates = root.path("candidates");
        if (candidates.isArray()) {
            for (JsonNode candidate : candidates) {
                JsonNode parts = candidate.path("content").path("parts");
                if (parts.isArray()) {
                    for (JsonNode part : parts) {
                        JsonNode inlineData = part.path("inlineData");
                        if (!inlineData.isMissingNode() && inlineData.has("data")) {
                            images.add(LlmImage.builder()
                                .b64Json(inlineData.get("data").asText())
                                .contentType(inlineData.path("mimeType").asText(null))
                                .index(images.size())
                                .build());
                        }
                    }
                }
            }
        }
        resp.setImages(images);

        LlmChatResponse.Usage usage = new LlmChatResponse.Usage();
        JsonNode usageMeta = root.path("usageMetadata");
        usage.setPromptTokens(usageMeta.path("promptTokenCount").asInt(0));
        usage.setCompletionTokens(usageMeta.path("candidatesTokenCount").asInt(0));
        usage.setTotalTokens(usageMeta.path("totalTokenCount").asInt(0));
        usage.setImageCount(images.size());

        // 按 modality 拆出 text/image token 维度（image_token 计费的 input/output 细分）
        // 不填充会令 image_token 模式费用恒为 0。
        parseModalityTokens(usageMeta.path("promptTokensDetails"), true, usage);
        parseModalityTokens(usageMeta.path("candidatesTokensDetails"), false, usage);

        resp.setUsage(usage);
        return resp;
    }

    /**
     * 解析 usageMetadata 的 tokensDetails，按 modality 拆出 text/image token 维度
     * @param details promptTokensDetails 或 candidatesTokensDetails
     * @param isInput true=输入（prompt），false=输出（candidates）
     */
    private void parseModalityTokens(JsonNode details, boolean isInput, LlmChatResponse.Usage usage) {
        if (!details.isArray()) {
            return;
        }
        for (JsonNode d : details) {
            String modality = d.path("modality").asText("");
            int count = d.path("tokenCount").asInt(0);
            boolean image = "IMAGE".equalsIgnoreCase(modality);
            if (isInput) {
                if (image) {
                    usage.setInputImageTokens(usage.getInputImageTokens() + count);
                } else {
                    usage.setInputTextTokens(usage.getInputTextTokens() + count);
                }
            } else {
                if (image) {
                    usage.setOutputImageTokens(usage.getOutputImageTokens() + count);
                } else {
                    usage.setOutputTextTokens(usage.getOutputTextTokens() + count);
                }
            }
        }
    }

    /**
     * 生效端点配置：DB 配置缺失或命中通用默认（chat/completions）时，
     * 用替换了 {model} 的本渠道默认路径构建（doPostBlocking 内部优先使用 endpointConfig.getFullUrl()）。
     * DB 配置的端点路径也可能含 {model} 占位符（如 /gemini/v1beta/models/{model}:generateContent，
     * 可能带前导斜杠而 ≠ DEFAULT_PATH），同样必须替换为上游模型名，否则 WebClient 展开 URI 模板报
     * "Not enough variable values available to expand 'model'"。
     */
    private ModelEndpointConfig effectiveEndpoint(ModelEndpointConfig ec, String resolvedDefault, String model) {
        if (ec != null && ec.getEndpointPath() != null && !ec.getEndpointPath().isBlank()
                && !ec.getEndpointPath().equals("chat/completions")) {
            // 若路径不含 {model}，replace 是 no-op，原样保留 DB 自定义路径
            String path = ec.getEndpointPath().replace("{model}", model);
            return ModelEndpointConfig.builder()
                    .baseUrl(ec.getBaseUrl() != null ? ec.getBaseUrl() : baseUrl)
                    .endpointPath(path)
                    .httpMethod(ec.getHttpMethod())
                    .build();
        }
        return ModelEndpointConfig.builder()
                .baseUrl(baseUrl)
                .endpointPath(resolvedDefault)
                .httpMethod("POST")
                .build();
    }
}
