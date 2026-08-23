package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.converter.ImageProtocolConverter;
import com.llmate.multiprotocol.dto.LlmImageInput;
import com.llmate.multiprotocol.dto.LlmRequestType;
import com.llmate.multiprotocol.dto.openai.OpenAiImageRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiImageResponse;
import com.llmate.multiprotocol.engine.LlmGateway;
import com.llmate.multiprotocol.engine.ProtocolManager;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import lombok.extern.log4j.Log4j2;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.http.codec.multipart.FormFieldPart;
import org.springframework.http.codec.multipart.Part;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;

/**
 * 图像生成 / 编辑控制器（MVC 分层：仅 HTTP 层，业务走 LlmGateway 引擎）
 *
 * - POST /v1/images/generations：OpenAI 兼容 JSON 生图
 * - POST /v1/images/edits：multipart 编辑（照老项目 ImageGenerationController 参数设计）：
 *   image（单张原图，必填）/ mask（蒙版，可选）/ prompt（必填）/ model（必填），仅此四参。
 *
 * 设计背景：只有 Azure 渠道有独立的 /images/edits 端点；qwen / nano banana 没有独立编辑接口，
 * 由各自 Adapter 复用单端点（multimodal-generation / generateContent）并把输入图带进请求体。
 * 因此对外编辑入口统一成这份最简 multipart 参数，按 model 路由到对应渠道。
 *
 * 均通过 {@link ImageProtocolConverter} 转内部请求 → {@link LlmGateway#execute} 路由到各渠道
 * ProviderAdapter（OpenAI/Azure、DashScope qwen-image、Gemini nano banana）。
 */
@RestController
@RequestMapping("/v1")
@RequireApiKey
@Log4j2
public class ImageController {

    /** 编辑接口允许的图片 MIME（照老项目 validateImageFile 白名单） */
    private static final Set<String> ALLOWED_IMAGE_MIMES = Set.of(
            "image/png", "image/jpeg", "image/jpg", "image/webp", "image/gif");

    private final ImageProtocolConverter converter;
    private final LlmGateway gateway;
    private final ProtocolManager protocolManager;

    public ImageController(ImageProtocolConverter converter, LlmGateway gateway, ProtocolManager protocolManager) {
        this.converter = converter;
        this.gateway = gateway;
        this.protocolManager = protocolManager;
    }

    /**
     * 图像生成（OpenAI 兼容 JSON）：POST /v1/images/generations
     */
    @PostMapping(value = "/images/generations", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<OpenAiImageResponse>> generations(@RequestBody OpenAiImageRequest request, ServerWebExchange exchange) {
        protocolManager.bindProtocol(exchange, ProtocolType.OPENAI_IMAGES);

        // 本轮只做同步非流式（用户已确认）
        if (Boolean.TRUE.equals(request.getStream())) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "图片流式生成暂未支持，请去掉 stream:true"));
        }

        // 图生图 = 图像编辑：请求携带输入图（images[] / imageInputs）时按 IMAGE_EDIT 处理，否则纯生图。
        // 这样 Azure/OpenAI 也能走 JSON 带图编辑（OpenAiImageAdapter.generateImage 的 buildGenerationBody
        // 不带图，图会被静默丢弃）；Gemini/DashScope 的 editImage 与 generateImage 走同一 doGenerate，
        // 仅日志标签不同，行为不变（见各自 adapter：edit || hasImages 判定）。
        boolean hasImages = (request.getImageInputs() != null && !request.getImageInputs().isEmpty())
                || (request.getImages() != null && !request.getImages().isEmpty());
        LlmRequestType type = hasImages ? LlmRequestType.IMAGE_EDIT : LlmRequestType.IMAGE_GENERATION;

        return converter.toInternalRequest(request, type)
            .flatMap(internal -> gateway.execute(internal, exchange))
            .map(converter::toExternalResponse)
            .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp))
            .doOnError(e -> log.error("[ImageController] 图像生成失败", e));
    }

    /**
     * 图像编辑（multipart）：POST /v1/images/edits (Content-Type: multipart/form-data)
     * 照老项目 ImageGenerationController 编辑接口参数设计，四个字段：
     *   image（单张原图，必填）、mask（蒙版，可选）、prompt（编辑提示词，必填）、model（模型ID，必填）
     * 响应仍为 OpenAI images 格式。
     */
    @PostMapping(value = "/images/edits", consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<OpenAiImageResponse>> edits(ServerWebExchange exchange) {
        protocolManager.bindProtocol(exchange, ProtocolType.OPENAI_IMAGES);
        log.info("[ImageController] 编辑请求开始: contentType={}", exchange.getRequest().getHeaders().getContentType());

        return exchange.getMultipartData()
            .doOnNext(multipart -> log.info("[ImageController] getMultipartData 成功: parts={}", multipart.size()))
            .switchIfEmpty(Mono.fromRunnable(() -> log.error("[ImageController] getMultipartData 返回空Mono！"))
                .then(Mono.empty()))
            .flatMap(this::buildEditRequest)
            .doOnNext(req -> log.info("[ImageController] buildEditRequest 完成: model={}, prompt={}, imageCount={}, hasMask={}",
                req.getModel(), req.getPrompt(),
                req.getImageInputs() != null ? req.getImageInputs().size() : 0,
                req.getMaskInput() != null))
            .flatMap(req -> converter.toInternalRequest(req, LlmRequestType.IMAGE_EDIT))
            .flatMap(internal -> gateway.execute(internal, exchange))
            .map(converter::toExternalResponse)
            .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp))
            .doOnSuccess(resp -> log.info("[ImageController] 图像编辑响应完成"))
            .doOnError(e -> log.error("[ImageController] 图像编辑失败", e));
    }

    /**
     * 图像编辑（JSON 格式）：POST /v1/images/edits (Content-Type: application/json)
     * 与上方 multipart 的 edits() 同路径，按 Content-Type 分派（JSON → 本方法，multipart → edits()）。
     *
     * 支持 OpenAI 新版编辑请求体：images 数组（每个元素 image_url 可为 base64 data URI 或普通 URL）
     * + prompt + model + size/quality/output_format/n 等可选参数。示例：
     * {"prompt":"把椅子改成深蓝色","model":"azure/gpt-image-2","size":"1024x1024",
     *  "quality":"low","output_format":"png","n":1,"images":["data:image/jpeg;base64,..."]}
     * images[] 的 data URI 解析在 ImageProtocolConverter.mergeImageInputs 完成（解出 base64 + mime）。
     */
    @PostMapping(value = "/images/edits", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<OpenAiImageResponse>> editsJson(@RequestBody OpenAiImageRequest request,
                                                               ServerWebExchange exchange) {
        protocolManager.bindProtocol(exchange, ProtocolType.OPENAI_IMAGES);

        if (Boolean.TRUE.equals(request.getStream())) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "图片流式编辑暂未支持，请去掉 stream:true"));
        }
        if (request.getModel() == null || request.getModel().isBlank()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "缺少 model 字段"));
        }
        if (request.getPrompt() == null || request.getPrompt().isBlank()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "prompt 不能为空"));
        }
        boolean hasImage = (request.getImageInputs() != null && !request.getImageInputs().isEmpty())
                || (request.getImages() != null && !request.getImages().isEmpty());
        if (!hasImage) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST,
                    "JSON 编辑缺少输入图：请用 images=[{\"image_url\":\"data:image/png;base64,...\"}] 传 base64 图片引用"));
        }

        log.info("[ImageController] JSON编辑请求开始: model={}, prompt={}, images={}",
                request.getModel(), request.getPrompt(),
                request.getImages() != null ? request.getImages().size() : 0);

        return converter.toInternalRequest(request, LlmRequestType.IMAGE_EDIT)
                .flatMap(internal -> gateway.execute(internal, exchange))
                .map(converter::toExternalResponse)
                .map(resp -> ResponseEntity.ok().contentType(MediaType.APPLICATION_JSON).body(resp))
                .doOnSuccess(resp -> log.info("[ImageController] JSON图像编辑响应完成"))
                .doOnError(e -> log.error("[ImageController] JSON图像编辑失败", e));
    }

    // ==================== multipart 组装 ====================

    /**
     * 从 multipart 表单组装编辑请求（兼容多图）：
     * - 输入图：`image`（单张，老项目命名）或 `image[]`（多张，OpenAI 命名），可混用，顺序=提交顺序
     * - 蒙版：`mask`（单张，可选）
     * - 文本：`prompt` / `model`
     */
    private Mono<OpenAiImageRequest> buildEditRequest(MultiValueMap<String, Part> multipart) {
        String model = fieldValue(multipart, "model");
        String prompt = fieldValue(multipart, "prompt");
        List<FilePart> images = collectImageParts(multipart);
        FilePart mask = firstFilePart(multipart, "mask");

        // 基础参数校验（照老项目 editImage：prompt/model/image 必填）
        if (model == null || model.isBlank()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "缺少 model 字段"));
        }
        if (prompt == null || prompt.isBlank()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "prompt 不能为空"));
        }
        if (images.isEmpty()) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST, "image 不能为空"));
        }
        for (FilePart img : images) {
            if (!isAllowedImageMime(img)) {
                return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST,
                    "image 仅支持 png/jpeg/webp/gif，当前类型: " + mimeOf(img)));
            }
        }
        if (mask != null && !isAllowedImageMime(mask)) {
            return Mono.error(new LlmGatewayException(LlmErrorCode.INVALID_REQUEST,
                "mask 仅支持 png/jpeg/webp/gif，当前类型: " + mimeOf(mask)));
        }

        // 逐张读字节 → base64 + mime，保持 image 在前、image[] 在后的提交顺序
        return Flux.fromIterable(images)
            .concatMap(img -> readFilePart(img).map(bytes -> LlmImageInput.builder()
                .base64Data(Base64.getEncoder().encodeToString(bytes))
                .mimeType(mimeOf(img))
                .build()))
            .collectList()
            .flatMap(imageInputs -> {
                OpenAiImageRequest req = OpenAiImageRequest.builder()
                    .model(model)
                    .prompt(prompt)
                    .stream(false)
                    .imageInputs(imageInputs)
                    .build();
                if (mask != null) {
                    return readFilePart(mask).map(maskBytes -> {
                        req.setMaskInput(LlmImageInput.builder()
                            .base64Data(Base64.getEncoder().encodeToString(maskBytes))
                            .mimeType(mimeOf(mask))
                            .build());
                        return req;
                    });
                }
                return Mono.just(req);
            });
    }

    /**
     * 收集全部输入图 FilePart：兼容 `image`（单张）与 `image[]`（多张）两种命名，
     * image 在前、image[] 在后，保证顺序稳定。
     */
    private static List<FilePart> collectImageParts(MultiValueMap<String, Part> multipart) {
        List<FilePart> images = new ArrayList<>();
        for (String name : List.of("image", "image[]")) {
            List<Part> parts = multipart.get(name);
            if (parts != null) {
                for (Part p : parts) {
                    if (p instanceof FilePart fp) {
                        images.add(fp);
                    }
                }
            }
        }
        return images;
    }

    private static String fieldValue(MultiValueMap<String, Part> multipart, String name) {
        Part part = multipart.getFirst(name);
        if (part instanceof FormFieldPart ffp) {
            return ffp.value();
        }
        return null;
    }

    private static FilePart firstFilePart(MultiValueMap<String, Part> multipart, String name) {
        List<Part> parts = multipart.get(name);
        if (parts != null) {
            for (Part p : parts) {
                if (p instanceof FilePart fp) {
                    return fp;
                }
            }
        }
        return null;
    }

    private static Mono<byte[]> readFilePart(FilePart part) {
        return DataBufferUtils.join(part.content()).map(db -> {
            ByteBuffer bb = db.toByteBuffer();
            byte[] bytes = new byte[bb.remaining()];
            bb.get(bytes);
            DataBufferUtils.release(db);
            return bytes;
        });
    }

    private static String mimeOf(FilePart part) {
        if (part.headers().getContentType() == null) {
            return "image/png";
        }
        String mime = part.headers().getContentType().toString();
        int semi = mime.indexOf(';');
        return semi > 0 ? mime.substring(0, semi).trim() : mime;
    }

    private static boolean isAllowedImageMime(FilePart part) {
        return ALLOWED_IMAGE_MIMES.contains(mimeOf(part).toLowerCase());
    }
}
