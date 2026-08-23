package com.llmate.multiprotocol.converter;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmImageInput;
import com.llmate.multiprotocol.dto.LlmImageParams;
import com.llmate.multiprotocol.dto.LlmRequestType;
import com.llmate.multiprotocol.dto.openai.OpenAiImageRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiImageResponse;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.mapping.ModelMappingResolver;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 图像协议转换器（OpenAI images/generations + images/edits 双向转换）
 *
 * 独立组件，不实现泛型 ProtocolConverter（图像协议不走通用聊天转换链路）。
 * 请求方向：外部 OpenAiImageRequest → 内部 LlmChatRequest，
 *   model 处理逻辑与文本聊天一致 —— 先经 {@link ModelMappingResolver} 映射
 *   （YAML model-mappings 可把外部模型名映射成 "渠道/模型"），再交给 ModelRouter 按渠道前缀/纯模型ID解析路由；
 * 响应方向：内部 LlmChatResponse → OpenAI 兼容 OpenAiImageResponse。
 */
@Component
public class ImageProtocolConverter {

    private final ModelMappingResolver mappingResolver;

    public ImageProtocolConverter(ModelMappingResolver mappingResolver) {
        this.mappingResolver = mappingResolver;
    }

    /**
     * 外部图像请求 → 内部标准请求
     *
     * @param external     OpenAI 兼容图像请求
     * @param requestType  IMAGE_GENERATION / IMAGE_EDIT
     */
    public Mono<LlmChatRequest> toInternalRequest(OpenAiImageRequest external, LlmRequestType requestType) {
        if (external == null) {
            return Mono.empty();
        }

        // 与文本聊天一致：先走 ModelMappingResolver 模型映射（可把外部模型名映射成 渠道/模型），
        // 无映射则透传；后续由 ModelRouter 解析渠道前缀或纯模型ID兜底。
        // 渠道前缀形式（如 openai-image/gpt-image-2）可在同一模型多渠道时确定路由到指定渠道。
        String internalModel = mappingResolver.resolve(external.getModel(), ProtocolType.OPENAI_IMAGES);

        try {
            // 输入图统一为 LlmImageInput 列表：multipart 二进制上传 + OpenAI 标准 images 引用（data URI/URL）合并
            // file_id 不支持等校验在 mergeImageInputs 内抛 LlmGatewayException，这里转成 Mono.error（WebFlux 反应式错误）
            List<LlmImageInput> mergedImages = mergeImageInputs(external);

            LlmImageParams params = LlmImageParams.builder()
                .prompt(external.getPrompt())
                .n(external.getN())
                .size(external.getSize())
                .quality(external.getQuality())
                .style(external.getStyle())
                .outputFormat(external.getOutputFormat())
                .outputCompression(external.getOutputCompression())
                .background(external.getBackground())
                .moderation(external.getModeration())
                .seed(external.getSeed())
                .user(external.getUser())
                .images(mergedImages)
                .mask(external.getMaskInput())
                .extraParams(external.getExtraParams())
                .build();

            LlmChatRequest internal = LlmChatRequest.builder()
                .model(internalModel)
                .requestType(requestType)
                .stream(false)
                .imageParams(params)
                .build();
            return Mono.just(internal);
        } catch (LlmGatewayException e) {
            return Mono.error(e);
        }
    }

    /**
     * 合并编辑输入图：multipart 二进制（imageInputs）在前，OpenAI images 引用（String | Map）在后，
     * 统一转成内部 LlmImageInput 列表。转换规则（对齐 OpenAI images 参数定义）：
     * - 元素是 String：data: 前缀 → base64 data URI 解出 base64Data + mimeType；否则视为普通 URL
     * - 元素是 Map（{image_url, file_id}）：image_url 同 String 规则；file_id → 报明确错误（网关未实现 /v1/files）
     * - 普通 URL 原样保留为 url（适配器按渠道处理：DashScope 直接透传、
     *   OpenAI/Azure 下载转 base64、Gemini 下载转 base64 进 inlineData——fileUri 不认 http 地址）
     */
    private List<LlmImageInput> mergeImageInputs(OpenAiImageRequest external) {
        List<LlmImageInput> inputs = new ArrayList<>();
        if (external.getImageInputs() != null) {
            inputs.addAll(external.getImageInputs());
        }
        if (external.getImages() != null) {
            for (Object ref : external.getImages()) {
                LlmImageInput in = fromImageRef(ref);
                if (in != null) {
                    inputs.add(in);
                }
            }
        }
        return inputs.isEmpty() ? null : inputs;
    }

    private LlmImageInput fromImageRef(Object ref) {
        if (ref == null) {
            return null;
        }
        String url = null;
        String fileId = null;
        if (ref instanceof String s) {
            url = s;
        } else if (ref instanceof Map<?, ?> map) {
            Object u = map.get("image_url");
            Object f = map.get("file_id");
            url = u instanceof String su ? su : null;
            fileId = f instanceof String sf ? sf : null;
        }
        if (url != null && !url.isBlank()) {
            if (url.startsWith("data:")) {
                // data URI：data:<mime>;base64,<data>
                int comma = url.indexOf(',');
                if (comma < 0) {
                    return null;
                }
                String meta = url.substring(5, comma);
                String mime = null;
                int semi = meta.indexOf(';');
                if (semi >= 0) {
                    mime = meta.substring(0, semi);
                }
                return LlmImageInput.builder()
                    .base64Data(url.substring(comma + 1))
                    .mimeType(mime)
                    .build();
            }
            // 普通 URL：原样保留为 url，适配器按渠道处理（DashScope 直接透传；OpenAI/Azure 下载转 base64；
            // Gemini 下载转 base64 进 inlineData——fileUri 不认普通 http 地址，见 GeminiImageAdapter）。
            return LlmImageInput.builder().url(url).build();
        }
        if (fileId != null && !fileId.isBlank()) {
            throw new LlmGatewayException(LlmErrorCode.INVALID_REQUEST,
                "file_id 暂不支持（网关未实现 /v1/files），请改用 image_url 传 base64 data URI");
        }
        return null;
    }

    /**
     * 内部标准响应 → OpenAI 兼容图像响应
     */
    public OpenAiImageResponse toExternalResponse(LlmChatResponse internal) {
        List<OpenAiImageResponse.ImageData> data = null;
        if (internal.getImages() != null) {
            data = internal.getImages().stream()
                .map(img -> OpenAiImageResponse.ImageData.builder()
                    .b64Json(img.getB64Json())
                    .url(img.getUrl())
                    .revisedPrompt(img.getRevisedPrompt())
                    .build())
                .collect(Collectors.toList());
        }

        OpenAiImageResponse.OpenAiImageUsage usage = null;
        if (internal.getUsage() != null) {
            LlmChatResponse.Usage u = internal.getUsage();
            OpenAiImageResponse.OpenAiImageUsage.OpenAiImageUsageBuilder ub = OpenAiImageResponse.OpenAiImageUsage.builder()
                .totalTokens((long) u.getTotalTokens())
                .inputTokens((long) u.getPromptTokens())
                .outputTokens((long) u.getCompletionTokens());
            if (u.getInputTextTokens() > 0 || u.getInputImageTokens() > 0) {
                ub.inputTokensDetails(OpenAiImageResponse.InputTokensDetails.builder()
                    .textTokens(u.getInputTextTokens())
                    .imageTokens(u.getInputImageTokens())
                    .build());
            }
            usage = ub.build();
        }

        return OpenAiImageResponse.builder()
            .created(Instant.now().getEpochSecond())
            .data(data)
            .usage(usage)
            .build();
    }
}
