package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.llmate.multiprotocol.dto.LlmImageInput;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 符合 OpenAI 标准的图像生成/编辑请求体
 *
 * - POST /v1/images/generations：JSON 请求，仅使用上方常规字段
 * - POST /v1/images/edits：multipart/form-data，由控制器从表单组装
 *   imageInputs / maskInput（@JsonIgnore，不参与 JSON 反序列化）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiImageRequest {

    private String model;

    private String prompt;

    private Integer n;

    private String size;

    private String quality;

    private String style;

    /** 输出文件格式（仅 GPT image 模型支持）：png / jpeg */
    @JsonProperty("output_format")
    private String outputFormat;

    @JsonProperty("output_compression")
    private Integer outputCompression;

    private String background;

    private String moderation;

    private String seed;

    private String user;

    private Boolean stream;

    /** 渠道专属参数透传（可选，各渠道按需读取）：
     *  Gemini 的 generationConfig / systemInstruction、DashScope 的 prompt_extend / negative_prompt / watermark 等。
     *  OpenAI 标准入口参数之外的渠道特有参数，从这里以可选方式传入。 */
    @JsonProperty("extra_params")
    private Map<String, Object> extraParams;

    /** OpenAI 标准 images 数组（JSON 编辑格式），兼容两种元素形态：
     *  ① 纯字符串：base64 data URI 或完整 URL，如 "data:image/jpeg;base64,xxx"
     *  ② 对象：{file_id | image_url}，如 {"image_url":"data:...","file_id":"..."}
     *  与 multipart 的 image/image[] 二进制上传二选一（都传则先二进制后引用）。
     *  解析在 ImageProtocolConverter.fromImageRef 完成（String | Map 双分支）。 */
    @JsonProperty("images")
    private List<Object> images;

    // ========== 编辑专用（由 multipart 控制器填充，不入 JSON 请求体） ==========

    /** 输入图列表（编辑用，base64 或 url，来自 multipart 二进制上传） */
    @JsonIgnore
    private List<LlmImageInput> imageInputs;

    /** 蒙版图（可选，编辑用） */
    @JsonIgnore
    private LlmImageInput maskInput;

    /**
     * OpenAI 编辑接口的输入图引用
     * file_id 与 image_url 二选一。
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ImageRef {

        /** File API 上传的文件 ID（本网关暂未实现 /v1/files，传了会报错） */
        @JsonProperty("file_id")
        private String fileId;

        /** 完整 URL 或 base64-encoded data URL（maxLength 20971520，format uri）。
         *  普通 URL 由各渠道适配器自处理：DashScope 直接透传、OpenAI/Azure 下载转 base64、
         *  Gemini 下载转 base64 进 inlineData（fileUri 不认普通 http 地址）。 */
        @JsonProperty("image_url")
        private String imageUrl;
    }
}
