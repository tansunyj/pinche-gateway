package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * OpenAI 兼容的视频生成请求体（POST /v1/videos/generations）
 *
 * 采用 OpenAI 异步任务形态的超集字段，覆盖两渠道能力：
 * - 文生视频：model + prompt（+ resolution/ratio/duration/generateAudio）
 * - 图生视频：model + prompt + images 或 imageUrl/firstFrame/lastFrame（单图=首帧）
 * - 首尾帧：firstFrame + lastFrame
 * - 多模态参考：images[]（>=3）/ referenceImages[] + referenceVideos[] + referenceAudios[]
 * - DashScope 原生 media 数组（视频续写/参考媒体）：media[]
 * 渠道特有的参数（negative_prompt / watermark / prompt_extend / seed 等）可放 extra_params 透传。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiVideoRequest {

    /** 模型ID（完整名称，含渠道前缀，如 aliyun/happyhorse-1.0-t2v） */
    private String model;

    /** 正向提示词 */
    private String prompt;

    /** 输入图列表（OpenAI 风格 images 引用）：image_url 支持 URL 或 base64 data URI */
    @JsonProperty("images")
    private List<ImageRef> images;

    /** 单个首帧图 URL（便捷字段） */
    @JsonProperty("image_url")
    @JsonAlias("imageUrl")
    private String imageUrl;

    /** 首帧图 URL（显式） */
    @JsonProperty("first_frame")
    @JsonAlias("firstFrame")
    private String firstFrame;

    /** 尾帧图 URL（显式，与首帧配对成首尾帧） */
    @JsonProperty("last_frame")
    @JsonAlias("lastFrame")
    private String lastFrame;

    /** 参考图 URL 列表（多模态参考） */
    @JsonProperty("reference_images")
    @JsonAlias("referenceImages")
    private List<String> referenceImages;

    /** 参考视频 URL 列表 */
    @JsonProperty("reference_videos")
    @JsonAlias("referenceVideos")
    private List<String> referenceVideos;

    /** 参考音频 URL 列表 */
    @JsonProperty("reference_audios")
    @JsonAlias("referenceAudios")
    private List<String> referenceAudios;

    /** 单个参考音频 URL（兼容旧 audioUrl 参数） */
    @JsonProperty("audio_url")
    @JsonAlias("audioUrl")
    private String audioUrl;

    /** DashScope media 数组（新格式） */
    private List<MediaItem> media;

    /** 反向提示词（DashScope happyhorse/wan 支持） */
    @JsonProperty("negative_prompt")
    @JsonAlias("negativePrompt")
    private String negativePrompt;

    /** 分辨率：480P/720P/1080P/4K */
    private String resolution;

    /** 宽高比：16:9/9:16/1:1/4:3/3:4/4:5/5:4/9:21/21:9 */
    private String ratio;

    /** 宽高比别名（与 ratio 二选一，Seedance 用 ratio） */
    private String aspect;

    /** 时长（秒） */
    private Integer duration;

    /** 是否生成同步音频 */
    @JsonProperty("generate_audio")
    @JsonAlias("generateAudio")
    private Boolean generateAudio;

    /** 是否加水印 */
    private Boolean watermark;

    /** 是否扩展提示词 */
    @JsonProperty("prompt_extend")
    @JsonAlias("promptExtend")
    private Boolean promptExtend;

    /** 视频模式提示（text2video/image2video/reference2video/first_last_frame），可选 */
    private String mode;

    private String seed;

    private String user;

    /** 渠道专属参数透传 */
    @JsonProperty("extra_params")
    private Map<String, Object> extraParams;

    /**
     * OpenAI 风格输入图引用
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ImageRef {
        @JsonProperty("file_id")
        private String fileId;
        @JsonProperty("image_url")
        private String imageUrl;
        /** 角色提示：first_frame / last_frame / reference_image（可选，缺省按数量推断） */
        private String role;
    }

    /**
     * DashScope media 数组元素
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MediaItem {
        private String type;
        private String url;
        @JsonProperty("reference_voice")
        private String referenceVoice;
    }
}
