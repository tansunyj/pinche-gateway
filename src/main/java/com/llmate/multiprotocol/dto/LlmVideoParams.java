package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 视频生成内部标准参数（requestType=VIDEO_GENERATION 时由 LlmChatRequest.videoParams 承载）
 *
 * 字段覆盖阿里云百炼（happyhorse/wan2.7）与火山引擎 Seedance 2.0 两渠道的公共能力，
 * 各渠道 ProviderAdapter 按需取用：
 * - images（LlmImageInput 有序列表）：首帧/尾帧/参考图，适配器按图片数量自动判模式
 * - media（视频续写/参考媒体，DashScope 原生 media 数组透传）
 * - referenceVideos / referenceAudios / audioUrl：参考视频/参考音频（Seedance 多模态）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmVideoParams {

    /** 正向提示词 */
    private String prompt;

    /** 反向提示词（DashScope happyhorse/wan 支持） */
    private String negativePrompt;

    /** 分辨率：480P/720P/1080P/4K（各渠道适配器自行归一化大小写） */
    private String resolution;

    /** 宽高比：16:9/9:16/1:1/4:3/3:4/4:5/5:4/9:21/21:9（Seedance 用 ratio 字段） */
    private String aspectRatio;

    /** 时长（秒） */
    private Integer duration;

    /** 是否生成同步音频（Seedance 2.0 支持） */
    private Boolean generateAudio;

    /** 是否加水印（wan2.7 默认 false） */
    private Boolean watermark;

    /** 是否扩展提示词（wan2.7 默认 true） */
    private Boolean promptExtend;

    /** 视频模式提示（text2video/image2video/reference2video/first_last_frame），可选，缺省按图片数量/模型名推断 */
    private String mode;

    private String seed;

    private String user;

    /** 输入图片（首帧/尾帧/参考图，保持客户端传入顺序），url 或 base64Data */
    private List<LlmImageInput> images;

    /** 参考视频 URL 列表（Seedance，非 mini 模型，最多 3 个） */
    private List<String> referenceVideos;

    /** 参考音频 URL 列表（Seedance，非 mini 模型，最多 3 个） */
    private List<String> referenceAudios;

    /** 单个参考音频 URL（兼容旧 audioUrl 参数） */
    private String audioUrl;

    /** DashScope media 数组（视频续写/参考媒体，新格式优先于 images） */
    private List<VideoMedia> media;

    /** 渠道专属参数透传 */
    private Map<String, Object> extraParams;

    /**
     * DashScope media 数组元素
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class VideoMedia {
        /** 媒体类型：first_frame / reference_image / reference_video / reference_audio 等 */
        private String type;
        private String url;
        /** 声音复刻参考音色 */
        private String referenceVoice;
    }
}
