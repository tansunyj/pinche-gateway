package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * 图像生成/编辑参数（跨渠道超集）
 *
 * 覆盖 OpenAI images/generations + images/edits 全部参数，并保留 extraParams
 * 承接各渠道专属参数（Gemini 的 generationConfig/systemInstruction、DashScope 的
 * parameters 等），由各 ProviderAdapter 按需读取，保证参数能完整满足生图/编辑需求。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmImageParams {

    // ========== OpenAI images 通用参数 ==========

    /** 提示词 */
    private String prompt;

    /** 生成图片张数 */
    private Integer n;

    /** 尺寸，如 1024x1024 / 512x512 */
    private String size;

    /** 质量：low / medium / high / auto */
    private String quality;

    /** 风格：vivid / natural / auto */
    private String style;

    /** 输出文件格式（仅 GPT image 模型支持）：png / jpeg */
    private String outputFormat;

    /** 输出压缩率 0-100（gpt-image 系） */
    private Integer outputCompression;

    /** 背景：transparent / opaque / auto */
    private String background;

    /** 审核级别：low / medium / high */
    private String moderation;

    /** 随机种子（确定性生成） */
    private String seed;

    /** 用户标识 */
    private String user;

    // ========== 图像编辑专用 ==========

    /** 输入图列表（编辑用） */
    private List<LlmImageInput> images;

    /** 蒙版图（可选，编辑用） */
    private LlmImageInput mask;

    // ========== 渠道扩展 ==========

    /** 各渠道专属参数透传（如 generationConfig / systemInstruction / parameters 等） */
    private Map<String, Object> extraParams;
}
