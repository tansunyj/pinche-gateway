package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * 符合 OpenAI 规范的流式响应分块数据包 (Chunk)
 * 对应流式 SSE 吐出的标准 JSON 数据：data: {"id":"chatcmpl-xxx", "object":"chat.completion.chunk", ...}
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL) // 过滤无用为 null 的属性，符合官方 SDK 的反序列化严格要求
public class OpenAiStreamChunk {

    /** 响应流的唯一随机 ID，同一个请求的流中所有 chunk 的 ID 必须保持一致 */
    private String id;

    /** 固定字符串，流式响应下固定为 "chat.completion.chunk" */
    @Builder.Default
    private String object = "chat.completion.chunk";

    /** Unix 时间戳（秒），表示当前 Chunk 生成的时间 */
    private Long created;

    /** 逆向掩码后的模型名称（例如: "gpt-4o"） */
    private String model;

    /**
     * 系统指纹（可选字段，OpenAI 用来标识后端集群版本）
     */
    @JsonProperty("system_fingerprint")
    private String systemFingerprint;

    /**
     * 流式响应的分支选择列表（核心增量数据区域）
     * 注意：依据 OpenAI 最新规范，流尾专门携带 include_usage 的包中，该列表可以为空数组
     */
    private List<OpenAiStreamChoice> choices;

    /**
     * 专属 Token 计量统计
     * 当客户端设置 include_usage=true 时，流的最后一个 Chunk 会包含该节点
     */
    private OpenAiUsage usage;
}