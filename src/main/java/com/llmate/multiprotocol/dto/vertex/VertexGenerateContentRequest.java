package com.llmate.multiprotocol.dto.vertex;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Google Vertex AI GenerateContent 请求体
 *
 * Vertex AI 请求格式与 Gemini 原生 API 基本一致，但端点结构不同：
 * POST /v1/projects/{project}/locations/{location}/publishers/google/models/{model}:generateContent
 *
 * 关键差异：
 * - 认证方式：GCP OAuth2 / Service Account (Bearer token)，非 API Key
 * - 需要 projectId 和 location 构建 URL
 * - 请求体结构与 Gemini 相同（contents/parts/systemInstruction/generationConfig）
 *
 * 参考: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/projects.locations.publishers.models/generateContent
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class VertexGenerateContentRequest {

    /** 模型名称 (e.g. "gemini-3.6-flash") */
    private String model;

    /**
     * 对话内容列表
     * 支持两种格式: 单个 GeminiContent 对象 或 GeminiContent 数组
     */
    private Object contents;

    /**
     * 系统指令（顶层字段，不放在 contents 中）
     */
    @JsonProperty("systemInstruction")
    private VertexContent systemInstruction;

    /**
     * 工具列表（functionDeclarations 声明）
     */
    private List<VertexTool> tools;

    /**
     * 工具调用策略（对应 Anthropic tool_choice / OpenAI tool_choice）
     */
    @JsonProperty("toolConfig")
    private VertexToolConfig toolConfig;

    /**
     * 生成配置
     */
    @JsonProperty("generationConfig")
    private VertexGenerationConfig generationConfig;

    /**
     * 未显式建模字段兜底透传（metadata / output_config / stream_options 等），
     * 以 @JsonAnyGetter 原样合并进 Gemini 请求体。
     */
    private Map<String, Object> extraParams = new LinkedHashMap<>();

    @JsonAnySetter
    public void putExtraParam(String key, Object value) {
        if (this.extraParams == null) {
            this.extraParams = new LinkedHashMap<>();
        }
        this.extraParams.put(key, value);
    }

    @JsonAnyGetter
    public Map<String, Object> getExtraParams() {
        return this.extraParams;
    }

    /**
     * Vertex AI 内容单元 (单轮对话)
     * parts: 内容片段列表 [{text: "..."}, {inlineData: {...}}, ...]
     * role: "user" | "model" (Vertex 用 "model" 而非 "assistant")
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexContent {
        private List<VertexPart> parts;
        private String role;
    }

    /**
     * 内容片段 - 文本或内联数据（图片/音频等）
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexPart {
        /** 文本内容 */
        private String text;

        /** 内联数据 (base64 图片等)，包含 mimeType 和 data 字段 */
        @JsonProperty("inlineData")
        private VertexInlineData inlineData;

        /** 函数调用（模型返回工具调用时用，请求侧一般不填） */
        @JsonProperty("functionCall")
        private Object functionCall;

        /** 函数响应结果（客户端将工具执行结果发回模型时用） */
        @JsonProperty("functionResponse")
        private Object functionResponse;

        /** Gemini thinking 模式下的思考签名，回传 functionCall 时必须附带 */
        @JsonProperty("thought_signature")
        private String thoughtSignature;
    }

    /**
     * 内联数据（用于多模态）
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexInlineData {
        private String mimeType;
        private String data; // base64 编码
    }

    /**
     * 工具声明（Gemini tools 数组元素）
     * { "functionDeclarations": [ {name, description, parameters} ] }
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexTool {
        @JsonProperty("functionDeclarations")
        private List<VertexFunctionDeclaration> functionDeclarations;
    }

    /**
     * 函数声明
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexFunctionDeclaration {
        private String name;
        private String description;
        /**
         * 入参 JSON Schema。
         * 标准 Gemini/Vertex API 字段名为 "parameters"，但 Claude Desktop 等客户端
         * 发送的是 "parametersJsonSchema"（Google proto JSON 表述），两者语义完全一致。
         * 用 @JsonAlias 同时接受两种名称，反序列化都不丢失；
         * 序列化输出仍为 "parameters"（Java 字段名），对齐 Gemini 上游标准格式。
         */
        @JsonAlias("parametersJsonSchema")
        private Map<String, Object> parameters;
    }

    /**
     * 工具调用策略（对应 Anthropic tool_choice / OpenAI tool_choice）
     * { "functionCallingConfig": { "mode": "AUTO"|"ANY"|"NONE", "allowedFunctionNames": [...] } }
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexToolConfig {
        @JsonProperty("functionCallingConfig")
        private VertexFunctionCallingConfig functionCallingConfig;
    }

    /**
     * 函数调用配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexFunctionCallingConfig {
        /** "AUTO" | "ANY" | "NONE" */
        private String mode;
        @JsonProperty("allowedFunctionNames")
        private List<String> allowedFunctionNames;
    }

    /**
     * 思考/推理配置（对应 Anthropic thinking）
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexThinkingConfig {
        @JsonProperty("includeThoughts")
        private Boolean includeThoughts;
        @JsonProperty("thinkingBudget")
        private Integer thinkingBudget;
    }

    /**
     * 生成配置
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class VertexGenerationConfig {
        @JsonProperty("maxOutputTokens")
        private Integer maxOutputTokens;
        private Double temperature;
        private Double topP;
        @JsonProperty("topK")
        private Integer topK;

        /** 停止序列 */
        @JsonProperty("stopSequences")
        private List<String> stopSequences;

        /** 思考/推理配置（对应 Anthropic thinking） */
        @JsonProperty("thinkingConfig")
        private VertexThinkingConfig thinkingConfig;
    }
}
