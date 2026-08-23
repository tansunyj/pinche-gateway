package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * 网关内部标准工具定义（请求透传）
 *
 * 从任一外部协议（Anthropic / OpenAI Chat / OpenAI Responses）解析出的工具定义统一落到这里，
 * 再由各上游转换器转成目标格式：
 * - Anthropic: tools[{name, description, input_schema}]
 * - OpenAI Chat: tools[{type, function:{name, description, parameters}}]
 * - Gemini: tools[{functionDeclarations:[{name, description, parameters}]}]
 *
 * parameters 即 JSON Schema（各协议等价字段：input_schema / function.parameters / functionDeclarations.parameters）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmToolDefinition {

    /** 工具名（如 "get_weather"、"Agent"） */
    private String name;

    /** 工具描述 */
    private String description;

    /** 工具入参 JSON Schema */
    private Map<String, Object> parameters;
}
