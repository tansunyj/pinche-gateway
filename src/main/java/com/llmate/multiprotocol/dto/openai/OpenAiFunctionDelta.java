package com.llmate.multiprotocol.dto.openai;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OpenAiFunctionDelta {

    /**
     * 被调用的函数名称（例如: "get_weather"），通常在流式前段一次性返回
     */
    private String name;

    /**
     * 流式吐出的函数入参 JSON 字符串增量片段（例如: "{\"loca" -> "tion\":" -> "\"Beijing\"}"）
     * 客户端 SDK 收到后会在前端将其自行拼接成完整的 JSON
     */
    private String arguments;
}