package com.llmate.multiprotocol.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LlmMessage {
    private String role; // system, user, assistant, tool
    @JsonProperty("content")
    private String textContent; // 纯文本内容

    @Builder.Default
    private List<LlmContent> contents = new ArrayList<>(); // 多模态内容块（图片等）

    /** assistant 消息中的工具调用列表 */
    private List<LlmToolCall> toolCalls;

    /** tool 角色消息中，对应的工具调用 ID */
    private String toolCallId;

    /** tool 角色消息中，被调用的函数名称 */
    private String name;

    public static LlmMessage system(String content) {
        return LlmMessage.builder().role("system").textContent(content).build();
    }
}