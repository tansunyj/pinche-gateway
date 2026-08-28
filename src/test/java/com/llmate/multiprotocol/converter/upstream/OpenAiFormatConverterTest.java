package com.llmate.multiprotocol.converter.upstream;

import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.openai.OpenAiDelta;
import com.llmate.multiprotocol.dto.openai.OpenAiStreamChunk;
import com.llmate.multiprotocol.dto.openai.OpenAiStreamChoice;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 回归测试：上游 OpenAI 兼容 SSE 的 reasoning_content 必须映射到内部
 * LlmStreamChunk.deltaReasoningContent，不得并入 deltaContent。
 *
 * 修复前 OpenAiFormatConverter 把 "reasoningContent 优先于 content" 二选一塞进
 * deltaContent，导致推理被当作正文拼进用户 content（见 docs/response.md）。
 */
class OpenAiFormatConverterTest {

    private final OpenAiFormatConverter converter = new OpenAiFormatConverter();

    private static OpenAiStreamChunk chunkWithDelta(OpenAiDelta delta) {
        return OpenAiStreamChunk.builder()
                .id("chatcmpl-1")
                .model("deepseek-v4-flash-0731")
                .choices(List.of(OpenAiStreamChoice.builder().delta(delta).build()))
                .build();
    }

    @Test
    void reasoningContentMapsToDeltaReasoningContentNotContent() {
        LlmStreamChunk internal = converter.toInternalStreamChunk(
                chunkWithDelta(OpenAiDelta.builder().reasoningContent("The").build()));
        assertEquals("The", internal.getDeltaReasoningContent());
        assertEquals("", internal.getDeltaContent());
    }

    @Test
    void contentMapsToDeltaContentOnly() {
        LlmStreamChunk internal = converter.toInternalStreamChunk(
                chunkWithDelta(OpenAiDelta.builder().content("你好").build()));
        assertEquals("你好", internal.getDeltaContent());
        assertNull(internal.getDeltaReasoningContent());
    }

    @Test
    void mixedDeltaKeepsFieldsSeparate() {
        LlmStreamChunk internal = converter.toInternalStreamChunk(
                chunkWithDelta(OpenAiDelta.builder().reasoningContent("think").content("answer").build()));
        assertEquals("think", internal.getDeltaReasoningContent());
        assertEquals("answer", internal.getDeltaContent());
    }
}
