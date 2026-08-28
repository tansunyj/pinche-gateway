package com.llmate.multiprotocol.converter.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.openai.OpenAiChatRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiStreamChunk;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 回归测试：内部 LlmStreamChunk 输出到用户 OpenAI SSE 时，
 * delta.reasoning_content 与 delta.content 必须分属独立字段。
 *
 * 修复前 StreamingConverter 把内部 deltaContent（含推理）原样输出到 delta.content，
 * 页面 content 显示为"推理+正文"拼接（见 docs/response.md）。
 */
class StreamingConverterTest {

    private final StreamingConverter converter = new StreamingConverter(new ObjectMapper());

    private static OpenAiStreamChunk sseData(ServerSentEvent<Object> event) {
        assertNotNull(event, "SSE 事件不应为 null");
        Object data = event.data();
        assertTrue(data instanceof OpenAiStreamChunk, "data 应为 OpenAiStreamChunk: " + data);
        return (OpenAiStreamChunk) data;
    }

    private static List<ServerSentEvent<Object>> toEvents(Flux<ServerSentEvent<Object>> stream) {
        List<ServerSentEvent<Object>> events = stream.collectList().block();
        assertNotNull(events);
        return events;
    }

    @Test
    void reasoningChunkOutputsReasoningContentNotContent() {
        List<ServerSentEvent<Object>> events = toEvents(converter.toOpenAiStream(
                Flux.just(LlmStreamChunk.builder().deltaReasoningContent("The").deltaContent("").build()),
                new OpenAiChatRequest(),
                "masked-model"));

        var delta = sseData(events.get(0)).getChoices().get(0).getDelta();
        assertNull(delta.getContent(), "推理 chunk 不得携带 content");
        assertEquals("The", delta.getReasoningContent());
    }

    @Test
    void contentChunkOutputsContentOnly() {
        List<ServerSentEvent<Object>> events = toEvents(converter.toOpenAiStream(
                Flux.just(LlmStreamChunk.builder().deltaContent("你好").build()),
                new OpenAiChatRequest(),
                "masked-model"));

        var delta = sseData(events.get(0)).getChoices().get(0).getDelta();
        assertEquals("你好", delta.getContent());
        assertNull(delta.getReasoningContent());
    }

    @Test
    void mixedChunkSeparatesFields() {
        List<ServerSentEvent<Object>> events = toEvents(converter.toOpenAiStream(
                Flux.just(LlmStreamChunk.builder()
                        .deltaReasoningContent("think")
                        .deltaContent("answer")
                        .build()),
                new OpenAiChatRequest(),
                "masked-model"));

        var delta = sseData(events.get(0)).getChoices().get(0).getDelta();
        assertEquals("answer", delta.getContent());
        assertEquals("think", delta.getReasoningContent());
    }
}
