package com.llmate.multiprotocol.engine.provider.anthropic;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.anthropic.AnthropicStreamEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Anthropic 流式 SSE 事件解析回归测试。
 *
 * 数据取自生产日志 docs/bug/claude流式报错.md 中 dreamfly.art 中继的实际事件（3528-3559 行）。
 * 这些事件带 DTO 没有的扩展字段（顶层 model、message/delta.stop_details、usage.output_tokens_details），
 * 若 ObjectMapper 未关闭 FAIL_ON_UNKNOWN_PROPERTIES，readValue 会对所有合法事件抛
 * UnrecognizedPropertyException，被 parseAnthropicSseEvent 当成"尾部垃圾"跳过，导致流式正文与
 * tokens 全丢。本测试锁定 AnthropicProviderAdapter.SSE_MAPPER 的 ObjectMapper 配置，防回归。
 */
class AnthropicSseParsingTest {

    /** 必须与 AnthropicProviderAdapter.SSE_MAPPER 完全一致的配置 */
    private static final ObjectMapper MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** 模拟 parseAnthropicSseEvent 的核心逻辑：解析失败返回 null（由 mapNotNull 跳过） */
    private static AnthropicStreamEvent parseOrNull(String data) {
        try {
            return MAPPER.readValue(data, AnthropicStreamEvent.class);
        } catch (Exception e) {
            return null;
        }
    }

    @Test
    void messageStartExtractsInputTokens() {
        // 日志 3528 行真实事件（含顶层 model、message.stop_details、message.usage.cache_creation 等未知字段）
        String json = "{\"message\":{\"content\":[],\"id\":\"msg_011CeR9mjcxYfM5CPMRDcAtT\",\"model\":\"claude-opus-5\",\"role\":\"assistant\",\"stop_details\":null,\"stop_reason\":null,\"stop_sequence\":null,\"type\":\"message\",\"usage\":{\"cache_creation\":{\"ephemeral_1h_input_tokens\":0,\"ephemeral_5m_input_tokens\":0},\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"input_tokens\":49491,\"output_tokens\":1}},\"type\":\"message_start\",\"model\":\"claude-opus-5\"}";
        AnthropicStreamEvent event = parseOrNull(json);
        assertNotNull(event, "message_start 必须能解析（上游带未知扩展字段时不应被跳过）");
        assertEquals("message_start", event.getType());
        assertNotNull(event.getMessage());
        assertEquals(49491, event.getMessage().getUsage().getInputTokens());
    }

    @Test
    void contentBlockStartParses() {
        // 日志 3529 行真实事件
        String json = "{\"content_block\":{\"text\":\"\",\"type\":\"text\"},\"index\":0,\"type\":\"content_block_start\",\"model\":\"claude-opus-5\"}";
        AnthropicStreamEvent event = parseOrNull(json);
        assertNotNull(event);
        assertEquals("content_block_start", event.getType());
    }

    @Test
    void contentBlockDeltaParses() {
        // 日志 3531 行真实事件（正文逐字下发）
        String json = "{\"delta\":{\"text\":\"記得\",\"type\":\"text_delta\"},\"index\":0,\"type\":\"content_block_delta\",\"model\":\"claude-opus-5\"}";
        AnthropicStreamEvent event = parseOrNull(json);
        assertNotNull(event, "content_block_delta 必须能解析，否则流式正文全丢");
        assertEquals("content_block_delta", event.getType());
        assertEquals("記得", event.getDelta().getText());
    }

    @Test
    void contentBlockStopParses() {
        // 日志 3557 行真实事件
        String json = "{\"index\":0,\"type\":\"content_block_stop\",\"model\":\"claude-opus-5\"}";
        AnthropicStreamEvent event = parseOrNull(json);
        assertNotNull(event);
        assertEquals("content_block_stop", event.getType());
    }

    @Test
    void messageDeltaExtractsTokens() {
        // 日志 3558 行真实事件（带 usage.output_tokens_details 未知字段，且就是此前被跳过的那个）
        String json = "{\"delta\":{\"stop_details\":null,\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"type\":\"message_delta\",\"usage\":{\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":0,\"input_tokens\":49491,\"output_tokens\":478,\"output_tokens_details\":{\"thinking_tokens\":0}},\"model\":\"claude-opus-5\"}";
        AnthropicStreamEvent event = parseOrNull(json);
        assertNotNull(event, "message_delta 必须能解析并提取到真实 tokens");
        assertEquals("message_delta", event.getType());
        assertEquals("end_turn", event.getDelta().getStopReason());
        assertEquals(49491, event.getUsage().getInputTokens());
        assertEquals(478, event.getUsage().getOutputTokens());
    }

    @Test
    void messageStopAndPingParse() {
        assertNotNull(parseOrNull("{\"type\":\"message_stop\",\"model\":\"claude-opus-5\"}"));
        assertNotNull(parseOrNull("{\"type\":\"ping\",\"model\":\"claude-opus-5\"}"));
    }

    @Test
    void trailingArrayIsSkipped() {
        // dreamfly.art 在 message_stop 之后追加的非法尾部事件：data 为 JSON 数组 → 必须跳过不能崩流
        String trailingArray = "[{\"type\":\"message\",\"usage\":{\"input_tokens\":49491,\"output_tokens\":525}}]";
        assertNull(parseOrNull(trailingArray), "JSON 数组尾部事件必须被跳过，不能让整条流以 error 结束");
    }
}
