package com.llmate.multiprotocol.dto.anthropic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * count_tokens 上游响应反序列化测试：
 * 上游返回 {"input_tokens": N}，网关需能正确映射到 CountTokensResponse.inputTokens。
 * （Claude Code 依赖该字段判断上下文占用，映射错误会导致其反复重试。）
 */
class CountTokensResponseTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void jsonDeserializesInputTokens() throws Exception {
        CountTokensResponse resp = MAPPER.readValue("{\"input_tokens\":123}", CountTokensResponse.class);
        assertNotNull(resp);
        assertEquals(123, resp.getInputTokens());
    }

    @Test
    void jsonDeserializesTypicalClaudeCodeRequest() throws Exception {
        // Claude Code 对 count_tokens 的典型请求体（body 可含 system/messages/tools 等），
        // 响应只关心 input_tokens 是否正确映射。
        CountTokensResponse resp = MAPPER.readValue(
            "{\"input_tokens\":49491,\"output_tokens\":0}", CountTokensResponse.class);
        assertEquals(49491, resp.getInputTokens());
    }

    @Test
    void jsonDeserializesOfficialResponseWithContextManagement() throws Exception {
        // Claude 官方文档示例响应（docs/bug/count_tokens.md）：
        // {"context_management":{"original_input_tokens":0},"input_tokens":2095}
        // context_management 是官方字段，Claude Code 会读取，必须完整保留。
        CountTokensResponse resp = MAPPER.readValue(
            "{\"context_management\":{\"original_input_tokens\":0},\"input_tokens\":2095}",
            CountTokensResponse.class);
        assertEquals(2095, resp.getInputTokens());
        assertNotNull(resp.getContextManagement());
        assertEquals(0L, resp.getContextManagement().getOriginalInputTokens());
    }
}
