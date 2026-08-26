package com.llmate.multiprotocol.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * API Key 首尾遮罩回归测试：安全敏感逻辑，锁死"只露首尾、绝不露中间"的行为。
 */
class KeyMaskUtilTest {

    @Test
    void maskKeepsHeadAndTailOnly() {
        String masked = KeyMaskUtil.mask("sk-silievo-2c4b76e3bbe34eb31621066e222bd8fd");
        // 头部保留 12 字符（含 sk-silievo- 前缀 + 1 个主体字符），尾部保留最后 6 位，中间星号
        assertTrue(masked.startsWith("sk-silievo-2"), "头部应保留 sk-xxx- 前缀 + 若干字符");
        assertTrue(masked.endsWith("2bd8fd"), "尾部应保留最后 6 位");
        assertTrue(masked.contains("****"), "中间必须是星号遮罩");
        assertFalse(masked.contains("76e3bbe34eb31621066e"), "中段绝不能被打印");
        assertEquals("sk-silievo-2****2bd8fd", masked);
    }

    @Test
    void maskNullAndEmptyReturnsNull() {
        assertEquals("null", KeyMaskUtil.mask(null));
        assertEquals("null", KeyMaskUtil.mask(""));
    }

    @Test
    void maskShortKeyFullyHidden() {
        // 长度不足以"首12尾6+4星号"时整段全遮，不泄露任何字符
        assertEquals("****", KeyMaskUtil.mask("sk-abc123"));
    }

    @Test
    void describeKeysPrintsIdsAndMaskedKeys() {
        String desc = KeyMaskUtil.describeKeys(
                5L, "sk-silievo-2c4b76e3bbe34eb31621066e222bd8fd",
                3L, "sk-silievo-8a67d8fbfb05d3980ebbf710c94609e3");
        assertTrue(desc.contains("userTokenId=5"), "应含用户 key ID");
        assertTrue(desc.contains("userKey=sk-silievo-2****2bd8fd"), "用户 key 应首尾遮罩");
        assertTrue(desc.contains("channelTokenId=3"), "应含渠道 key ID");
        assertTrue(desc.contains("channelKey=sk-silievo-8****4609e3"), "渠道 key 应首尾遮罩");
        assertFalse(desc.contains("cbb3bbd9638aaf8520c52ff8"), "渠道 key 中段绝不能被打印");
    }
}
