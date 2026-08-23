package com.llmate.multiprotocol.converter.support;

import com.llmate.multiprotocol.dto.LlmMessage;
import lombok.extern.log4j.Log4j2;

import java.util.List;

/**
 * 客户端中断污染清洗
 *
 * Claude Desktop / Claude Code 在 Bash 工具执行失败（如退出码非 0）且模型反复重试后，
 * 会自动中断工具调用并把 {"[Tool use interrupted]", "(no content)"} 污染对写进对话历史；
 * 若网关把这些文本原样转发给上游模型，模型会把标记当对话内容反复回显
 * （表现为"触发了 function call 但迟迟没有返回结果"的退化循环）。
 *
 * 本工具在协议转换完成后统一剥离此类纯污染消息，保证上游模型看到的永远是干净历史
 * （用户问题 → functionCall → functionResponse/error），从而正常应答。
 *
 * 只剥离"纯污染"消息：文本恰好是中断/空内容标记，且不含 toolCalls / 多模态 contents /
 * toolCallId（即不误伤任何真实工具往返）。
 */
@Log4j2
public final class PollutionCleaner {

    private PollutionCleaner() {
    }

    /** 客户端工具中断标记（assistant 文本块） */
    private static final String TOOL_INTERRUPTED = "[Tool use interrupted]";
    /** 客户端空内容标记（user 文本） */
    private static final String NO_CONTENT = "(no content)";

    /**
     * 原地剥离消息列表中的纯污染消息，并打印清洗日志。
     */
    public static void clean(List<LlmMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }
        int before = messages.size();
        messages.removeIf(PollutionCleaner::isPurePollution);
        int removed = before - messages.size();
        if (removed > 0) {
            log.info("[PollutionCleaner] 已剥离 {} 条客户端中断污染消息 ({} → {})",
                    removed, before, messages.size());
        }
    }

    private static boolean isPurePollution(LlmMessage msg) {
        String text = msg.getTextContent();
        if (text == null) {
            return false;
        }
        String t = text.trim();
        if (!TOOL_INTERRUPTED.equals(t) && !NO_CONTENT.equals(t)) {
            return false;
        }
        // 仅剥离"纯污染"：不含工具调用 / 多模态内容 / 工具结果关联
        boolean hasToolCalls = msg.getToolCalls() != null && !msg.getToolCalls().isEmpty();
        boolean hasContents = msg.getContents() != null && !msg.getContents().isEmpty();
        boolean isToolResult = msg.getToolCallId() != null || msg.getName() != null;
        return !hasToolCalls && !hasContents && !isToolResult;
    }
}
