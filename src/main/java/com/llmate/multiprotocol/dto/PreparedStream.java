package com.llmate.multiprotocol.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Flux;

/**
 * 流式请求的「预飞」结果。
 *
 * 持有已完成全部 setup 阶段的上游流式 Flux。setup 阶段（模型路由 → 渠道解析 → 价格查询 →
 * 余额预占 → 请求日志开始 → 端点解析）由 {@link com.llmate.multiprotocol.engine.LlmGateway#prepareStream}
 * 在响应提交【之前】以 Mono 形式执行——此时客户端还没有收到任何字节，SSE 的 200 响应头尚未发出。
 *
 * 因此 setup 阶段的任何业务错误（余额不足 402 / 模型未配置 400 / 渠道不存在 502 等）都会在
 * Mono 中抛出，由 Controller 交给 {@link com.llmate.multiprotocol.config.GlobalExceptionHandler}
 * 转成普通 HTTP 错误响应，而不是塞进 SSE 流里当 error 事件——客户端拿到的就是一个干净的
 * 余额不足报错，而不是一个 200 + text/event-stream 的畸形流。
 *
 * 只有响应提交后（上游已开始吐 chunk）的失败才走流内 SSE error 事件（见各 Controller 的
 * onErrorResume）。
 */
@Getter
@RequiredArgsConstructor
public class PreparedStream {

    /** 已预占余额、可直接订阅输出的上游流式 Flux（计费结算/失败清理编排已就位） */
    private final Flux<LlmStreamChunk> flux;
}
