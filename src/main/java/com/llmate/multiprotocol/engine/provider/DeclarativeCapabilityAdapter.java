package com.llmate.multiprotocol.engine.provider;

import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 【声明式】能力占位适配器 —— 供不走 LlmGateway 实时路由的能力域使用。
 *
 * 两类能力落在本适配器上：
 * 1. <b>平台已实现但走独立服务编排</b>的能力域（embedding/rerank）：
 *    运行时由 EmbeddingService 等按
 *    model_channel_configs → proxy_channels → proxy_channel_tokens 配置链路路由，
 *    不经 ProviderRegistry，本实例在正常链路里<b>永远不会被调用</b>；
 * 2. <b>尚未接入真实 Adapter</b>的未来能力（如 zhipu/moonshot/gemini_video 等）：
 *    仅为了让 admin 的「模型↔渠道关联」能选到能力、网关启动注册不报错，
 *    万一有请求路由到它，chat/chatStream 给出明确"未接入实时路由"错误，而不是静默吞掉。
 *
 * chat / chatStream 是 ProviderAdapter 唯一抽象方法；image/video 等默认方法
 * 已由接口兜底抛 UnsupportedOperationException（消息带 providerName）。
 */
public class DeclarativeCapabilityAdapter implements ProviderAdapter {

    private final String providerAlias;
    private final String providerName;
    /** 真实路由去向说明，用于错误提示（如"向量走 EmbeddingService，使用 /v1/embeddings"） */
    private final String routeHint;

    public DeclarativeCapabilityAdapter(String providerAlias, String providerName, String routeHint) {
        this.providerAlias = providerAlias;
        this.providerName = providerName;
        this.routeHint = routeHint;
    }

    @Override
    public String getProviderAlias() {
        return providerAlias;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    @Override
    public Mono<LlmChatResponse> chat(LlmChatRequest request) {
        return Mono.error(new UnsupportedOperationException(
                "能力 " + providerAlias + " 为声明式占位，未接入 LlmGateway 实时路由；" + routeHint));
    }

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest request) {
        return Flux.error(new UnsupportedOperationException(
                "能力 " + providerAlias + " 为声明式占位，未接入 LlmGateway 实时路由；" + routeHint));
    }
}
