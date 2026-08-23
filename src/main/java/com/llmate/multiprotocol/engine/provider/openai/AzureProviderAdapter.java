package com.llmate.multiprotocol.engine.provider.openai;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.converter.upstream.OpenAiFormatConverter;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.openai.OpenAiChatRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiStreamChunk;
import com.llmate.multiprotocol.util.UrlUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.HttpHeaders;
import reactor.core.publisher.Flux;

import java.util.List;

/**
 * Azure OpenAI / Azure AI Provider 适配器
 *
 * 支持模型前缀: "azure/"
 * 上游通信协议：OpenAI 兼容格式
 *
 * 特殊点：
 * 1. 认证方式：api-key header + Bearer token（双重认证）
 * 2. 流式解析：使用 flatMap 逐行处理（历史遗留，与其他 OpenAI 兼容渠道略有不同）
 * 3. baseUrl 自动补 /
 *
 * 继承 OpenAiCompatibleAdapter，覆写流式解析逻辑
 *
 * 注意：本类不由 Spring 自动扫描创建，而是由 ProviderFactory 根据配置手动实例化
 */
@Log4j2
public class AzureProviderAdapter extends OpenAiCompatibleAdapter {

    private final String providerName;
    private final String providerAlias;

    public AzureProviderAdapter(
            String baseUrl,
            String apiKey,
            OpenAiFormatConverter formatConverter,
            String providerName,
            String providerAlias,
            ObjectMapper objectMapper,
            List<String> apiKeys,
            List<Long> tokenIds) {
        super(baseUrl, formatConverter, builder ->
                builder.defaultHeader("api-key", apiKey)
                        .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + apiKey), objectMapper, apiKeys, tokenIds);
        this.providerName = providerName;
        this.providerAlias = providerAlias;
    }

    @Override
    public String getProviderAlias() {
        return providerAlias;
    }

    @Override
    public String getProviderName() {
        return providerName;
    }

    /**
     * Azure 流式解析使用 flatMap 逐行处理（与其他 OpenAI 兼容渠道略有不同）
     * 保留原有实现以确保兼容性。
     *
     * 注意：LlmGateway 统一调用【双参】chatStream(internalReq, endpointConfig)，单参只是便捷入口。
     * 早期只覆写了单参版本：双参被父类 OpenAiCompatibleAdapter 覆写 → 这里覆写的 Azure 专用
     * flatMap 解析与 [Azure] 前缀日志从未执行（流式日志一直显示父类类名，误以为路由到了
     * OpenAiCompatibleAdapter）。现在单参委托给双参，两个签名都覆写，保证 Azure 逻辑真正生效。
     */
    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalReq) {
        return chatStream(internalReq, null);
    }

    @Override
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalReq, ModelEndpointConfig endpointConfig) {
        log.info("[Azure] 开始流式调用");
        OpenAiChatRequest openAiReq = formatConverter.toOpenAiRequest(internalReq);
        openAiReq.setStream(true);
        // 客户端已传 stream_options 就沿用客户端的，没传才补上 include_usage。
        // 流式对话缺少 usage chunk 会导致 token 计费归零，所以 include_usage 是刚需。
        boolean clientHasStreamOptions = openAiReq.getExtraParams() != null
                && openAiReq.getExtraParams().containsKey("stream_options");
        if (!clientHasStreamOptions) {
            openAiReq.setStreamOptions(OpenAiChatRequest.StreamOptions.builder().includeUsage(true).build());
        }

        // 尊重自定义端点配置：有 endpointPath 用它，否则回退默认 chat/completions；
        // 打印完整 URL（baseUrl + 路径），避免只显示相对路径误导
        String uri = endpointConfig != null && endpointConfig.getEndpointPath() != null
                ? endpointConfig.getEndpointPath()
                : DEFAULT_PATH;
        logRequest("流式", UrlUtils.join(baseUrl, uri), openAiReq);

        return doPostStreamRaw(uri, openAiReq, endpointConfig)
                .flatMap(rawLine -> {
                    log.debug("[Azure] 收到原始SSE行: {}", rawLine);

                    if (rawLine == null || rawLine.isEmpty()) {
                        return Flux.empty();
                    }

                    String data = rawLine;
                    if (data.startsWith("data: ")) {
                        data = data.substring(6);
                    }

                    if ("[DONE]".equals(data.trim())) {
                        return Flux.empty();
                    }

                    OpenAiStreamChunk chunk = safeReadValue(data, OpenAiStreamChunk.class);
                    if (chunk == null) {
                        return Flux.empty();
                    }
                    return Flux.just(chunk);
                })
                .map(formatConverter::toInternalStreamChunk)
                // 状态化累积 tool_call 参数（与父类 OpenAiCompatibleAdapter 同构）：Azure 把 arguments
                // 以增量片段下发，无状态逐事件转换会让下游 Vertex 丢掉所有工具块（见父类注释）。
                .map(new ToolCallArgsAccumulator()::process)
                .doOnNext(chunk -> log.debug("[Azure] 流式chunk: deltaContent={}", chunk.getDeltaContent()))
                .doOnComplete(() -> log.info("[Azure] 流式调用完成"))
                .doOnError(e -> logError("流式", e))
                // 上游流错误禁止吞成 deltaContent 文本（会污染客户端对话历史 + 泄露上游信息），
                // 必须 Flux.error(e) 传播到 Controller 统一转 SSE error 事件（GatewayErrorResponseBuilder.streamErrorEvents）
                ;
    }
}
