package com.llmate.multiprotocol.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.llmate.multiprotocol.constant.BusinessConstants;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.BillingContext;
import com.llmate.multiprotocol.dto.RoutingResult;
import com.llmate.multiprotocol.dto.UsageData;
import com.llmate.multiprotocol.dto.embedding.MultimodalEmbeddingRequest;
import com.llmate.multiprotocol.dto.embedding.TextEmbeddingRequest;
import com.llmate.multiprotocol.entity.ProxyChannelTokensEntity;
import com.llmate.multiprotocol.entity.ProxyChannelsEntity;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.repository.ProxyChannelTokensRepository;
import com.llmate.multiprotocol.repository.ProxyChannelsRepository;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UserContext;
import com.llmate.multiprotocol.util.WebClientUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 向量接口服务（MVC 分层：Controller 只做 HTTP，业务逻辑全在本服务）
 *
 * 照老项目 EmbeddingController / MultimodalEmbeddingController 迁移，但按当前项目
 * 的计费基建接入：预占 → 调上游 → 解析 usage → 按 EMBEDDING 模式扣费 + 结算日志。
 *
 * 渠道解析链：model_channel_configs（首个启用配置）→ proxy_channels（base_url）→
 * proxy_channel_tokens（api key）。endpointPath 优先取 custom_endpoint_path，否则用协议默认路径。
 *
 * 不做文本聊天那套 ProviderAdapter/多协议编排 —— 向量接口是轻量透传代理 + 计费。
 */
@Service
@Log4j2
public class EmbeddingService {

    /** 上游调用超时：10 分钟（慢向量大输入/DashScope 长尾留足余量，值来自 SystemConstants） */
    private static final Duration UPSTREAM_TIMEOUT =
            Duration.ofSeconds(SystemConstants.HTTP_TIMEOUT_UPSTREAM_SECONDS);

    private final ModelEndpointResolver modelEndpointResolver;
    private final ProxyChannelsRepository proxyChannelsRepository;
    private final ProxyChannelTokensRepository proxyChannelTokensRepository;
    private final BillingService billingService;
    private final SettlementService settlementService;
    private final WebClient webClient;

    public EmbeddingService(ModelEndpointResolver modelEndpointResolver,
                            ProxyChannelsRepository proxyChannelsRepository,
                            ProxyChannelTokensRepository proxyChannelTokensRepository,
                            BillingService billingService,
                            SettlementService settlementService,
                            WebClient.Builder webClientBuilder) {
        this.modelEndpointResolver = modelEndpointResolver;
        this.proxyChannelsRepository = proxyChannelsRepository;
        this.proxyChannelTokensRepository = proxyChannelTokensRepository;
        this.billingService = billingService;
        this.settlementService = settlementService;
        // 使用 ConnectionProvider.newConnection() 禁用连接池，每次请求新建 TCP+TLS 连接。
        // 国内服务器出站经阿里云 NAT，NAT 空闲超时后会断开池中旧连接，Reactor Netty 默认
        // DefaultPooledConnectionProvider 不知情，拿出来复用 → Connection reset by peer
        // （与 AbstractProviderAdapter / Anthropic / Vertex 各适配器同一处修复）。
        // 统一由 WebClientUtils 构建 HttpClient。
        HttpClient httpClient = WebClientUtils.newConnHttpClient(UPSTREAM_TIMEOUT);
        this.webClient = webClientBuilder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    // ==================== 对外入口 ====================

    /**
     * 文本向量：POST /v1/embeddings/text_embeddings
     */
    public Mono<JsonNode> textEmbeddings(TextEmbeddingRequest request, ServerWebExchange exchange) {
        return execute(request.getModel(), request,
                pureModelId -> buildTextBody(pureModelId, request),
                BusinessConstants.UPSTREAM_PATH_TEXT_EMBEDDING,
                "/v1/embeddings/text_embeddings", true, exchange);
    }

    /**
     * 多模态向量：POST /v1/embeddings/multimodal_embeddings
     */
    public Mono<JsonNode> multimodalEmbeddings(MultimodalEmbeddingRequest request, ServerWebExchange exchange) {
        return execute(request.getModel(), request,
                pureModelId -> buildMultimodalBody(pureModelId, request),
                BusinessConstants.UPSTREAM_PATH_MULTIMODAL_EMBEDDING,
                "/v1/embeddings/multimodal_embeddings", false, exchange);
    }

    // ==================== 核心流程 ====================

    /**
     * 统一执行流程：解析渠道 → 查价 → 预占 → 调上游 → 结算/清理
     */
    private Mono<JsonNode> execute(String modelId, Object clientRequest,
                                   Function<String, Map<String, Object>> bodyBuilder,
                                   String defaultEndpoint, String requestPath,
                                   boolean isTextEmbedding,
                                   ServerWebExchange exchange) {
        Long userId = UserContext.getUserId(exchange);
        Long tokenId = UserContext.getTokenId(exchange);
        ProxyTokensEntity tokenEntity = UserContext.getTokenEntity(exchange);
        String requestId = UserContext.getOrGenerateRequestId(exchange);
        long startTime = System.currentTimeMillis();

        String pureModelId = pureModelId(modelId);
        boolean hasChannelPrefix = modelId != null && modelId.contains("/");

        log.info("[Embedding] 请求开始: model={}, pureModelId={}, requestId={}", modelId, pureModelId, requestId);

        // 1. 解析渠道
        return resolveChannel(pureModelId, defaultEndpoint, modelId)
            // 2. 查价格配置
            .flatMap(channel ->
                billingService.getPriceConfig(pureModelId, channel.channelId())
                    .flatMap(priceConfig -> {
                        RoutingResult routing = RoutingResult.builder()
                            .modelId(modelId)
                            .pureModelId(pureModelId)
                            .channelCode(channel.channelCode())
                            .channelId(channel.channelId())
                            .upstreamModel(pureModelId)
                            .hasChannelPrefix(hasChannelPrefix)
                            .build();

                        BillingContext billingContext = BillingContext.builder()
                            .requestId(requestId)
                            .userId(userId)
                            .tokenId(tokenId)
                            .tokenEntity(tokenEntity)
                            .routing(routing)
                            .priceConfig(priceConfig)
                            .build();

                        // 3. 预占余额
                        return billingService.reserve(userId, requestId, priceConfig)
                            .flatMap(reserved -> {
                                // 4. 记录请求日志（开始）
                                settlementService.recordEmbeddingRequestLogStart(
                                        requestId, userId, tokenId, routing, requestPath, exchange);

                                // 5. 调上游并透传
                                Map<String, Object> upstreamBody = bodyBuilder.apply(pureModelId);
                                return callUpstream(channel, upstreamBody, requestId, userId)
                                    .flatMap(response -> {
                                        long latency = System.currentTimeMillis() - startTime;
                                        // 6. 解析 usage 并结算（内联 await，与聊天/生图一致）：
                                        //    结算返回 Mono<Void>，保证费用计算日志块（用量提取 → 计费明细 →
                                        //    计费计算 → 余额扣减 → 结算记录）在响应返回前按固定顺序打印，
                                        //    向量费用来源（价格/用量）完整可见。结算失败不阻塞响应返回。
                                        UsageData usageData = parseUsage(response, isTextEmbedding);
                                        return billingService.settleEmbedding(
                                                    billingContext, usageData, latency)
                                                .onErrorResume(e -> {
                                                    log.error("[Embedding] 结算异常但响应仍返回: requestId={}, err={}",
                                                            requestId, e.getMessage());
                                                    return Mono.empty();
                                                })
                                                .thenReturn(response);
                                    })
                                    // 7. 失败清理（上游调用失败 / usage 解析失败等）
                                    .onErrorResume(e -> {
                                        billingService.abortNonStream(billingContext,
                                                System.currentTimeMillis() - startTime, e);
                                        return Mono.error(e);
                                    });
                            });
                    }));
    }

    // ==================== 渠道解析 ====================

    /**
     * 解析向量模型渠道：proxy_channel_models → proxy_channels → proxy_channel_tokens
     * 端点路径优先取 proxy_channel_models.use_endpoint_id → endpoint.path，未绑定则用协议默认路径。
     */
    private Mono<ChannelInfo> resolveChannel(String pureModelId, String defaultEndpoint, String originalModelId) {
        return modelEndpointResolver.resolveByModelId(pureModelId)
            .switchIfEmpty(Mono.error(new LlmGatewayException(
                    LlmErrorCode.SERVICE_UNAVAILABLE, "向量模型服务未配置，模型: " + originalModelId)))
            .flatMap(mce -> {
                Long channelId = mce.channelId();
                String endpointPath = mce.endpointPath() != null ? mce.endpointPath() : defaultEndpoint;

                return proxyChannelsRepository.findById(channelId)
                    .filter(ch -> ch.getStatus() != null && ch.getStatus() == SystemConstants.STATUS_ENABLED)
                    .switchIfEmpty(Mono.error(new LlmGatewayException(
                            LlmErrorCode.SERVICE_UNAVAILABLE, "向量模型渠道不可用，模型: " + originalModelId)))
                    .flatMap(channel ->
                        proxyChannelTokensRepository.findByChannelIdAndStatus(channelId, SystemConstants.STATUS_ENABLED)
                            .next()
                            .switchIfEmpty(Mono.error(new LlmGatewayException(
                                    LlmErrorCode.CHANNEL_TOKEN_EXHAUSTED, "向量模型渠道API Key未配置，模型: " + originalModelId)))
                            .map(token -> new ChannelInfo(
                                    channel.getChannelCode(), channelId, channel.getBaseUrl(),
                                    endpointPath, token.getApiKeyEncrypted())));
            });
    }

    // ==================== 上游调用 ====================

    /**
     * 调用向量上游接口（反应式，不 block；带 ==== 方框请求/响应日志）
     */
    private Mono<JsonNode> callUpstream(ChannelInfo channel, Map<String, Object> upstreamBody,
                                        String requestId, Long userId) {
        String baseUrl = channel.baseUrl().endsWith("/")
                ? channel.baseUrl().substring(0, channel.baseUrl().length() - 1)
                : channel.baseUrl();
        String endpoint = channel.endpointPath().startsWith("/")
                ? channel.endpointPath()
                : "/" + channel.endpointPath();
        String url = baseUrl + endpoint;

        log.info("[Embedding] 上游请求: url={}", url);
        LogBox.logUpstreamRequest(channel.channelCode(), url, upstreamBody, requestId, userId);

        return webClient.post()
            .uri(url)
            .header("Authorization", "Bearer " + channel.apiKey())
            .header("Content-Type", "application/json")
            .bodyValue(upstreamBody)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .timeout(UPSTREAM_TIMEOUT)
            .doOnNext(resp -> {
                // 上游响应体走 LogBox 方框日志；异步执行避免大响应体序列化阻塞 Netty 事件循环线程
                Mono.fromRunnable(() -> LogBox.logUpstreamResponse(channel.channelCode(), resp, requestId, userId))
                        .subscribeOn(Schedulers.boundedElastic())
                        .subscribe();
            });
    }

    // ==================== 请求体构建 ====================

    /**
     * 文本向量上游请求体：{ model, input:{ texts } }，model 去掉渠道前缀
     */
    private Map<String, Object> buildTextBody(String pureModelId, TextEmbeddingRequest request) {
        Map<String, Object> body = new HashMap<>();
        body.put("model", pureModelId);
        Map<String, Object> input = new HashMap<>();
        input.put("texts", request.getInput() != null ? request.getInput().getTexts() : List.of());
        body.put("input", input);
        return body;
    }

    /**
     * 多模态向量上游请求体：{ model, input:{ contents } , parameters:{ enable_fusion } }
     * 照老项目：参数名 enable_fusion（下划线），input.contents 保留 text/image/video 字段
     */
    private Map<String, Object> buildMultimodalBody(String pureModelId, MultimodalEmbeddingRequest request) {
        Map<String, Object> body = new HashMap<>();
        body.put("model", pureModelId);

        Map<String, Object> input = new HashMap<>();
        if (request.getInput() != null && request.getInput().getContents() != null) {
            List<Map<String, String>> contents = new ArrayList<>();
            for (MultimodalEmbeddingRequest.ContentItem item : request.getInput().getContents()) {
                Map<String, String> content = new HashMap<>();
                if (item.getText() != null) {
                    content.put("text", item.getText());
                }
                if (item.getImage() != null) {
                    content.put("image", item.getImage());
                }
                if (item.getVideo() != null) {
                    content.put("video", item.getVideo());
                }
                contents.add(content);
            }
            input.put("contents", contents);
        }
        body.put("input", input);

        if (request.getParameters() != null && request.getParameters().getEnableFusion() != null) {
            Map<String, Object> parameters = new HashMap<>();
            parameters.put("enable_fusion", request.getParameters().getEnableFusion());
            body.put("parameters", parameters);
        }
        return body;
    }

    // ==================== usage 解析 ====================

    /**
     * 从上游响应解析 usage 为 UsageData（向量计费维度）
     *
     * 字段兼容（不同厂商字段名不同）：
     * - 文本向量：prompt_tokens / total_tokens（DashScope text-embedding 通常只回 total_tokens）
     * - 多模态向量：input_tokens(文本) + image_tokens(图像)，或 text_tokens + image_tokens
     *
     * 计费拆分规则：
     * - 文本向量：请求纯文本，所有 token 一律按文本向量单价计费（textTokensEmbedding → text_tokens_per_1m），
     *   即使上游只回 total_tokens 也走文本桶，绝不落到通用向量桶（避免 0.5 被 0.25 替代）。
     * - 多模态向量：能拆出文本/图像 token → 分别填 textTokensEmbedding / imageTokensEmbedding（各按自己单价计费）；
     *   只有 total_tokens → 填 vectorTokens（按通用向量单价计费）。
     * 避免同时填导致同一段 token 重复计费。
     */
    private UsageData parseUsage(JsonNode response, boolean isTextEmbedding) {
        JsonNode usage = response != null ? response.get("usage") : null;
        if (usage == null || !usage.isObject()) {
            return UsageData.builder().build();
        }

        long total = usage.path("total_tokens").asLong(0);
        long input = usage.path("input_tokens").asLong(0);
        long prompt = usage.path("prompt_tokens").asLong(0);
        long text = usage.path("text_tokens").asLong(0);
        long image = usage.path("image_tokens").asLong(0);

        if (isTextEmbedding) {
            // 纯文本：优先级 input_tokens > prompt_tokens > text_tokens > total_tokens
            long textTokens = firstNonZero(input, prompt, text, total);
            UsageData.UsageDataBuilder b = UsageData.builder()
                .inputTokens(textTokens)
                .totalTokens(textTokens);
            if (textTokens > 0) {
                b.textTokensEmbedding(textTokens);
            }
            return b.build();
        }

        long textTokens = input > 0 ? input : text;
        long totalEffective = total > 0 ? total : (textTokens + image);

        UsageData.UsageDataBuilder builder = UsageData.builder()
            .inputTokens(totalEffective)
            .totalTokens(totalEffective);

        if (textTokens > 0 || image > 0) {
            // 能拆分：文本/图像分别计费
            builder.textTokensEmbedding(textTokens).imageTokensEmbedding(image);
        } else if (total > 0) {
            // 只有总 token：按通用向量计费
            builder.vectorTokens(total);
        }
        return builder.build();
    }

    /**
     * 取第一个非 0 值（用于各厂商 usage 字段名不同的兼容）
     */
    private long firstNonZero(long... values) {
        for (long v : values) {
            if (v > 0) {
                return v;
            }
        }
        return 0;
    }

    // ==================== 内部工具方法 ====================

    /**
     * 去掉渠道前缀（第一个 / 之前的部分），取纯模型ID
     */
    private String pureModelId(String modelId) {
        if (modelId == null || !modelId.contains("/")) {
            return modelId;
        }
        return modelId.substring(modelId.indexOf('/') + 1);
    }

    /**
     * 渠道信息（baseUrl / endpointPath / apiKey / channelId / channelCode）
     */
    private record ChannelInfo(String channelCode, Long channelId, String baseUrl,
                               String endpointPath, String apiKey) {
    }
}
