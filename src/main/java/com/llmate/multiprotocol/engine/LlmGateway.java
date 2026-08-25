package com.llmate.multiprotocol.engine;

import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.*;
import com.llmate.multiprotocol.engine.provider.ProviderAdapter;
import com.llmate.multiprotocol.engine.provider.ProviderRegistry;
import com.llmate.multiprotocol.entity.ModelPricesEntity;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import com.llmate.multiprotocol.service.*;
import com.llmate.multiprotocol.util.UserContext;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * LLM网关 - 统一调度层
 *
 * 职责（仅流程控制）：
 * 1. 模型路由解析
 * 2. 根据模型路径路由到对应的 ProviderAdapter
 * 3. 端点配置解析
 * 4. 编排计费：预占 → 结算/清理（具体实现下沉 {@link BillingService}）
 * 5. 编排日志：请求日志开始/完成（具体实现下沉 {@link SettlementService}）
 *
 * 本类不含任何计费/日志的构建与持久化逻辑，只负责把流程串起来。
 */
@Component
@Log4j2
public class LlmGateway {

    /**
     * 网关生成的 requestId 存放到 exchange 属性的键名。
     * proxy_request_logs 的 request_id 列用的是网关生成的 id（而非 WebFilter 的 LogBox id），
     * RequestLoggingWebFilter 需要读取它以回填流式响应体。
     */
    public static final String REQUEST_ID_ATTR = "llmGatewayRequestId";

    private final ProviderRegistry providerRegistry;
    private final ModelRouter modelRouter;
    private final ModelEndpointResolver modelEndpointResolver;
    private final BillingService billingService;
    private final SettlementService settlementService;
    private final TokenSelectorService tokenSelectorService;

    public LlmGateway(ProviderRegistry providerRegistry,
                      ModelRouter modelRouter,
                      ModelEndpointResolver modelEndpointResolver,
                      BillingService billingService,
                      SettlementService settlementService,
                      TokenSelectorService tokenSelectorService) {
        this.providerRegistry = providerRegistry;
        this.modelRouter = modelRouter;
        this.modelEndpointResolver = modelEndpointResolver;
        this.billingService = billingService;
        this.settlementService = settlementService;
        this.tokenSelectorService = tokenSelectorService;
    }

    /**
     * 非流式调用
     */
    public Mono<LlmChatResponse> chat(LlmChatRequest internalRequest, ServerWebExchange exchange) {
        String modelPath = internalRequest.getModel();
        String requestId = UserContext.getOrGenerateRequestId(exchange);
        exchange.getAttributes().put(REQUEST_ID_ATTR, requestId);
        log.info("[LlmGateway] 非流式调用开始: model={}, requestId={}", modelPath, requestId);

        return routeAndExecute(modelPath, internalRequest, requestId, false, exchange);
    }

    /**
     * 统一执行入口（非流式）：按 internalRequest.requestType 派发能力
     * 文本聊天（默认）/ 图像生成 / 图像编辑 共用同一套路由、预占、计费、日志编排。
     */
    public Mono<LlmChatResponse> execute(LlmChatRequest internalRequest, ServerWebExchange exchange) {
        String modelPath = internalRequest.getModel();
        String requestId = UserContext.getOrGenerateRequestId(exchange);
        exchange.getAttributes().put(REQUEST_ID_ATTR, requestId);
        log.info("[LlmGateway] 统一执行开始: model={}, requestType={}, requestId={}",
            modelPath, internalRequest.getRequestType(), requestId);

        return routeAndExecute(modelPath, internalRequest, requestId, false, exchange);
    }

    /**
     * 流式调用（兼容入口）。
     *
     * 内部先走 {@link #prepareStream} 完成 路由/价格/余额预占 等 setup 阶段再订阅上游流。
     * 注意：setup 阶段错误（余额不足等）会随内部 Mono 失败传播到本 Flux，旧调用方需自行
     * 将其转成流内 SSE error 事件。
     *
     * @deprecated 建议 Controller 改用 {@link #prepareStream}：在 SSE 响应提交【之前】完成预占，
     *             setup 失败可直接返回普通 HTTP 错误（如 402 余额不足），而非 200 + SSE error 事件。
     */
    @Deprecated
    public Flux<LlmStreamChunk> chatStream(LlmChatRequest internalRequest, ServerWebExchange exchange) {
        return prepareStream(internalRequest, exchange).flatMapMany(PreparedStream::getFlux);
    }

    /**
     * 流式请求预飞：在响应提交【之前】完成 模型路由 → 渠道解析 → 价格查询 → 余额预占 →
     * 请求日志开始 → 端点解析 全部 setup 阶段，返回持有「已预占、可直接订阅」上游流的
     * {@link PreparedStream}。
     *
     * 关键价值：setup 阶段任何业务错误（如 BALANCE_INSUFFICIENT）都会在此 Mono 上失败——此时
     * SSE 的 200 响应头尚未发出，Controller 可交给 {@link com.llmate.multiprotocol.config.GlobalExceptionHandler}
     * 返回普通 HTTP 错误响应（402 余额不足），客户端拿到的是干净的报错，而不是 200 + SSE error 事件。
     *
     * 预占的余额由 {@link PreparedStream#getFlux()}（executeStreamAndHandleBilling）在
     * 正常结算 / 失败清理 / 客户端断开兜底中释放，与旧 chatStream 行为一致。
     */
    public Mono<PreparedStream> prepareStream(LlmChatRequest internalRequest, ServerWebExchange exchange) {
        String modelPath = internalRequest.getModel();
        String requestId = UserContext.getOrGenerateRequestId(exchange);
        exchange.getAttributes().put(REQUEST_ID_ATTR, requestId);
        log.info("[LlmGateway] 流式预飞开始: model={}, requestId={}", modelPath, requestId);

        long startTime = System.currentTimeMillis();
        return modelRouter.resolve(modelPath)
            .flatMap(routing -> prepareStreamWithBilling(routing, internalRequest, requestId, startTime, exchange));
    }

    /**
     * 路由并执行非流式调用（编排计费与日志）
     * 关键修复：所有副作用操作均在响应 commit 前完成
     */
    private Mono<LlmChatResponse> routeAndExecute(String modelPath, LlmChatRequest internalRequest, String requestId, boolean isStream, ServerWebExchange exchange) {
        long startTime = System.currentTimeMillis();

        return modelRouter.resolve(modelPath)
            .flatMap(routing ->
                // 响应式获取 Provider（支持懒加载：新增渠道首次访问自动从 DB 加载并注册到内存）
                findProviderAsync(routing.getChannelCode(), routing.getProviderAlias())
                    .flatMap(provider -> {
                        // 保存原始请求信息到 LlmChatRequest
                        // 注意：用 routing.getModelId()（客户端原始模型ID，含渠道前缀，如 deepseek/deepseek-v4-flash），
                        // 而不是 pureModelId（去掉渠道后的模型名）。proxy_request_logs 需要记录原始模型ID。
                        internalRequest.setOriginalModelId(routing.getModelId());
                        internalRequest.setChannelId(routing.getChannelId());
                        internalRequest.setChannelCode(routing.getChannelCode());

                        // 替换为上游真实模型名
                        internalRequest.setModel(routing.getUpstreamModel());

                        log.info("[LlmGateway] 非流式调用路由到Provider: {}, providerAlias={}, upstreamModel={}",
                            provider.getProviderName(), routing.getProviderAlias(), routing.getUpstreamModel());

                        // 获取用户信息
                        Long userId = UserContext.getUserId(exchange);
                        Long tokenId = UserContext.getTokenId(exchange);
                        ProxyTokensEntity tokenEntity = UserContext.getTokenEntity(exchange);

                        // 1. 获取模型价格配置
                        return billingService.getPriceConfig(routing.getUpstreamModel(), routing.getChannelId())
                            .flatMap(priceConfig ->
                                // 2. 预估费用并预占余额
                                billingService.reserve(userId, requestId, priceConfig)
                                    .flatMap(reservedBalance -> {
                                        // 3. 记录请求开始日志（同步操作，不阻塞）
                                        settlementService.recordRequestLogStart(requestId, userId, tokenId, routing, isStream, exchange);

                                        // 4. 解析端点配置
                                        return resolveEndpointConfig(routing.getUpstreamModel(), routing.getChannelId())
                                            .flatMap(endpointConfig -> {
                                                log.info("[LlmGateway] 使用端点配置: baseUrl={}, endpointPath={}",
                                                    endpointConfig.getBaseUrl() != null ? endpointConfig.getBaseUrl() : "(默认)",
                                                    endpointConfig.getEndpointPath());

                                                // 5. 执行请求
                                                BillingContext billingContext = BillingContext.builder()
                                                    .requestId(requestId)
                                                    .userId(userId)
                                                    .tokenId(tokenId)
                                                    .tokenEntity(tokenEntity)
                                                    .routing(routing)
                                                    .priceConfig(priceConfig)
                                                    .build();

                                                Mono<LlmChatResponse> providerCall = dispatchProviderCall(provider, internalRequest, endpointConfig);
                                                return executeWithBilling(provider, providerCall, internalRequest, billingContext, startTime);
                                            });
                                    })
                            );
                    })
            );
    }

    /**
     * 按请求类型派发到 ProviderAdapter 对应能力方法
     * 文本聊天 → chat；图像生成 → generateImage；图像编辑 → editImage。
     *
     * 生图/生视频渠道按"渠道显式能力类型"路由（如 openai_image / dashscope_image / gemini_image）：
     * ProviderFactory 按渠道 type 创建对应专用适配器，一个渠道只服务一种能力（一个上游拆成多个子渠道）。
     */
    private Mono<LlmChatResponse> dispatchProviderCall(ProviderAdapter provider, LlmChatRequest req, ModelEndpointConfig endpointConfig) {
        LlmRequestType type = req.getRequestType() != null ? req.getRequestType() : LlmRequestType.CHAT_COMPLETION;
        return switch (type) {
            case IMAGE_GENERATION -> provider.generateImage(req, endpointConfig);
            case IMAGE_EDIT       -> provider.editImage(req, endpointConfig);
            default               -> provider.chat(req, endpointConfig);
        };
    }

    /**
     * 执行请求并编排结算/清理
     * 结算与失败清理均委托 {@link BillingService}，不阻塞主响应流
     */
    private Mono<LlmChatResponse> executeWithBilling(
            ProviderAdapter provider,
            Mono<LlmChatResponse> providerCall,
            LlmChatRequest internalRequest,
            BillingContext billingContext,
            long startTime) {

        // 记录请求是否成功（用于 reportTokenUsage）
        AtomicBoolean successFlag = new AtomicBoolean(true);

        // 通过 Reactor Context 传递 requestId/userId 到 Provider 层，供上游请求/响应日志使用
        return providerCall
            .contextWrite(ctx -> ctx
                .put("requestId", billingContext.getRequestId())
                .put("userId", billingContext.getUserId()))
            .doOnNext(resp -> {
                // 设置响应上下文信息用于反向映射
                resp.setOriginalModelId(internalRequest.getOriginalModelId());
                resp.setChannelId(internalRequest.getChannelId());
                resp.setResponseModelId(billingContext.getRouting().getResponseModelId());
            })
            .flatMap(resp -> {
                long latency = System.currentTimeMillis() - startTime;

                // 计费内联执行（返回 Mono<Void>）：保证计费日志块（用量提取 → 计费明细 → 计费计算 →
                // 余额扣减 → 结算记录）在响应返回前按固定顺序打印完，任何请求类型（文本聊天/生图/生视频）
                // 日志标准一致。结算失败不阻塞响应返回（onErrorResume 吞掉，错误已在结算层留痕）。
                return billingService.settleNonStream(billingContext, resp, latency)
                    .onErrorResume(e -> {
                        log.error("[LlmGateway] 结算异常但响应仍返回: requestId={}, err={}",
                            billingContext.getRequestId(), e.getMessage());
                        return Mono.empty();
                    })
                    .thenReturn(resp);
            })
            .onErrorResume(e -> {
                successFlag.set(false);
                log.error("[LlmGateway] 非流式调用失败", e);
                // 失败清理：释放预占余额 + 回填失败日志（独立执行）
                billingService.abortNonStream(billingContext, System.currentTimeMillis() - startTime, e);

                if (e instanceof LlmGatewayException) {
                    return Mono.error(e);
                }
                // 保 cause 链：原 varargs 构造器会走 errorCode.format() 丢弃 cause，内部日志必须能看到
                // 上游 WebClientResponseException（含 URL/状态/堆栈）。对外文案由响应构建器统一脱敏。
                return Mono.error(new LlmGatewayException(
                    LlmErrorCode.PROVIDER_ERROR,
                    "Provider '" + provider.getProviderName() + "' 调用失败：" + e.getMessage(), e));
            })
            .doFinally(signal -> {
                // 方案 C：上报 Token 使用完成，释放 usage 计数
                Long tokenId = provider.getCurrentTokenId();
                if (tokenId != null) {
                    boolean success = successFlag.get() && (signal == SignalType.ON_COMPLETE || signal == SignalType.CANCEL);
                    tokenSelectorService.reportTokenUsage(tokenId, success)
                        .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, billingContext.getRequestId()))
                        .doOnError(e -> log.warn("[LlmGateway] 上报Token使用失败: tokenId={}, error={}", tokenId, e.getMessage()))
                        .subscribe();
                }
            });
    }

    /**
     * 流式 setup 阶段（在响应提交前执行，Mono）：保存原始模型信息 → 构建 StreamContext →
     * 响应式获取 Provider → 查价格 → 预占余额 → 记录请求日志开始 → 解析端点配置。
     *
     * 返回持有「可直接订阅」上游流的 {@link PreparedStream}。任何一步失败（如余额不足）都在
     * 此 Mono 上抛出，保证客户端在 SSE 响应头发出前就能拿到普通 HTTP 错误。
     */
    private Mono<PreparedStream> prepareStreamWithBilling(
            RoutingResult routing,
            LlmChatRequest internalRequest,
            String requestId,
            long startTime,
            ServerWebExchange exchange) {

        // 保存原始请求信息（记录客户端原始模型ID，而非去渠道后的纯模型名）
        internalRequest.setOriginalModelId(routing.getModelId());
        internalRequest.setChannelId(routing.getChannelId());
        internalRequest.setChannelCode(routing.getChannelCode());
        internalRequest.setModel(routing.getUpstreamModel());

        // 创建流式处理上下文（provider 在 findProviderAsync 回调中延迟设置）
        StreamContext context = new StreamContext();
        context.routing = routing;
        context.internalRequest = internalRequest;
        context.requestId = requestId;
        context.startTime = startTime;
        context.userId = UserContext.getUserId(exchange);
        context.tokenId = UserContext.getTokenId(exchange);
        context.tokenEntity = UserContext.getTokenEntity(exchange);

        // 响应式获取 Provider（支持懒加载：新增渠道首次访问自动从 DB 加载并注册到内存）
        return findProviderAsync(routing.getChannelCode(), routing.getProviderAlias())
            .flatMap(provider -> {
                context.provider = provider;

                log.info("[LlmGateway] 流式调用路由到Provider: {}, providerAlias={}, upstreamModel={}",
                    provider.getProviderName(), routing.getProviderAlias(), routing.getUpstreamModel());

                return billingService.getPriceConfig(routing.getUpstreamModel(), routing.getChannelId())
                    .flatMap(priceConfig -> {
                        context.priceConfig = priceConfig;

                        return billingService.reserve(context.userId, requestId, priceConfig)
                            .flatMap(reservedBalance -> {
                                // 关键修复：之前误传 null 导致流式请求的 request_headers/client_ip/user_agent 全部落空
                                settlementService.recordRequestLogStart(requestId, context.userId, context.tokenId, routing, true, exchange);

                                return resolveEndpointConfig(routing.getUpstreamModel(), routing.getChannelId())
                                    .map(endpointConfig -> {
                                        log.info("[LlmGateway] 使用端点配置: baseUrl={}, endpointPath={}",
                                            endpointConfig.getBaseUrl() != null ? endpointConfig.getBaseUrl() : "(默认)",
                                            endpointConfig.getEndpointPath());

                                        return new PreparedStream(executeStreamAndHandleBilling(context, endpointConfig));
                                    });
                            });
                    });
            });
    }

    /**
     * 执行流式请求并编排结算/清理
     * 结算与失败清理均委托 {@link BillingService}，不阻塞主响应链路
     */
    private Flux<LlmStreamChunk> executeStreamAndHandleBilling(StreamContext ctx, ModelEndpointConfig endpointConfig) {
        StreamUsageAccumulator usageAccumulator = new StreamUsageAccumulator();
        BillingContext billingContext = ctx.toBillingContext();
        AtomicReference<Long> firstChunkTimeRef = new AtomicReference<>(null);
        AtomicBoolean terminated = new AtomicBoolean(false);

        // 通过 Reactor Context 传递 requestId/userId 到 Provider 层，供上游请求/响应日志使用
        return ctx.provider.chatStream(ctx.internalRequest, endpointConfig)
            .contextWrite(ctxView -> ctxView
                .put("requestId", billingContext.getRequestId())
                .put("userId", billingContext.getUserId()))
            .doOnNext(chunk -> {
                // 记录首块到达时间：首块延迟 = 首块时间 - 请求开始时间
                if (firstChunkTimeRef.get() == null) {
                    firstChunkTimeRef.set(System.currentTimeMillis());
                    // 标记流内首块：Anthropic 外部协议要求 SSE 必须以 message_start 开头（由首块
                    // isFirstChunk=true 触发），而 OpenAI/Azure 等上游转换器从不设 isFirstChunk
                    // （只有 Anthropic 上游转换器在收到上游 message_start 时设置）。若不标记，
                    // message_start 永不发射，SSE 直接以 content_block_start 开头——严格客户端
                    // （Claude Code SDK）判定流非法，会在流结束后自动降级为【非流式重发】（stream
                    // 字段被省略），导致上游重复调用、请求重复计费。这里在统一网关层标记首块，覆盖所有上游。
                    // 注意加 !isFirstChunk 守卫：Anthropic 上游首块（上游 message_start 转换而来）
                    // 已被转换器标记过，直接放行避免重复发射 message_start。
                    if (!chunk.isFirstChunk()) {
                        chunk.setFirstChunk(true);
                    }
                }
                chunk.setOriginalModelId(ctx.internalRequest.getOriginalModelId());
                chunk.setResponseModelId(ctx.routing.getResponseModelId());
                usageAccumulator.accumulate(chunk);
            })
            .doOnComplete(() -> {
                terminated.set(true);
                long latency = System.currentTimeMillis() - ctx.startTime;
                UsageData usageData = usageAccumulator.toUsageData();

                billingService.settleStream(billingContext, usageData, latency,
                    firstChunkLatencyMs(ctx.startTime, firstChunkTimeRef.get()), usageAccumulator.getChunkCount());
            })
            .doOnError(e -> {
                log.error("[LlmGateway] 流式调用失败", e);
                terminated.set(true);
                billingService.abortStream(billingContext, usageAccumulator.toUsageData(),
                    System.currentTimeMillis() - ctx.startTime,
                    firstChunkLatencyMs(ctx.startTime, firstChunkTimeRef.get()), e, usageAccumulator.getChunkCount());
            })
            .doFinally(signal -> {
                // 兜底：客户端中途断开(CANCEL)时 doOnComplete/doOnError 都不触发 → 结算回填完全跳过，
                // proxy_request_logs 的 stream_chunks / first_chunk_latency_ms 落空，且预占额度泄漏。
                // 这里按失败路径释放预占 + 回填日志，确保两个字段有值。
                if (!terminated.get()) {
                    log.warn("[LlmGateway] 流式请求提前终止({})，兜底清理: requestId={}", signal, ctx.requestId);
                    billingService.abortStream(billingContext, usageAccumulator.toUsageData(),
                        System.currentTimeMillis() - ctx.startTime,
                        firstChunkLatencyMs(ctx.startTime, firstChunkTimeRef.get()),
                        new LlmGatewayException(LlmErrorCode.INTERNAL_ERROR, "stream " + signal),
                        usageAccumulator.getChunkCount());
                }

                // 方案 C：上报 Token 使用完成，释放 usage 计数
                Long tokenId = ctx.provider.getCurrentTokenId();
                if (tokenId != null) {
                    boolean success = terminated.get() && signal == SignalType.ON_COMPLETE;
                    tokenSelectorService.reportTokenUsage(tokenId, success)
                        .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, ctx.requestId))
                        .doOnError(e -> log.warn("[LlmGateway] 上报Token使用失败: tokenId={}, error={}", tokenId, e.getMessage()))
                        .subscribe();
                }
            })
            .onErrorResume(e -> {
                // 上游调用失败时，不能把异常信息伪装成模型输出文本流给客户端。
                // 若将 "[ERROR] Provider调用失败: ..." 当作 deltaContent 下传，
                // 客户端（Codex Desktop 等）会将其作为 assistant 消息存入对话历史，
                // 下一轮请求带回后毒化模型上下文，形成"报错→存入历史→带回→再报错"的恶性循环。
                // 正确做法：让异常沿 Flux 链向上传播，由各协议 Converter 和 Controller
                // 的 onErrorResume 将其转为 SSE error 事件，客户端据此识别为流错误而非文本输出。
                log.error("[LlmGateway] Provider流式调用失败，向上传播异常", e);
                return Flux.error(e);
            });
    }

    /**
     * 首块延迟：首块到达时间 - 请求开始时间；流未产生任何 chunk（立即失败/断开）时为 0
     */
    private long firstChunkLatencyMs(long startTime, Long firstChunkTime) {
        if (firstChunkTime == null) {
            return 0;
        }
        long latency = firstChunkTime - startTime;
        return latency >= 0 ? latency : 0;
    }

    /**
     * 流式处理上下文
     */
    private static class StreamContext {
        RoutingResult routing;
        LlmChatRequest internalRequest;
        String requestId;
        long startTime;
        ProviderAdapter provider;
        ServerWebExchange exchange;
        Long userId;
        Long tokenId;
        ProxyTokensEntity tokenEntity;
        ModelPricesEntity priceConfig;

        /**
         * 转换为计费上下文，供 BillingService 使用
         */
        BillingContext toBillingContext() {
            return BillingContext.builder()
                .requestId(requestId)
                .userId(userId)
                .tokenId(tokenId)
                .tokenEntity(tokenEntity)
                .routing(routing)
                .priceConfig(priceConfig)
                .build();
        }
    }

    /**
     * 解析端点配置
     * 查询 model_channel_configs 和 model_templates 表获取完整的 URL 配置
     */
    private Mono<ModelEndpointConfig> resolveEndpointConfig(String upstreamModel, Long channelId) {
        return modelEndpointResolver.resolve(upstreamModel, channelId)
            .onErrorResume(e -> {
                log.warn("[LlmGateway] 解析端点配置失败，使用默认配置: {}", e.getMessage());
                // 降级到默认配置
                return Mono.just(ModelEndpointConfig.builder()
                    .endpointPath("chat/completions")
                    .httpMethod("POST")
                    .build());
            });
    }

    /**
     * 根据渠道代码 + providerAlias 找到对应的Provider（响应式，支持懒加载）。
     *
     * 查询优先级：内存精确匹配 → 数据库懒加载（首次访问时自动注册）。不再有任何按渠道 type
     * 的默认兜底（type 无意义）：alias 只能来自绑定行 provider_capability，渠道/alias 对不上即报错。
     * 懒加载完成后该渠道的所有 adapter 均注册到 ProviderRegistry 内存表，
     * 后续请求走纯内存快路径，零 DB 开销。
     */
    private Mono<ProviderAdapter> findProviderAsync(String channelCode, String providerAlias) {
        return providerRegistry.getOrLoadChatProvider(channelCode, providerAlias)
            .switchIfEmpty(Mono.error(new LlmGatewayException(LlmErrorCode.CHANNEL_NOT_FOUND, channelCode)));
    }

}
