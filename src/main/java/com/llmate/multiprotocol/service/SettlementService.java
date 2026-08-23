package com.llmate.multiprotocol.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.dto.BillingResult;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmImage;
import com.llmate.multiprotocol.dto.LlmImageInput;
import com.llmate.multiprotocol.dto.LlmImageParams;
import com.llmate.multiprotocol.dto.LlmRequestType;
import com.llmate.multiprotocol.dto.LlmVideoParams;
import com.llmate.multiprotocol.dto.RoutingResult;
import com.llmate.multiprotocol.dto.UsageData;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.entity.ProxyLogsEntity;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.filter.RequestLoggingWebFilter;
import com.llmate.multiprotocol.repository.ProxyLogsRepository;
import com.llmate.multiprotocol.util.LogBox;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 结算/审计日志服务
 *
 * 职责：集中处理合并后的 proxy_logs（结算 + 全量请求/响应审计，原 proxy_request_logs 已并入）
 * 的构建、持久化与回填。LlmGateway 只负责调度，不掺入日志细节。
 *
 * 持久化均为异步（boundedElastic），不阻塞响应主链路。
 *
 * 单表写入纪律（照 proxy_request_logs 时代教训，避免并发整行覆盖）：
 * - recordRequestLogStart 建行（INSERT，审计字段 + status='processing'）；
 * - 结算回填用部分 UPDATE（updateSettlementByRequestId，列集 = 结算字段）；
 * - 完成/响应回填用部分 UPDATE（updateCompletionByRequestId / updateStreamCompletion /
 *   updateStreamResponseBody，列集 = 审计字段），与结算列集不相交。
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class SettlementService {

    private final ProxyLogsRepository proxyLogsRepository;
    private final ObjectMapper objectMapper;

    // 注意：proxy_logs.response_body 是 longtext，响应体完整存储、不做任何截断。
    // 流式响应体由 RequestLoggingWebFilter 完整捕获后经 recordStreamResponseBody 回填；
    // 非流式响应体由 recordRequestLogComplete 完整写入。errorMessage 仍按 2000 字符截断。

    // ==================== proxy_logs 结算 + 审计日志 ====================

    /**
     * 记录非流式结算日志（部分更新回填结算字段，行由 recordRequestLogStart 建好）。
     * billing_detail 为该请求计费多行明细（tokens 消耗 + 各维度费用）。
     */
    public void recordSettlementLog(
            String requestId,
            Long userId,
            Long tokenId,
            ProxyTokensEntity tokenEntity,
            RoutingResult routing,
            LlmChatResponse resp,
            BillingResult costResult,
            long latency,
            String status,
            String errorMsg) {

        ProxyLogsEntity logsEntity = ProxyLogsEntity.builder()
            .userId(userId)
            .tokenId(tokenId)
            .tokenName(tokenEntity != null ? tokenEntity.getTokenName() : null)
            .channelId(routing.getChannelId())
            .requestId(requestId)
            .channelName(routing.getChannelCode())
            // model 记录客户端原始模型ID（含渠道前缀，如 deepseek/deepseek-v4-flash）
            .model(routing.getModelId())
            .promptTokens(resp.getUsage() != null ? resp.getUsage().getPromptTokens() : 0)
            .completionTokens(resp.getUsage() != null ? resp.getUsage().getCompletionTokens() : 0)
            .quotaConsumed(costResult.getQuota())
            .latencyMs((int) latency)
            .status(status)
            .errorMsg(errorMsg)
            .isThinking(0)
            .priceMarkup(costResult.getPackageMarkup())
            .billingDetail(costResult.getBillingDetail())
            .createdAt(LocalDateTime.now())
            .aborted(0)
            .build();

        recordSettlement(logsEntity);
    }

    /**
     * 记录流式结算日志（部分更新回填结算字段，行由 recordRequestLogStart 建好）。
     */
    public void recordStreamSettlementLog(
            String requestId,
            Long userId,
            Long tokenId,
            ProxyTokensEntity tokenEntity,
            RoutingResult routing,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            String status) {

        ProxyLogsEntity logsEntity = ProxyLogsEntity.builder()
            .userId(userId)
            .tokenId(tokenId)
            .tokenName(tokenEntity != null ? tokenEntity.getTokenName() : null)
            .channelId(routing.getChannelId())
            .requestId(requestId)
            .channelName(routing.getChannelCode())
            // model 记录客户端原始模型ID（含渠道前缀，如 deepseek/deepseek-v4-flash）
            .model(routing.getModelId())
            .promptTokens((int) usageData.getInputTokens())
            .completionTokens((int) usageData.getOutputTokens())
            .quotaConsumed(costResult.getQuota())
            .latencyMs((int) latency)
            .status(status)
            .isThinking(0)
            .priceMarkup(costResult.getPackageMarkup())
            .billingDetail(costResult.getBillingDetail())
            .createdAt(LocalDateTime.now())
            .aborted(0)
            .build();

        recordSettlement(logsEntity);
    }

    /**
     * 结算回填（部分 UPDATE，行已由 recordRequestLogStart 建好；罕见未建行则插入结算快照）。
     * 更新列集（结算字段）与完成回填（审计字段）不相交，避免并发整行覆盖。
     */
    private void recordSettlement(ProxyLogsEntity proxyLogs) {
        LogBox.logSettlement(proxyLogs.getRequestId(), proxyLogs.getUserId(), proxyLogs);

        Mono.fromCallable(() -> {
            Integer updated = proxyLogsRepository.updateSettlementByRequestId(
                    proxyLogs.getRequestId(),
                    proxyLogs.getTokenName(),
                    proxyLogs.getChannelName(),
                    proxyLogs.getPromptTokens(),
                    proxyLogs.getCompletionTokens(),
                    proxyLogs.getQuotaConsumed(),
                    proxyLogs.getLatencyMs(),
                    proxyLogs.getStatus(),
                    proxyLogs.getErrorMsg(),
                    proxyLogs.getIsThinking(),
                    proxyLogs.getPriceMarkup(),
                    proxyLogs.getBillingDetail(),
                    proxyLogs.getAborted())
                .block();
            if (updated == null || updated == 0) {
                // 行未建（罕见：无 recordRequestLogStart 路径）→ 直接插入结算快照
                proxyLogsRepository.save(proxyLogs).block();
            }
            return null;
        }).subscribeOn(Schedulers.boundedElastic())
            .doOnError(e -> log.error("Failed to save settlement log", e))
            // 独立订阅链 Context 为空，补写 requestId 让 boundedElastic 线程上日志 [reqId=] 不为空
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, proxyLogs.getRequestId()))
            .subscribe();
    }

    // ==================== 请求/响应审计日志（并入 proxy_logs） ====================

    /**
     * 记录请求日志（开始）：建行（INSERT）。
     * 补全请求头、请求体、请求大小等字段；status 置 'processing'，结算回填后改为 success/error。
     */
    public void recordRequestLogStart(
            String requestId,
            Long userId,
            Long tokenId,
            RoutingResult routing,
            LlmChatRequest request,
            boolean isStream,
            ServerWebExchange exchange) {

        String clientIp = resolveClientIp(exchange);

        String userAgent = exchange != null ? exchange.getRequest().getHeaders().getFirst("User-Agent") : null;

        String requestHeadersJson = buildRequestHeadersJson(exchange);
        // 请求体优先用客户端原始上报数据（过滤器缓存的 rawRequestBody），
        // multipart（编辑）等无原始 JSON 的场景回退到内部请求重建 + base64 脱敏。
        String requestBodyJson = buildRequestLogBody(request, routing, exchange);
        Integer requestSizeBytes = requestBodyJson != null ? requestBodyJson.length() : 0;

        // 请求路径取实际路径（生图 /v1/images/generations、编辑 /v1/images/edits 等）
        String requestPath = exchange != null && exchange.getRequest().getPath() != null
                ? exchange.getRequest().getPath().value() : "/v1/chat/completions";

        ProxyLogsEntity logsEntity = ProxyLogsEntity.builder()
            .requestId(requestId)
            .userId(userId)
            .tokenId(tokenId)
            .channelId(routing.getChannelId())
            // model 列记录客户端原始模型ID，而非去渠道后的上游模型名
            .model(routing.getModelId())
            .requestMethod("POST")
            .requestPath(requestPath)
            .requestHeaders(requestHeadersJson)
            .requestBody(requestBodyJson)
            .requestSizeBytes(requestSizeBytes)
            .isStream(isStream ? 1 : 0)
            .status("processing")
            .clientIp(clientIp)
            .userAgent(userAgent)
            .createdAt(LocalDateTime.now())
            .build();

        recordRequest(logsEntity);
    }

    /**
     * 记录向量接口请求日志（开始）：建行（INSERT）。
     * 与 {@link #recordRequestLogStart} 的区别：requestPath 和 requestBody 直接传入
     * （向量请求不是 LlmChatRequest，无法走 buildRequestLogBody），其余字段逻辑一致。
     */
    public void recordEmbeddingRequestLogStart(
            String requestId,
            Long userId,
            Long tokenId,
            RoutingResult routing,
            String requestPath,
            String requestBodyJson,
            ServerWebExchange exchange) {

        String clientIp = resolveClientIp(exchange);

        String userAgent = exchange != null ? exchange.getRequest().getHeaders().getFirst("User-Agent") : null;

        String requestHeadersJson = buildRequestHeadersJson(exchange);
        Integer requestSizeBytes = requestBodyJson != null ? requestBodyJson.length() : 0;

        ProxyLogsEntity logsEntity = ProxyLogsEntity.builder()
            .requestId(requestId)
            .userId(userId)
            .tokenId(tokenId)
            .channelId(routing.getChannelId())
            .model(routing.getModelId())
            .requestMethod("POST")
            .requestPath(requestPath)
            .requestHeaders(requestHeadersJson)
            .requestBody(requestBodyJson)
            .requestSizeBytes(requestSizeBytes)
            .isStream(0)
            .status("processing")
            .clientIp(clientIp)
            .userAgent(userAgent)
            .createdAt(LocalDateTime.now())
            .build();

        recordRequest(logsEntity);
    }

    /**
     * 记录向量接口请求日志（完成）—— UsageData 版，不依赖 LlmChatResponse。
     * 异步回填：response_status / response_body / response_size_bytes / tokens /
     * quota_consumed / latency_ms / completed_at / error_msg，并兜底结算 status。
     */
    public void recordEmbeddingRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            int statusCode,
            String responseBodyJson,
            String errorMessage) {

        String truncatedError = truncateLog(errorMessage, 2000);
        String status = statusCode < 400 ? "success" : "error";

        proxyLogsRepository.updateCompletionByRequestId(
                requestId,
                statusCode,
                "{\"Content-Type\":\"application/json\"}",
                responseBodyJson,
                responseBodyJson != null ? responseBodyJson.length() : null,
                (int) latency,
                LocalDateTime.now(),
                usageData != null ? (int) usageData.getInputTokens() : null,
                usageData != null ? (int) usageData.getOutputTokens() : null,
                usageData != null ? (int) usageData.getTotalTokens() : null,
                costResult != null ? costResult.getQuota() : null,
                status,
                truncatedError)
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(saved -> log.debug("向量请求日志更新完成: requestId={}, statusCode={}, latency={}ms",
                        requestId, statusCode, latency))
            .doOnError(err -> log.error("向量请求日志更新失败: requestId={}", requestId, err))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId))
            .subscribe(v -> {}, err -> {});
    }

    /**
     * 记录视频接口请求日志（完成）—— UsageData 版，薄封装 {@link #recordEmbeddingRequestLogComplete}。
     * 状态码按是否有错误信息取 200/500；响应体 JSON 含 video_url/cover_url/completion_tokens。
     */
    public void recordVideoRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            String responseBodyJson,
            String errorMessage) {

        recordEmbeddingRequestLogComplete(
            requestId, usageData, costResult, latency,
            errorMessage != null ? 500 : 200, responseBodyJson, errorMessage);
    }

    /**
     * 记录 ASR 转写接口请求日志（完成）—— UsageData 版，薄封装 {@link #recordEmbeddingRequestLogComplete}。
     * 状态码由调用方显式传入（成功 200 / 失败 500）；响应体 JSON 含转写 text（不落音频二进制，只落文本摘要）。
     */
    public void recordAudioRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            int statusCode,
            String responseBodyJson,
            String errorMessage) {

        recordEmbeddingRequestLogComplete(
            requestId, usageData, costResult, latency,
            statusCode, responseBodyJson, errorMessage);
    }

    /**
     * 记录非流式请求日志（完成）—— 异步回填响应相关字段。
     * 补全：response_status / response_headers / response_body / response_size_bytes /
     * tokens / quota_consumed / latency_ms / completed_at / error_msg，并兜底结算 status。
     */
    public void recordRequestLogComplete(
            String requestId,
            LlmChatResponse resp,
            BillingResult costResult,
            long latency,
            int statusCode,
            String errorMessage) {

        String truncatedError = truncateLog(errorMessage, 2000);
        String status = statusCode < 400 ? "success" : "error";

        Integer promptTokens = null;
        Integer completionTokens = null;
        Integer totalTokens = null;
        String responseHeaders = null;
        String responseBody = null;
        Integer responseSizeBytes = null;

        if (resp != null) {
            if (resp.getUsage() != null) {
                promptTokens = resp.getUsage().getPromptTokens();
                completionTokens = resp.getUsage().getCompletionTokens();
                totalTokens = resp.getUsage().getTotalTokens();
            }
            // 图像响应含大体积 base64，序列化前脱敏（保留 url/contentType/index）
            String bodyJson = buildJson(sanitizeResponseForLog(resp));
            if (bodyJson != null) {
                // 响应体完整存储（response_body 是 longtext，不做任何截断）
                responseBody = bodyJson;
                responseSizeBytes = bodyJson.length();
            }
            responseHeaders = "{\"Content-Type\":\"application/json\"}";
        }

        proxyLogsRepository.updateCompletionByRequestId(
                requestId,
                statusCode,
                responseHeaders,
                responseBody,
                responseSizeBytes,
                (int) latency,
                LocalDateTime.now(),
                promptTokens,
                completionTokens,
                totalTokens,
                costResult != null ? costResult.getQuota() : null,
                status,
                truncatedError)
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(saved -> log.debug("请求日志更新完成: requestId={}, statusCode={}, latency={}ms",
                        requestId, statusCode, latency))
            .doOnError(err -> log.error("请求日志更新失败: requestId={}", requestId, err))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId))
            .subscribe(v -> {}, err -> {});
    }

    /**
     * 记录流式请求日志（完成）—— 异步部分更新回填结算相关字段。
     *
     * 注意：这里只更新结算拥有的列（status/latency/stream_chunks/tokens/cost/billing_detail/headers），
     * 不再写 response_body —— 真实 SSE 响应体由 RequestLoggingWebFilter 在响应写完后
     * 单独部分更新（recordStreamResponseBody），两条 UPDATE 列集不相交，避免并发整行覆盖。
     */
    public void recordStreamRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            long firstChunkLatencyMs,
            int statusCode,
            String errorMessage,
            int streamChunks) {

        Integer promptTokens = null;
        Integer completionTokens = null;
        Integer totalTokens = null;
        if (usageData != null) {
            promptTokens = (int) usageData.getInputTokens();
            completionTokens = (int) usageData.getOutputTokens();
            totalTokens = (int) usageData.getTotalTokens();
        }
        Long quotaConsumed = costResult != null ? costResult.getQuota() : null;
        String truncatedError = truncateLog(errorMessage, 2000);
        String status = statusCode < 400 ? "success" : "error";

        proxyLogsRepository.updateStreamCompletion(
                requestId,
                statusCode,
                status,
                (int) latency,
                (int) firstChunkLatencyMs,
                LocalDateTime.now(),
                streamChunks,
                promptTokens,
                completionTokens,
                totalTokens,
                quotaConsumed,
                costResult != null ? costResult.getBillingDetail() : null,
                "{\"Content-Type\":\"text/event-stream\"}",
                truncatedError)
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(updated -> log.debug("流式请求日志更新完成: requestId={}, statusCode={}", requestId, statusCode))
            .doOnError(err -> log.error("流式请求日志更新失败: requestId={}", requestId, err))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId))
            .subscribe(v -> {}, err -> {});
    }

    /**
     * 流式响应体回填（异步、只更新 response_body / response_size_bytes 两列）
     *
     * 由 RequestLoggingWebFilter 在响应写完后调用。真实 SSE 字节在 WebFilter 层捕获
     * （writeAndFlushWith），这里用部分 UPDATE 覆盖 recordStreamRequestLogComplete 写入的
     * 占位摘要 {"streamChunks":N}，避免与结算回填并发整行覆盖。
     */
    public void recordStreamResponseBody(String requestId, String responseBody, int responseSizeBytes) {
        proxyLogsRepository.updateStreamResponseBody(requestId, responseBody, responseSizeBytes)
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(updated -> log.debug("流式响应体回填完成: requestId={}, size={}bytes, affected={}",
                        requestId, responseSizeBytes, updated))
            .doOnError(err -> log.error("流式响应体回填失败: requestId={}", requestId, err))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId))
            .subscribe(v -> {}, err -> {});
    }

    /**
     * 持久化请求日志（INSERT，异步）
     */
    private void recordRequest(ProxyLogsEntity proxyLogs) {
        LogBox.logSettlement(proxyLogs.getRequestId(), proxyLogs.getUserId(), proxyLogs);

        Mono.fromCallable(() -> {
            proxyLogsRepository.save(proxyLogs).block();
            return null;
        }).subscribeOn(Schedulers.boundedElastic())
            .doOnError(e -> log.error("Failed to save request log", e))
            .contextWrite(c -> c.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, proxyLogs.getRequestId()))
            .subscribe();
    }

    // ==================== 内部工具方法 ====================

    /**
     * 序列化请求头为 JSON，敏感头（Authorization / api-key）脱敏
     */
    private String buildRequestHeadersJson(ServerWebExchange exchange) {
        if (exchange == null) {
            return null;
        }
        try {
            Map<String, String> headers = new LinkedHashMap<>();
            exchange.getRequest().getHeaders().forEach((key, values) -> {
                String lower = key.toLowerCase();
                if (lower.equals("authorization") || lower.equals("x-api-key") || lower.equals("api-key")) {
                    headers.put(key, "***");
                } else {
                    headers.put(key, String.join(", ", values));
                }
            });
            return objectMapper.writeValueAsString(headers);
        } catch (Exception e) {
            log.warn("序列化请求头失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 构造请求日志用的请求体 JSON（优先原始上报数据）。
     * 非 multipart 请求由 RequestLoggingWebFilter 预读时把原始 body 缓存在 exchange 属性，
     * 这里直接返回它（存客户端原始上报数据，不含 requestType/channelId 等内部业务字段）。
     * multipart（编辑）无法预读 JSON，回退到 {@link #buildRequestLogBody(LlmChatRequest, RoutingResult)}。
     */
    private String buildRequestLogBody(LlmChatRequest request, RoutingResult routing, ServerWebExchange exchange) {
        if (exchange != null) {
            Object raw = exchange.getAttribute(RequestLoggingWebFilter.REQUEST_BODY_ATTR);
            if (raw instanceof String s && !s.isBlank()) {
                return s;
            }
        }
        return buildRequestLogBody(request, routing);
    }

    /**
     * 构造请求日志用的请求体 JSON。
     * 关键点：此时 LlmChatRequest.model 已被替换为上游模型名（去渠道前缀），
     * 但 proxy_logs 需要记录客户端原始模型ID，因此复制一份请求并覆盖 model 字段。
     */
    private String buildRequestLogBody(LlmChatRequest request, RoutingResult routing) {
        if (request == null) {
            return null;
        }
        LlmChatRequest logRequest = LlmChatRequest.builder()
            .model(routing.getModelId())
            .messages(request.getMessages())
            .temperature(request.getTemperature())
            .maxTokens(request.getMaxTokens())
            .stream(request.getStream())
            .requestType(request.getRequestType())
            .imageParams(sanitizeImageParamsForLog(request.getImageParams()))
            .videoParams(sanitizeVideoParamsForLog(request.getVideoParams()))
            .originalModelId(request.getOriginalModelId())
            .channelId(request.getChannelId())
            .channelCode(request.getChannelCode())
            .build();
        return buildJson(logRequest);
    }

    /**
     * 通用 JSON 序列化（用于请求体/响应体），失败返回 null
     */
    private String buildJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("序列化对象失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 请求日志用：图像参数脱敏，把输入图/蒙版的 base64 替换为占位标记，
     * 避免 proxy_logs.request_body（TEXT 65535）被 base64 撑爆
     */
    private LlmImageParams sanitizeImageParamsForLog(LlmImageParams params) {
        if (params == null) {
            return null;
        }
        List<LlmImageInput> sanitizedImages = null;
        if (params.getImages() != null) {
            sanitizedImages = params.getImages().stream()
                .map(in -> LlmImageInput.builder()
                    .url(in.getUrl())
                    .base64Data(omitBase64(in.getBase64Data()))
                    .mimeType(in.getMimeType())
                    .build())
                .collect(Collectors.toList());
        }
        LlmImageInput sanitizedMask = null;
        if (params.getMask() != null) {
            sanitizedMask = LlmImageInput.builder()
                .url(params.getMask().getUrl())
                .base64Data(omitBase64(params.getMask().getBase64Data()))
                .mimeType(params.getMask().getMimeType())
                .build();
        }
        return LlmImageParams.builder()
            .prompt(params.getPrompt())
            .n(params.getN())
            .size(params.getSize())
            .quality(params.getQuality())
            .style(params.getStyle())
            .outputFormat(params.getOutputFormat())
            .outputCompression(params.getOutputCompression())
            .background(params.getBackground())
            .moderation(params.getModeration())
            .seed(params.getSeed())
            .user(params.getUser())
            .images(sanitizedImages)
            .mask(sanitizedMask)
            .build();
    }

    /**
     * 请求日志用：视频参数脱敏，把输入图/参考图的 base64 替换为占位标记，
     * 避免 proxy_logs.request_body（TEXT 65535）被 base64 撑爆（与图像脱敏同套路）。
     */
    private LlmVideoParams sanitizeVideoParamsForLog(LlmVideoParams params) {
        if (params == null) {
            return null;
        }
        List<LlmImageInput> sanitizedImages = null;
        if (params.getImages() != null) {
            sanitizedImages = params.getImages().stream()
                .map(in -> LlmImageInput.builder()
                    .url(in.getUrl())
                    .base64Data(omitBase64(in.getBase64Data()))
                    .mimeType(in.getMimeType())
                    .build())
                .collect(Collectors.toList());
        }
        return LlmVideoParams.builder()
            .prompt(params.getPrompt())
            .negativePrompt(params.getNegativePrompt())
            .resolution(params.getResolution())
            .aspectRatio(params.getAspectRatio())
            .duration(params.getDuration())
            .generateAudio(params.getGenerateAudio())
            .watermark(params.getWatermark())
            .promptExtend(params.getPromptExtend())
            .mode(params.getMode())
            .seed(params.getSeed())
            .user(params.getUser())
            .images(sanitizedImages)
            .referenceVideos(params.getReferenceVideos())
            .referenceAudios(params.getReferenceAudios())
            .audioUrl(params.getAudioUrl())
            .media(params.getMedia())
            .extraParams(params.getExtraParams())
            .build();
    }

    /**
     * 响应日志用：图像 base64 脱敏（保留 url/contentType/index），
     * 避免 proxy_logs.response_body（TEXT 65535）被大 base64 撑爆
     */
    private LlmChatResponse sanitizeResponseForLog(LlmChatResponse resp) {
        if (resp.getImages() == null || resp.getImages().isEmpty()) {
            return resp;
        }
        List<LlmImage> logImages = resp.getImages().stream()
            .map(img -> LlmImage.builder()
                .url(img.getUrl())
                .b64Json(omitBase64(img.getB64Json()))
                .revisedPrompt(img.getRevisedPrompt())
                .contentType(img.getContentType())
                .index(img.getIndex())
                .build())
            .collect(Collectors.toList());
        // originalModelId/channelId/responseModelId 已加 @JsonIgnore（内部路由上下文），无需拷入日志副本
        LlmChatResponse copy = new LlmChatResponse();
        copy.setId(resp.getId());
        copy.setModel(resp.getModel());
        copy.setChoices(resp.getChoices());
        copy.setUsage(resp.getUsage());
        copy.setImages(logImages);
        return copy;
    }

    private String omitBase64(String base64) {
        return base64 != null ? "[base64 omitted]" : null;
    }

    /**
     * 截断长文本，避免大响应体撑爆数据库字段
     */
    private String truncateLog(String content, int maxLen) {
        if (content == null) {
            return null;
        }
        return content.length() > maxLen ? content.substring(0, maxLen) + "...[truncated]" : content;
    }

    /**
     * 解析客户端真实 IP。
     *
     * 优先级：X-Forwarded-For（取第一个，原始客户端）→ X-Real-IP → 直连对端地址。
     *
     * 背景：Spring WebFlux 的 {@code exchange.getRequest().getRemoteAddress()} 返回的是
     * TCP 直连对端 IP。当网关前面有 nginx / CDN / 负载均衡器时，直连对端就是代理本身
     * （通常 127.0.0.1 或内网 IP），不是真实客户端。必须优先读代理头。
     *
     * X-Forwarded-For 格式：{@code client, proxy1, proxy2}，最左端是原始客户端。
     * 注意：该头可被客户端伪造，如有安全需求应在 nginx 层覆盖/清洗该头后再透传。
     */
    private String resolveClientIp(ServerWebExchange exchange) {
        if (exchange == null) {
            return null;
        }

        // 1. X-Forwarded-For：取第一个 IP（原始客户端）
        String xff = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            String firstIp = xff.split(",")[0].trim();
            if (!firstIp.isEmpty()) {
                return firstIp;
            }
        }

        // 2. X-Real-IP（nginx 常用）
        String xri = exchange.getRequest().getHeaders().getFirst("X-Real-IP");
        if (xri != null && !xri.isBlank()) {
            return xri.trim();
        }

        // 3. 兜底：TCP 直连对端（无代理 / 直连场景）
        if (exchange.getRequest().getRemoteAddress() != null) {
            return exchange.getRequest().getRemoteAddress().getAddress().getHostAddress();
        }

        return null;
    }
}
