package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.dto.BillingResult;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.RoutingResult;
import com.llmate.multiprotocol.dto.UsageData;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.entity.ProxyLogsEntity;
import com.llmate.multiprotocol.entity.ProxyTokensEntity;
import com.llmate.multiprotocol.repository.ProxyLogsRepository;
import com.llmate.multiprotocol.util.LogBox;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.LocalDateTime;

/**
 * 结算/审计日志服务
 *
 * 职责：集中处理合并后的 proxy_logs（结算 + 全量请求/响应审计，原 proxy_request_logs 已并入）
 * 的构建、持久化与回填。LlmGateway 只负责调度，不掺入日志细节。
 *
 * 持久化均为异步（boundedElastic），不阻塞响应主链路。
 *
 * 单表写入纪律（照 proxy_request_logs 时代教训，避免并发整行覆盖）：
 * - recordRequestLogStart 建行（INSERT，审计元数据 + status='processing'）；
 * - 结算回填用部分 UPDATE（updateSettlementByRequestId，列集 = 结算字段）；
 * - 完成/响应回填用部分 UPDATE（updateCompletionByRequestId / updateStreamCompletion，
 *   列集 = 审计元数据），与结算列集不相交。
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class SettlementService {

    private final ProxyLogsRepository proxyLogsRepository;

    // 注意：请求/应答数据（请求/响应头、正文、大小）已不再落库（DROP COLUMN），
    // 仅完整打印到网关日志（LogBox，logs/gateway.log）。落库的 errorMessage 仍按 2000 字符截断。

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
     * 只落结算/审计元数据（request_method/request_path/is_stream/client_ip/user_agent 等）；
     * 请求/应答数据（头/正文/大小）由 RequestLoggingWebFilter 完整打印到网关日志，不再落库。
     * status 置 'processing'，结算回填后改为 success/error。
     */
    public void recordRequestLogStart(
            String requestId,
            Long userId,
            Long tokenId,
            RoutingResult routing,
            boolean isStream,
            ServerWebExchange exchange) {

        String clientIp = resolveClientIp(exchange);

        String userAgent = exchange != null ? exchange.getRequest().getHeaders().getFirst("User-Agent") : null;

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
     * 与 {@link #recordRequestLogStart} 的区别：requestPath 直接传入
     * （向量请求不是 LlmChatRequest，无法从 exchange 取路由路径），其余字段逻辑一致。
     */
    public void recordEmbeddingRequestLogStart(
            String requestId,
            Long userId,
            Long tokenId,
            RoutingResult routing,
            String requestPath,
            ServerWebExchange exchange) {

        String clientIp = resolveClientIp(exchange);

        String userAgent = exchange != null ? exchange.getRequest().getHeaders().getFirst("User-Agent") : null;

        ProxyLogsEntity logsEntity = ProxyLogsEntity.builder()
            .requestId(requestId)
            .userId(userId)
            .tokenId(tokenId)
            .channelId(routing.getChannelId())
            .model(routing.getModelId())
            .requestMethod("POST")
            .requestPath(requestPath)
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
     * 异步回填：response_status / tokens / quota_consumed / latency_ms / completed_at /
     * error_msg，并兜底结算 status。
     */
    public void recordEmbeddingRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            int statusCode,
            String errorMessage) {

        String truncatedError = truncateLog(errorMessage, 2000);
        String status = statusCode < 400 ? "success" : "error";

        proxyLogsRepository.updateCompletionByRequestId(
                requestId,
                statusCode,
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
     * 状态码按是否有错误信息取 200/500。
     */
    public void recordVideoRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            String errorMessage) {

        recordEmbeddingRequestLogComplete(
            requestId, usageData, costResult, latency,
            errorMessage != null ? 500 : 200, errorMessage);
    }

    /**
     * 记录 ASR 转写接口请求日志（完成）—— UsageData 版，薄封装 {@link #recordEmbeddingRequestLogComplete}。
     * 状态码由调用方显式传入（成功 200 / 失败 500）。
     */
    public void recordAudioRequestLogComplete(
            String requestId,
            UsageData usageData,
            BillingResult costResult,
            long latency,
            int statusCode,
            String errorMessage) {

        recordEmbeddingRequestLogComplete(
            requestId, usageData, costResult, latency,
            statusCode, errorMessage);
    }

    /**
     * 记录非流式请求日志（完成）—— 异步回填响应相关字段。
     * 补全：response_status / tokens / quota_consumed / latency_ms / completed_at / error_msg，
     * 并兜底结算 status。响应体/头已不落库（仅打印到网关日志）。
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

        if (resp != null && resp.getUsage() != null) {
            promptTokens = resp.getUsage().getPromptTokens();
            completionTokens = resp.getUsage().getCompletionTokens();
            totalTokens = resp.getUsage().getTotalTokens();
        }

        proxyLogsRepository.updateCompletionByRequestId(
                requestId,
                statusCode,
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
     * 只更新结算/元数据列（status/latency/stream_chunks/first_chunk_latency_ms/tokens/cost/
     * billing_detail）。真实 SSE 响应体/头已不落库（RequestLoggingWebFilter 完整打印到网关日志）。
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
                truncatedError)
            .subscribeOn(Schedulers.boundedElastic())
            .doOnNext(updated -> log.debug("流式请求日志更新完成: requestId={}, statusCode={}", requestId, statusCode))
            .doOnError(err -> log.error("流式请求日志更新失败: requestId={}", requestId, err))
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
     * 截断长文本，避免超长错误信息撑爆数据库字段
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
