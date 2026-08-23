package com.llmate.multiprotocol.filter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.util.LogBox;
import com.llmate.multiprotocol.util.UserContext;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.ThreadContext;
import org.reactivestreams.Publisher;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 增强请求日志 WebFilter
 * 记录所有请求的入口日志和响应日志，使用方框样式便于查找
 *
 * 修复：确保请求体/响应体能正确传递给 LogBox
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 100) // 在认证过滤器之后执行，确保请求入口/响应日志能读到 UserId
@Log4j2
public class RequestLoggingWebFilter implements WebFilter {

    private final ObjectMapper objectMapper;

    public RequestLoggingWebFilter(ObjectMapper objectMapper) {
        // 注入 Spring 单例 ObjectMapper，不各自 new 一份
        this.objectMapper = objectMapper;
    }

    // 注意：不再设置任何响应体捕获/显示上限 —— HTTP 请求、HTTP 应答、SSE 响应体必须
    // 完整、详细地打印到网关日志（logs/gateway.log）。DB 已不再存储请求/应答数据。
    // 捕获的字节在响应写完前驻留内存，体量等于响应本身，这是完整记录的必要代价。

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        // 请求ID由最先执行的 RequestIdWebFilter 生成并写入 exchange 属性 / Reactor Context，
        // 这里直接复用，保证入口日志、响应日志与全链路日志用的是同一个 requestId
        String requestId = UserContext.getRequestId(exchange);

        ServerHttpRequest request = exchange.getRequest();
        String method = request.getMethod().name();
        String path = request.getPath().value();
        long startTime = System.currentTimeMillis();

        // multipart/form-data（文件上传）：不能预读请求体！
        // @RequestPart 的 multipart 解析走 exchange.getMultipartData()，该 Mono 绑定的是【原始】请求体流
        // （exchange.mutate().request(装饰).build() 返回的 MutativeDecorator 只替换 getRequest()，
        //  getMultipartData() 仍委托给原始 exchange）。一旦在这里用 DataBufferUtils.join 消费掉原始 body，
        // 解析器拿到空流并立即 complete，触发 MultipartParser "Could not find first boundary"。
        // 因此 multipart 请求不读 body：只按 Content-Length 打占位入口日志，请求体原样透传，
        // 仅装饰响应用于捕获响应体。
        boolean isMultipart = request.getHeaders().getContentType() != null
                && request.getHeaders().getContentType().isCompatibleWith(MediaType.MULTIPART_FORM_DATA);

        // 包装响应以捕获响应体（普通 writeWith + 流式 writeAndFlushWith 都要捕获；multipart 也需要）
        ByteArrayOutputStream responseBodyStream = new ByteArrayOutputStream();
        AtomicLong totalResponseBytes = new AtomicLong(0);
        ServerHttpResponse decoratedResponse = new ServerHttpResponseDecorator(exchange.getResponse()) {
            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {
                return super.writeWith(Flux.from(body)
                    .map(buffer -> wrapCapturedBuffer(getDelegate(), responseBodyStream, totalResponseBytes, buffer)));
            }

            /**
             * 流式(SSE)响应每个事件单独 flush，走的是 writeAndFlushWith 而不是 writeWith。
             * 只重写 writeWith 会一个字节都捕获不到（历史上 proxy_request_logs 流式 response_body
             * 一直是空/占位符）。必须一并重写，才能拿到真实的 SSE 响应体。
             */
            @Override
            public Mono<Void> writeAndFlushWith(Publisher<? extends Publisher<? extends DataBuffer>> body) {
                return super.writeAndFlushWith(Flux.from(body)
                    .map(innerPublisher -> Flux.from(innerPublisher)
                        .map(buffer -> wrapCapturedBuffer(getDelegate(), responseBodyStream, totalResponseBytes, buffer))));
            }
        };

        if (isMultipart) {
            // 不消费请求体；入口日志用 Content-Length 占位（chunked 无该头则标 unknown）
            Long userId = UserContext.getUserId(exchange);
            long contentLength = request.getHeaders().getContentLength();
            String entryBody = contentLength >= 0
                    ? "(multipart/form-data, " + contentLength + " bytes)"
                    : "(multipart/form-data, chunked)";
            Integer requestSizeBytes = contentLength >= 0 ? (int) contentLength : null;
            LogBox.logRequestEntry(method, path, requestId, userId,
                    maskedRequestHeaders(request), requestSizeBytes, entryBody);

            ServerWebExchange mutatedExchange = exchange.mutate().response(decoratedResponse).build();
            // 链只执行一次。requestId 已由 RequestIdWebFilter 写入 Reactor Context（最外层算子），
            // 本条请求所有下游日志（控制器/服务/Adapter/计费）自动带 requestId，无需手动传参。
            return chain.filter(mutatedExchange)
                .doFinally(signalType -> logResponse(exchange, requestId, startTime, responseBodyStream, totalResponseBytes));
        }

        // 非 multipart：读取请求体并缓存、装饰请求重发，保证下游可重复读取（@RequestBody 等）。
        // 关键修复：使用 defaultIfEmpty 在单一路径内处理空请求体，绝不再用 switchIfEmpty 二次执行 chain.filter。
        // switchIfEmpty 的旧写法是严重 bug：chain.filter() 返回 Mono<Void>，其"空完成"会让 switchIfEmpty
        // 总是触发，把整条 WebFilter 链（含控制器、路由、上游调用、计费）重复执行一遍，
        // 导致 Postman 只发一次请求、网关却处理两次。
        return DataBufferUtils.join(request.getBody())
            .defaultIfEmpty(exchange.getResponse().bufferFactory().wrap(new byte[0]))
            .flatMap(dataBuffer -> {
                byte[] bytes = new byte[dataBuffer.readableByteCount()];
                dataBuffer.read(bytes);
                DataBufferUtils.release(dataBuffer);

                String requestBody = new String(bytes, StandardCharsets.UTF_8);

                // 输出带方框的请求入口日志（空请求体也在此统一处理）。
                // 说明：请求入口日志发生在认证之后（见过滤器顺序），此时 UserContext 已带上 userId。
                Long userId = UserContext.getUserId(exchange);
                String entryBody = requestBody.isEmpty() ? "(no body)" : prettyJson(requestBody);
                LogBox.logRequestEntry(method, path, requestId, userId,
                        maskedRequestHeaders(request), requestBody.getBytes(StandardCharsets.UTF_8).length, entryBody);

                // 包装请求，使后续 filter 可以重新读取 body
                ServerHttpRequest decoratedRequest = new ServerHttpRequestDecorator(request) {
                    @Override
                    public Flux<DataBuffer> getBody() {
                        return Flux.just(exchange.getResponse().bufferFactory().wrap(bytes));
                    }
                };

                ServerWebExchange mutatedExchange = exchange.mutate()
                    .request(decoratedRequest)
                    .response(decoratedResponse)
                    .build();

                // 链只执行一次。requestId 已由 RequestIdWebFilter 写入 Reactor Context
                return chain.filter(mutatedExchange)
                    .doFinally(signalType -> logResponse(exchange, requestId, startTime, responseBodyStream, totalResponseBytes));
            });
    }

    /**
     * 响应完成后的统一日志（两个分支共用）：非流式 ==== 框 + 完整 JSON 美化体；
     * 流式(SSE)原文输出（不加框）。requestId 用网关生成的 id。
     * DB 已不再存储请求/应答数据，响应头/大小/正文全部完整打印到网关日志。
     */
    private void logResponse(ServerWebExchange exchange, String requestId, long startTime,
                             ByteArrayOutputStream responseBodyStream, AtomicLong totalResponseBytes) {
        // doFinally 在终态信号处理时才回调，此刻 Reactor 自动上下文传播可能已经把 MDC 的 requestId
        // 还原清空（终态路径不保证算子级传播，日志实证 7988 行曾 [reqId=] 恒空）。这里显式写入，
        // 保证响应框日志与全链路一致带 reqId；try/finally 确保不泄漏到线程池下一条请求。
        ThreadContext.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, requestId);
        try {
            long duration = System.currentTimeMillis() - startTime;
            String responseBody = responseBodyStream.toString(StandardCharsets.UTF_8);

            boolean isStream = isEventStreamResponse(exchange.getResponse());
            Long responseUserId = UserContext.getUserId(exchange);
            LogBox.logRequestResponse(requestId, responseUserId, duration,
                responseHeadersToMap(exchange.getResponse()),
                (int) totalResponseBytes.get(),
                responseBody.isEmpty()
                    ? "(empty body/stream response)"
                    : (isStream ? responseBody : prettyJson(responseBody)),
                isStream);
        } finally {
            ThreadContext.remove(SystemConstants.CONTEXT_REQUEST_ID_KEY);
        }
    }

    /**
     * 拷贝单个 DataBuffer：累计真实字节数 + 截断捕获前 8KB，返回同字节的新 buffer 继续写入
     */
    private DataBuffer wrapCapturedBuffer(
            ServerHttpResponse response,
            ByteArrayOutputStream stream,
            AtomicLong totalBytes,
            DataBuffer buffer) {
        byte[] buf = new byte[buffer.readableByteCount()];
        buffer.read(buf);
        totalBytes.addAndGet(buf.length);
        captureBodySafely(stream, buf);
        DataBufferUtils.release(buffer);
        return response.bufferFactory().wrap(buf);
    }

    /**
     * 判断响应是否为 SSE 流（text/event-stream），决定是否回填流式响应体
     */
    private boolean isEventStreamResponse(ServerHttpResponse response) {
        MediaType contentType = response.getHeaders().getContentType();
        return contentType != null && contentType.isCompatibleWith(MediaType.TEXT_EVENT_STREAM);
    }

    /**
     * 将响应体字节完整写入捕获流（不截断）。日志要完整记录 HTTP 应答，
     * 响应体字节在响应写完前驻留内存（等于响应本身大小），这是完整记录的必要代价。
     */
    private void captureBodySafely(ByteArrayOutputStream stream, byte[] buf) {
        stream.write(buf, 0, buf.length);
    }

    /**
     * 请求头脱敏转 Map，供入口日志打印。authorization / x-api-key / api-key 值一律置 "***"，
     * 其余头原样输出（key 统一小写）。空头返回空 Map。
     */
    private Map<String, String> maskedRequestHeaders(ServerHttpRequest request) {
        Map<String, String> result = new LinkedHashMap<>();
        request.getHeaders().forEach((name, values) -> {
            String lower = name.toLowerCase();
            String value = lower.equals("authorization")
                    || lower.equals("x-api-key")
                    || lower.equals("api-key")
                    ? "***" : String.join(", ", values);
            result.put(name, value);
        });
        return result;
    }

    /**
     * 响应头转 Map，供响应日志打印。key 统一小写，多值用 ", " 拼接。
     */
    private Map<String, String> responseHeadersToMap(ServerHttpResponse response) {
        Map<String, String> result = new LinkedHashMap<>();
        response.getHeaders().forEach((name, values) -> result.put(name, String.join(", ", values)));
        return result;
    }

    /**
     * 美化 JSON 输出
     */
    private String prettyJson(String json) {
        try {
            Object jsonObj = objectMapper.readValue(json, Object.class);
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObj);
        } catch (Exception e) {
            return json;
        }
    }
}
