package com.llmate.multiprotocol.config;

import com.llmate.multiprotocol.constant.SystemConstants;
import io.micrometer.context.ContextRegistry;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Hooks;

/**
 * 反应式 MDC（日志上下文）配置
 *
 * 目标：让每个请求的 requestId 自动出现在该请求全生命周期内的每一条日志里，
 * 无需在每个方法里手动传参（配合 log4j2.xml 的 %X{requestId} pattern 实现）。
 *
 * WebFlux 是响应式模型：一条请求会跨多个线程执行（Netty 事件循环线程、调度线程之间切换），
 * Log4j2 ThreadContext 本质是 ThreadLocal，普通写法在算子切换线程后会丢失。
 * 解决方式是「Reactor Context + micrometer context-propagation」两步：
 *
 * 1. Hooks.enableAutomaticContextPropagation()：让 Reactor 在每个算子执行边界自动把 Context
 *    中注册过 accessor 的键应用到线程局部量（执行回调前写入、离开后还原），并随线程调度传播。
 *    Reactor 3.6 起默认不自动开启（无 context-propagation 系统属性），需显式调用，重复调用幂等。
 * 2. 注册 requestId → Log4j2 ThreadContext 的 ThreadLocalAccessor：Context 中 key=requestId 的值
 *    自动写入 ThreadContext，日志 pattern 的 %X{requestId} 即可读取。
 *    （便捷方法生成的 accessor 按「apply 捕获旧值 / close 还原旧值」处理生命周期，不会泄漏到下一请求）
 *
 * requestId 的写入位置：RequestLoggingWebFilter 生成 UUID 后，用 contextWrite 放进 Reactor Context。
 */
@Configuration
public class ReactiveMdcConfiguration {

    static {
        // 显式开启 Reactor 自动上下文传播（幂等：仅置开关 + 注册调度钩子，可安全重复调用）
        Hooks.enableAutomaticContextPropagation();

        // 注册 requestId 存取器：Reactor Context 的 requestId ↔ Log4j2 ThreadContext（MDC）。
        ContextRegistry.getInstance().registerThreadLocalAccessor(
                SystemConstants.CONTEXT_REQUEST_ID_KEY,
                () -> ThreadContext.get(SystemConstants.CONTEXT_REQUEST_ID_KEY),
                v -> {
                    if (v != null) {
                        ThreadContext.put(SystemConstants.CONTEXT_REQUEST_ID_KEY, v);
                    } else {
                        ThreadContext.remove(SystemConstants.CONTEXT_REQUEST_ID_KEY);
                    }
                },
                () -> ThreadContext.remove(SystemConstants.CONTEXT_REQUEST_ID_KEY)
        );
    }
}
