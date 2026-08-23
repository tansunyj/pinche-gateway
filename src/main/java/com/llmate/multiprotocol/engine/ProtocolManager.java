package com.llmate.multiprotocol.engine;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.converter.ProtocolConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 协议管理器
 * 负责在反应式异步上下文中存取和路由对应的协议转换器(Converter)
 */
@Component
@SuppressWarnings("rawtypes")
public class ProtocolManager {

    private final Map<ProtocolType, ProtocolConverter> converters;

    /**
     * 依赖注入：自动收集 Spring 容器中所有实现了 ProtocolConverter 接口的 Bean
     */
    public ProtocolManager(List<ProtocolConverter> converterList) {
        this.converters = converterList.stream()
                .collect(Collectors.toMap(ProtocolConverter::getProtocolType, Function.identity()));
    }

    /**
     * 在入口处绑定当前连接的协议状态到 WebFlux 请求上下文中
     * @param exchange 反应式服务器网关交换机
     * @param type 触发的协议枚举类型
     */
    public void bindProtocol(ServerWebExchange exchange, ProtocolType type) {
        exchange.getAttributes().put(SystemConstants.CONTEXT_PROTOCOL_KEY, type);
    }

    /**
     * 动态提取当前请求对应的协议转换处理器 (Converter)
     * @param exchange 反应式服务器网关交换机
     * @return 对应的协议转换器实现，默认使用内部标准协议兜底
     */
    public ProtocolConverter getConverter(ServerWebExchange exchange) {
        ProtocolType type = (ProtocolType) exchange.getAttributes().get(SystemConstants.CONTEXT_PROTOCOL_KEY);
        if (type == null) {
            type = ProtocolType.INTERNAL; // 默认使用 LLMate 原生内部协议
        }
        return converters.get(type);
    }
}
