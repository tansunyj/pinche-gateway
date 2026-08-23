package com.llmate.multiprotocol.converter;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * 协议转换器契约 - 负责外部协议与网关内部核心模型的双向非阻塞转换
 */
public interface ProtocolConverter<EXT_REQ, EXT_RESP> {

    /** 获取当前转换器支持的协议类型 */
    ProtocolType getProtocolType();

    /** 外部请求 DTO 转换为网关内部标准统一请求模型 (包裹在 Mono 中支持异步 IO) */
    Mono<LlmChatRequest> toInternalRequest(EXT_REQ externalRequest);

    /** 网关内部统一标准响应转换为外部特定协议响应 DTO */
    EXT_RESP toExternalResponse(LlmChatResponse internalResponse);

    /** 将网关内部核心流 Chunk，包装转化为符合客户端 SDK 规范的原生 ServerSentEvent 流 */
    Flux<ServerSentEvent<Object>> toExternalStream(Flux<LlmStreamChunk> internalStream, EXT_REQ originalReq, String maskedModelName);

    /** 从原始请求体中快速提取模型名称 */
    String extractModelName(EXT_REQ externalRequest);
}