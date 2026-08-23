package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.dto.openai.OpenAiVideoRequest;
import com.llmate.multiprotocol.dto.openai.OpenAiVideoResponse;
import com.llmate.multiprotocol.exception.LlmErrorCode;
import com.llmate.multiprotocol.exception.LlmGatewayException;
import lombok.extern.log4j.Log4j2;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 视频生成接口控制器（异步生视频功能已下线）
 *
 * 数据库重构后 video_generation_tasks 等任务表已移除，异步生视频能力不再提供。
 * POST /v1/videos/generations 与 GET /v1/videos/generations/{taskId} 统一返回
 * 「生视频功能未启用」（HTTP 400），保留路径以提示客户端该能力已下线。
 */
//@RestController
@RequestMapping("/v1")
@RequireApiKey
@Log4j2
public class VideoController {

    /**
     * 创建视频生成任务：POST /v1/videos/generations（功能未启用）
     */
    @PostMapping(value = "/videos/generations", consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<OpenAiVideoResponse>> generations(@RequestBody OpenAiVideoRequest request,
                                                                 ServerWebExchange exchange) {
        log.warn("[VideoController] 异步生视频功能未启用: model={}", request.getModel());
        return Mono.error(new LlmGatewayException(LlmErrorCode.FEATURE_NOT_ENABLED, "生视频"));
    }

    /**
     * 查询视频任务状态：GET /v1/videos/generations/{taskId}（功能未启用）
     */
    @GetMapping(value = "/videos/generations/{taskId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<OpenAiVideoResponse>> query(@PathVariable String taskId, ServerWebExchange exchange) {
        log.warn("[VideoController] 异步生视频功能未启用: taskId={}", taskId);
        return Mono.error(new LlmGatewayException(LlmErrorCode.FEATURE_NOT_ENABLED, "生视频"));
    }
}
