package com.llmate.multiprotocol.controller;

import com.llmate.multiprotocol.annotation.RequireApiKey;
import com.llmate.multiprotocol.dto.openai.OpenAiModelsResponse;
import com.llmate.multiprotocol.service.ModelsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

/**
 * 模型列表控制器（MVC 分层：仅 HTTP 层，业务逻辑在 {@link ModelsService}）
 * GET /v1/models — 返回可用模型列表（OpenAI 兼容格式）
 */
@RestController
@RequestMapping("/v1")
@RequiredArgsConstructor
@Log4j2
@RequireApiKey
public class ModelController {

    private final ModelsService modelsService;

    @GetMapping("/models")
    public Mono<OpenAiModelsResponse> listModels() {
        log.info("[ModelController] 模型列表请求");
        return modelsService.list();
    }
}
