package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.dto.openai.OpenAiModelsResponse;
import com.llmate.multiprotocol.entity.ProxyChannelModelsEntity;
import com.llmate.multiprotocol.entity.ProxyChannelsEntity;
import com.llmate.multiprotocol.repository.ProxyChannelModelsRepository;
import com.llmate.multiprotocol.repository.ProxyChannelsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.ZoneOffset;
import java.util.List;

/**
 * 模型列表服务
 *
 * 参考老项目（ModelsServiceImpl.list / proxyChannelModelsMapper.list）：
 * 模型列表不是 model_library 目录表，而是"启用的渠道(proxy_channels.status=1) ×
 * 启用的渠道-模型关联(proxy_channel_models.is_enabled=1)"，id 拼成 {channel_code}/{model_id}，
 * 与网关实际可调用的模型前缀一致，保证 /v1/models 列出的每个 id 都能直接用于 /v1/chat/completions。
 *
 * MVC 分层：Controller 只处理 HTTP，业务逻辑全部在本服务实现。
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class ModelsService {

    private final ProxyChannelsRepository proxyChannelsRepository;
    private final ProxyChannelModelsRepository proxyChannelModelsRepository;

    /**
     * 列出全部可用模型（OpenAI 兼容格式）
     */
    public Mono<OpenAiModelsResponse> list() {
        return proxyChannelsRepository.findAllByStatus(SystemConstants.STATUS_ENABLED)
            .flatMap(channel ->
                proxyChannelModelsRepository.findByChannelIdAndIsEnabled(channel.getId(), SystemConstants.STATUS_ENABLED)
                    .map(model -> toModelData(channel, model)))
            .collectList()
            .map(this::buildResponse)
            .doOnSuccess(response ->
                log.info("[ModelsService] 返回 {} 个模型", response.getData().size()));
    }

    /**
     * 构建响应
     */
    private OpenAiModelsResponse buildResponse(List<OpenAiModelsResponse.ModelData> modelList) {
        return OpenAiModelsResponse.builder()
            .object("list")
            .data(modelList)
            .build();
    }

    /**
     * 转换为 OpenAI 格式的模型数据
     * id = {channel_code}/{model_id}（老项目同款拼接）；owned_by 固定 silievo（老项目同款）
     */
    private OpenAiModelsResponse.ModelData toModelData(ProxyChannelsEntity channel, ProxyChannelModelsEntity model) {
        return OpenAiModelsResponse.ModelData.builder()
            .id(channel.getChannelCode() + "/" + model.getModelId())
            .object("model")
            .created(model.getCreatedAt() != null
                ? model.getCreatedAt().atZone(ZoneOffset.UTC).toInstant().toEpochMilli()
                : System.currentTimeMillis())
            .ownedBy("silievo")
            .build();
    }
}
