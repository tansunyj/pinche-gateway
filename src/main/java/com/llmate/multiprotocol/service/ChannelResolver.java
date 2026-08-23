package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.constant.SystemConstants;
import com.llmate.multiprotocol.entity.ProxyChannelModelsEntity;
import com.llmate.multiprotocol.repository.ProxyChannelModelsRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 渠道解析器
 * 校验渠道是否支持指定模型，并返回绑定行（含 provider_capability，供路由解析 provider_alias）
 *
 * 说明：模型路由与上游 Token 负载均衡已在 ModelRouter 中处理，
 * 此处仅保留被实际使用的渠道-模型支持校验能力。
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class ChannelResolver {

    private final ProxyChannelModelsRepository proxyChannelModelsRepository;

    /**
     * 验证渠道是否支持指定模型，返回绑定行（含 provider_capability JSON）
     * 无绑定行时返回空 Mono（供上层走 fallback 或报 MODEL_NOT_SUPPORTED）
     */
    public Mono<ProxyChannelModelsEntity> validateModelSupport(Long channelId, String modelId) {
        return proxyChannelModelsRepository
            .findByChannelIdAndModelIdAndIsEnabled(channelId, modelId, SystemConstants.STATUS_ENABLED);
    }
}
