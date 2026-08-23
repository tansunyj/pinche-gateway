package com.llmate.multiprotocol.engine.provider;

import com.llmate.multiprotocol.constant.ProtocolType;
import com.llmate.multiprotocol.dto.LlmChatRequest;
import com.llmate.multiprotocol.dto.LlmChatResponse;
import com.llmate.multiprotocol.dto.LlmStreamChunk;
import com.llmate.multiprotocol.dto.ModelEndpointConfig;
import com.llmate.multiprotocol.dto.VideoTaskStatus;
import com.llmate.multiprotocol.dto.VideoTaskSubmitResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Provider适配器接口 - 统一所有底层LLM渠道的调用方式
 *
 * 每个Provider实现负责：
 * 1. 将内部标准请求转换为目标渠道格式
 * 2. 调用目标渠道API
 * 3. 将响应转换为内部标准格式
 *
 * 模型路由规则：
 * - 模型名称格式为 "providerAlias/modelName"
 * - 例如："aliyun/deepseek-v4-flash" 路由到阿里云百炼
 * - 例如："deepseek/deepseek-chat" 路由到 DeepSeek 官网
 */
public interface ProviderAdapter {

    /**
     * 获取Provider别名（用于模型名称前缀匹配）
     * 例如："aliyun", "deepseek", "anthropic"
     */
    String getProviderAlias();

    /**
     * 获取该Provider与上游API通信时使用的原生协议类型
     * 用于选择合适的上游格式转换器
     * 大多数Provider兼容OpenAI格式，默认返回 OPENAI_CHAT_COMPLETIONS
     * Anthropic等使用自有协议的Provider需要覆盖此方法
     */
    default ProtocolType getNativeProtocol() {
        return ProtocolType.OPENAI_CHAT_COMPLETIONS;
    }

    /**
     * 获取当前请求选中的 Token ID（用于 reportTokenUsage）
     * 方案 C：多 Token 负载均衡下，每次请求动态选择 Token
     * @return 当前 Token ID，如未选择或单 Token 模式返回 null
     */
    default Long getCurrentTokenId() {
        return null;
    }

    /**
     * 判断该Provider是否支持指定的模型路径
     * @param modelPath 格式如 "aliyun/deepseek-v4-flash" 或 "deepseek/deepseek-chat"
     * @return true if this provider can handle the model
     */
    default boolean supports(String modelPath) {
        if (modelPath == null || !modelPath.contains("/")) {
            return false;
        }
        String alias = modelPath.substring(0, modelPath.indexOf("/"));
        return getProviderAlias().equals(alias);
    }

    /**
     * 获取Provider名称标识（用于日志和错误信息）
     */
    String getProviderName();

    /**
     * 从模型路径中提取实际模型名（去掉provider前缀）
     */
    default String extractModelName(String modelPath) {
        if (modelPath == null || !modelPath.contains("/")) {
            return modelPath;
        }
        return modelPath.substring(modelPath.indexOf("/") + 1);
    }

    /**
     * 非流式调用
     * @param request 内部标准请求
     * @return 内部标准响应
     */
    Mono<LlmChatResponse> chat(LlmChatRequest request);

    /**
     * 非流式调用（带自定义端点配置）
     * @param request 内部标准请求
     * @param endpointConfig 端点配置（可为null，使用默认配置）
     * @return 内部标准响应
     */
    default Mono<LlmChatResponse> chat(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        // 默认实现忽略自定义端点，使用固定配置
        return chat(request);
    }

    /**
     * 流式调用
     * @param request 内部标准请求
     * @return 内部标准流式块
     */
    Flux<LlmStreamChunk> chatStream(LlmChatRequest request);

    /**
     * 流式调用（带自定义端点配置）
     * @param request 内部标准请求
     * @param endpointConfig 端点配置（可为null，使用默认配置）
     * @return 内部标准流式块
     */
    default Flux<LlmStreamChunk> chatStream(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        // 默认实现忽略自定义端点，使用固定配置
        return chatStream(request);
    }

    // ==================== 多模态能力（图像生成/编辑，默认不支持） ====================

    /**
     * 图像生成
     * @param request 内部标准请求（requestType=IMAGE_GENERATION，携带 imageParams）
     * @return 内部标准响应（images 字段承载结果）
     */
    default Mono<LlmChatResponse> generateImage(LlmChatRequest request) {
        return generateImage(request, null);
    }

    /**
     * 图像生成（带自定义端点配置）
     */
    default Mono<LlmChatResponse> generateImage(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        throw new UnsupportedOperationException("Image generation not supported by provider: " + getProviderName());
    }

    /**
     * 图像编辑
     * @param request 内部标准请求（requestType=IMAGE_EDIT，imageParams.images 携带输入图）
     * @return 内部标准响应
     */
    default Mono<LlmChatResponse> editImage(LlmChatRequest request) {
        return editImage(request, null);
    }

    /**
     * 图像编辑（带自定义端点配置）
     */
    default Mono<LlmChatResponse> editImage(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        throw new UnsupportedOperationException("Image edit not supported by provider: " + getProviderName());
    }

    // ==================== 多模态能力（视频生成，默认不支持） ====================

    /**
     * 视频生成任务提交（异步）
     * @param request 内部标准请求（requestType=VIDEO_GENERATION，携带 videoParams）
     * @return 任务提交结果（taskId + PENDING）
     */
    default Mono<VideoTaskSubmitResult> generateVideo(LlmChatRequest request) {
        return generateVideo(request, null);
    }

    /**
     * 视频生成任务提交（带自定义端点配置）
     */
    default Mono<VideoTaskSubmitResult> generateVideo(LlmChatRequest request, ModelEndpointConfig endpointConfig) {
        throw new UnsupportedOperationException("Video generation not supported by provider: " + getProviderName());
    }

    /**
     * 查询视频任务状态（轮询用）
     * @param taskId 上游任务ID
     * @return 归一化任务状态
     */
    default Mono<VideoTaskStatus> queryVideoTask(String taskId) {
        return queryVideoTask(taskId, null);
    }

    /**
     * 查询视频任务状态（带自定义端点配置）
     */
    default Mono<VideoTaskStatus> queryVideoTask(String taskId, ModelEndpointConfig endpointConfig) {
        throw new UnsupportedOperationException("Video task query not supported by provider: " + getProviderName());
    }
}
