package com.llmate.multiprotocol.dto;

/**
 * 网关内部标准请求类型
 * 区分文本聊天与多模态能力，LlmGateway 据此派发到 ProviderAdapter 对应方法。
 * 视频生成/编辑、语音转写等类型后续迭代追加。
 */
public enum LlmRequestType {
    /** 文本聊天补全（默认） */
    CHAT_COMPLETION,

    /** 图像生成 */
    IMAGE_GENERATION,

    /** 图像编辑 */
    IMAGE_EDIT,

    /** 视频生成（异步任务：提交返回 task_id，轮询状态结算） */
    VIDEO_GENERATION
}
