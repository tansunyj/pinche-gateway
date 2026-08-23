package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 视频任务状态查询结果（上游轮询接口返回，由各渠道视频适配器解析）
 * 状态统一归一化为：RUNNING / SUCCEEDED / FAILED（超时由本地定时器处理，不入此对象）。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VideoTaskStatus {

    /** 归一化状态：RUNNING / SUCCEEDED / FAILED */
    private String status;

    /** 生成视频 URL（SUCCEEDED） */
    private String videoUrl;

    /** 视频封面 URL（SUCCEEDED，可选） */
    private String coverUrl;

    /** 消耗 token 数（Seedance 按 token 计费用） */
    private Long completionTokens;

    private String errorCode;

    private String errorMessage;
}
