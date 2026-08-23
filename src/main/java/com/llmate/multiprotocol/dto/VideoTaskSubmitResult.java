package com.llmate.multiprotocol.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 视频任务提交结果（上游异步接口创建成功返回）
 * taskId 为上游任务ID（阿里云 task_id / Seedance cgt-xxx），提交成功后本地落库为 PENDING。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VideoTaskSubmitResult {

    /** 上游任务ID */
    private String taskId;

    /** 提交时上游任务状态（成功恒为 "PENDING"） */
    private String status;
}
