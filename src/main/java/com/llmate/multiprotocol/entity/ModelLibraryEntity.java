package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * 模型库表实体
 * 对应数据库表: model_library
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("model_library")
public class ModelLibraryEntity {

    @Id
    private Long id;

    /**
     * 模型唯一标识
     */
    @Column("model_id")
    private String modelId;

    /**
     * 模型显示名称
     */
    @Column("display_name")
    private String displayName;

    /**
     * 模型描述
     */
    @Column("description")
    private String description;

    /**
     * 模型分类
     */
    @Column("category")
    private String category;

    /**
     * 提供商
     */
    @Column("provider")
    private String provider;

    /**
     * 能力配置（JSON）
     */
    @Column("capabilities")
    private String capabilities;

    /**
     * 上下文窗口大小
     */
    @Column("context_window")
    private Integer contextWindow;

    /**
     * 最大输出token数
     */
    @Column("max_output_tokens")
    private Integer maxOutputTokens;

    /**
     * 训练数据截止日期
     */
    @Column("training_data_cutoff")
    private LocalDate trainingDataCutoff;

    /**
     * 状态: 0=禁用, 1=启用
     */
    @Column("status")
    private Integer status;

    /**
     * 是否可见
     */
    @Column("is_visible")
    private Integer isVisible;

    /**
     * 是否热门
     */
    @Column("is_hot")
    private Integer isHot;

    /**
     * 是否新品
     */
    @Column("is_new")
    private Integer isNew;

    /**
     * 徽章文字
     */
    @Column("badge_text")
    private String badgeText;

    /**
     * 徽章颜色
     */
    @Column("badge_color")
    private String badgeColor;

    /**
     * 排序
     */
    @Column("sort_order")
    private Integer sortOrder;

    /**
     * 图标URL
     */
    @Column("icon_url")
    private String iconUrl;

    /**
     * 文档URL
     */
    @Column("doc_url")
    private String docUrl;

    /**
     * 元数据（JSON）
     */
    @Column("metadata")
    private String metadata;

    @Column("created_at")
    private LocalDateTime createdAt;

    @Column("updated_at")
    private LocalDateTime updatedAt;
}
