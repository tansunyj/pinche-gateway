package com.llmate.multiprotocol.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

/**
 * 端点表实体
 * 对应数据库表: endpoint
 *
 * 存储模型绑定的上游端点路径。新库（pt_carpool）用它替代旧 model_channel_configs /
 * model_templates 的端点拼接：proxy_channel_models.use_endpoint_id → endpoint.path。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("endpoint")
public class EndpointEntity {

    @Id
    private Long id;

    /**
     * 端点名称，如 dashscope-embeddings
     */
    @Column("endpoint_name")
    private String endpointName;

    /**
     * 端点路径，如 /api/v1/embeddings/text-embedding
     */
    @Column("path")
    private String path;
}
