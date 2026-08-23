package com.llmate.multiprotocol.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 能力清单条目（GET /admin/provider-capabilities 下拉数据源）
 * 由 ProviderCapabilityCatalog 枚举生成，永不漂移；JSON key 与表/契约一致（snake_case）
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProviderCapabilityItem {

    /** provider_alias（路由 key，绑定行 JSON 用） */
    @JsonProperty("provider_alias")
    private String providerAlias;

    /** 后台展示名 */
    private String name;

    /** Adapter 实现类全限定名（绑定行快照用） */
    @JsonProperty("class_name")
    private String className;
}
