package com.llmate.multiprotocol.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * proxy_channel_models.provider_capability 单对象能力快照
 *
 * 与 {@code ProviderCapabilityCatalog} 枚举一一对应：
 * - provider_alias：路由 key，指向枚举中的 alias
 * - domain：能力域（chat/image/video），冗余存储便于后台校验与展示
 * - class_name：Adapter 实现类全限定名，自描述快照，不参与实例化
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProviderCapability {

    @JsonProperty("provider_alias")
    private String providerAlias;

    @JsonProperty("domain")
    private String domain;

    @JsonProperty("class_name")
    private String className;
}
