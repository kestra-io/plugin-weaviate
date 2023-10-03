package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;

public interface WeaviateConnectionInterface {

    @Schema(
        title = "Connection scheme. Default is https"
    )
    @PluginProperty(dynamic = true)
    String getScheme();

    @Schema(
        title = "Connection host",
        description = "Example: localhost:8080"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    String getHost();

    @Schema(
        title = "Connection api key",
        description = "If not provided, the anonymous authentication scheme will be used"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    String getApiKey();
}
