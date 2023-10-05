package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;
import java.util.Map;

public interface WeaviateConnectionInterface {
    @Schema(
        title = "Connection url",
        description = "Example: localhost:8080 or https://secured.weaviate:8080"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    String getUrl();

    @Schema(
        title = "Connection api key",
        description = "If not provided, the anonymous authentication scheme will be used"
    )
    @PluginProperty(dynamic = true)
    String getApiKey();

    @Schema(
        title = "Additional headers to add to the request"
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getHeaders();
}
