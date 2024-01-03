package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public interface WeaviateConnectionInterface {
    @Schema(
        title = "Connection URL",
        description = "Example: localhost:8080 or https://cluster-id.weaviate.network"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    String getUrl();

    @Schema(
        title = "API key to authenticate with a managed Weaviate cluster",
        description = "If not provided, the anonymous authentication scheme will be used."
    )
    @PluginProperty(dynamic = true)
    String getApiKey();

    @Schema(
        title = "Additional headers to add to the request e.g. to authenticate with OpenAI API"
    )
    @PluginProperty(dynamic = true)
    Map<String, String> getHeaders();
}
