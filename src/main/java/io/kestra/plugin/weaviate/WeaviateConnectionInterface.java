package io.kestra.plugin.weaviate;

import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public interface WeaviateConnectionInterface {
    @Schema(
        title = "Configure Weaviate endpoint URL",
        description = "Full http or https host for the Weaviate cluster, including port when not default. Example: localhost:8080 or https://cluster-id.weaviate.network"
    )
    @NotBlank
    @PluginProperty(dynamic = true, group = "connection")
    String getUrl();

    @Schema(
        title = "Provide managed cluster API key",
        description = "Optional bearer-style key for hosted Weaviate; if omitted, requests use anonymous access."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getApiKey();

    @Schema(
        title = "Set custom request headers",
        description = "Key/value headers appended to every call, e.g. extra auth tokens for upstream services."
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, String>> getHeaders();
}
