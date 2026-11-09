package io.kestra.plugin.weaviate;

import java.time.Duration;
import java.util.Map;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;

public interface WeaviateConnectionInterface {
    @Schema(
        title = "Configure Weaviate endpoint URL",
        description = """
            HTTP(S) URL of the Weaviate server, including the port when it is not the scheme default.
            Without a scheme, https is assumed. Example: http://localhost:8080 or https://cluster-id.weaviate.cloud.
            Weaviate Cloud URLs (*.weaviate.cloud, *.weaviate.network) are detected automatically. Requires Weaviate 1.32 or later."""
    )
    @NotBlank
    @PluginProperty(dynamic = true, group = "connection")
    String getUrl();

    @Schema(
        title = "Provide managed cluster API key",
        description = "Optional API key; required for Weaviate Cloud. If omitted, requests use anonymous access."
    )
    @PluginProperty(secret = true, group = "connection")
    Property<String> getApiKey();

    @Schema(
        title = "Override gRPC host",
        description = """
            Host of the Weaviate gRPC endpoint, used for queries and batch inserts.
            Defaults to the host of `url`, or to the `grpc-` prefixed host on Weaviate Cloud."""
    )
    @PluginProperty(group = "connection")
    Property<String> getGrpcHost();

    @Schema(
        title = "Override gRPC port",
        description = """
            Port of the Weaviate gRPC endpoint, between 1 and 65535. Defaults to 50051, or 443 on Weaviate Cloud.
            gRPC uses TLS when `url` is https."""
    )
    @PluginProperty(group = "connection")
    Property<Integer> getGrpcPort();

    @Schema(
        title = "Set request timeout",
        description = "Timeout applied to connection, query and insert requests. Defaults to the Weaviate client defaults (30s connect, 60s query, 120s insert)."
    )
    @PluginProperty(group = "advanced")
    Property<Duration> getRequestTimeout();

    @Schema(
        title = "Set custom request headers",
        description = """
            Key/value headers sent with every REST and gRPC call, e.g. vectorizer API keys such as `X-OpenAI-Api-Key`.
            Values that are credentials should come from `{{ secret('...') }}`.
            The plugin also sends an `X-Weaviate-Client-Integration` header identifying itself; setting that key here overrides it."""
    )
    @PluginProperty(group = "advanced")
    Property<Map<String, String>> getHeaders();
}
