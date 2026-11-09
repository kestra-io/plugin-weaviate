package io.kestra.plugin.weaviate;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.weaviate.client6.v1.api.Authentication;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.WeaviateUnsupportedVersionException;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class WeaviateConnection extends Task implements WeaviateConnectionInterface {
    static final String INTEGRATION_HEADER = "X-Weaviate-Client-Integration";
    static final String INTEGRATION_VALUE = "kestra-plugin-weaviate/" + pluginVersion();
    static final int DEFAULT_GRPC_PORT = 50051;

    private String url;

    @ToString.Exclude
    private Property<String> apiKey;

    private Property<String> grpcHost;

    private Property<Integer> grpcPort;

    private Property<Duration> requestTimeout;

    // Headers often carry vectorizer API keys.
    @ToString.Exclude
    private Property<Map<String, String>> headers;

    // The client holds a gRPC channel, so callers must close it.
    protected WeaviateClient connect(RunContext runContext) throws IllegalVariableEvaluationException {
        var endpoint = Endpoint.parse(
            runContext.render(url),
            runContext.render(grpcHost).as(String.class).orElse(null),
            runContext.render(grpcPort).as(Integer.class).orElse(null)
        );
        var rApiKey = runContext.render(apiKey).as(String.class).orElse(null);
        var rHeaders = buildHeaders(runContext.render(headers).asMap(String.class, String.class));
        var timeoutSeconds = runContext.render(requestTimeout).as(Duration.class)
            .map(d -> (int) Math.max(1, d.toSeconds()))
            .orElse(null);

        try {
            if (endpoint.cloud()) {
                if (rApiKey == null) {
                    throw new IllegalArgumentException("An `apiKey` is required to connect to Weaviate Cloud (" + endpoint.httpHost() + ").");
                }
                return WeaviateClient.connectToWeaviateCloud(endpoint.httpHost(), rApiKey, c ->
                {
                    c.setHeaders(rHeaders);
                    if (timeoutSeconds != null) {
                        c.timeout(timeoutSeconds);
                    }
                    return c;
                });
            }

            return WeaviateClient.connectToCustom(c ->
            {
                // scheme() resets both ports, so it must be set before them.
                c.scheme(endpoint.scheme())
                    .httpHost(endpoint.httpHost())
                    .httpPort(endpoint.httpPort())
                    .grpcHost(endpoint.grpcHost())
                    .grpcPort(endpoint.grpcPort())
                    .setHeaders(rHeaders);
                if (rApiKey != null) {
                    c.authentication(Authentication.apiKey(rApiKey));
                }
                if (timeoutSeconds != null) {
                    c.timeout(timeoutSeconds);
                }
                return c;
            });
        } catch (WeaviateUnsupportedVersionException e) {
            throw new IllegalStateException(
                e.getMessage() + " This plugin version uses the Weaviate v6 client, which requires Weaviate 1.32 or later; " +
                    "use a plugin-weaviate version built on the v5 client for older servers.",
                e
            );
        }
    }

    // User headers override the integration header, matched case-insensitively.
    static Map<String, String> buildHeaders(Map<String, String> userHeaders) {
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        result.put(INTEGRATION_HEADER, INTEGRATION_VALUE);
        if (userHeaders != null) {
            result.putAll(userHeaders);
        }
        return result;
    }

    private static String pluginVersion() {
        try (InputStream is = WeaviateConnection.class.getResourceAsStream("plugin.properties")) {
            if (is == null) {
                return "unknown";
            }
            var properties = new Properties();
            properties.load(is);
            var version = properties.getProperty("version");
            return version == null || version.startsWith("${") ? "unknown" : version;
        } catch (IOException e) {
            return "unknown";
        }
    }

    // `cloud`: Weaviate Cloud derives its own gRPC endpoint from the HTTP host.
    record Endpoint(String scheme, String httpHost, int httpPort, String grpcHost, int grpcPort, boolean cloud) {
        static Endpoint parse(String url, String grpcHost, Integer grpcPort) {
            if (url == null || url.isBlank()) {
                throw new IllegalArgumentException("`url` is required.");
            }
            var uri = URI.create(url.contains("://") ? url.trim() : "https://" + url.trim());
            var scheme = uri.getScheme().toLowerCase(Locale.ROOT);
            if (!scheme.equals("http") && !scheme.equals("https")) {
                throw new IllegalArgumentException("Unsupported scheme in `url`: " + scheme + ". Use http or https.");
            }
            if (uri.getHost() == null) {
                throw new IllegalArgumentException("Invalid Weaviate `url`: " + url);
            }
            if (uri.getPath() != null && !uri.getPath().isEmpty() && !uri.getPath().equals("/")) {
                throw new IllegalArgumentException("Weaviate `url` must not contain a path, got: " + uri.getPath());
            }

            var host = uri.getHost();
            if (grpcPort != null && (grpcPort < 1 || grpcPort > 65535)) {
                throw new IllegalArgumentException("`grpcPort` must be between 1 and 65535, got " + grpcPort + ".");
            }
            int httpPort = uri.getPort() != -1 ? uri.getPort() : scheme.equals("https") ? 443 : 80;
            boolean cloud = grpcHost == null && grpcPort == null && isWeaviateCloud(host);

            return new Endpoint(
                scheme,
                host,
                httpPort,
                grpcHost != null ? grpcHost : host,
                grpcPort != null ? grpcPort : DEFAULT_GRPC_PORT,
                cloud
            );
        }

        private static boolean isWeaviateCloud(String host) {
            var lower = host.toLowerCase(Locale.ROOT);
            return lower.endsWith(".weaviate.cloud") || lower.endsWith(".weaviate.network");
        }
    }
}
