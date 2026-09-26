package io.kestra.plugin.weaviate;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import io.weaviate.client.Config;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.auth.exception.AuthException;
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

    private String url;

    private Property<String> apiKey;

    private Property<Map<String, String>> headers;

    protected WeaviateClient connect(RunContext runContext) throws AuthException, IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(url);
        int schemeSeparatorIdx = renderedUrl.indexOf("://");
        String scheme = schemeSeparatorIdx == -1 ? "https" : renderedUrl.substring(0, schemeSeparatorIdx);
        @SuppressWarnings({ "unchecked", "rawtypes" })
        Config config = new Config(
            scheme,
            renderedUrl.substring(schemeSeparatorIdx == -1 ? 0 : schemeSeparatorIdx + 3),
            buildHeaders(runContext.render(headers).asMap(String.class, String.class))
        );

        if (apiKey == null) {
            return new WeaviateClient(config);
        }

        return WeaviateAuthClient.apiKey(config, runContext.render(apiKey).as(String.class).orElse(null));
    }

    /**
     * Returns the headers sent with every request: the integration header that lets Weaviate
     * attribute traffic to this plugin, then the user's headers, which take precedence.
     */
    static Map<String, String> buildHeaders(Map<String, String> userHeaders) {
        Map<String, String> result = new HashMap<>();
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
            Properties properties = new Properties();
            properties.load(is);
            return properties.getProperty("version", "unknown");
        } catch (IOException e) {
            return "unknown";
        }
    }
}
