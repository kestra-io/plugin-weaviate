package io.kestra.plugin.weaviate;

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

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class WeaviateConnection extends Task implements WeaviateConnectionInterface {
    private String url;

    private Property<String> apiKey;

    private Property<Map<String, String>> headers;

    protected WeaviateClient connect(RunContext runContext) throws AuthException, IllegalVariableEvaluationException {
        String renderedUrl = runContext.render(url);
        int schemeSeparatorIdx = renderedUrl.indexOf("://");
        String scheme = schemeSeparatorIdx == -1 ? "https" : renderedUrl.substring(0, schemeSeparatorIdx);
        @SuppressWarnings({"unchecked", "rawtypes"})
        Config config = new Config(
            scheme,
            renderedUrl.substring(schemeSeparatorIdx == -1 ? 0 : schemeSeparatorIdx + 3),
            runContext.render(headers).asMap(String.class, String.class)
        );

        if (apiKey == null) {
            return new WeaviateClient(config);
        }

        return WeaviateAuthClient.apiKey(config, runContext.render(apiKey).as(String.class).orElse(null));
    }
}
