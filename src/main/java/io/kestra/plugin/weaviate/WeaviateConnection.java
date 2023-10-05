package io.kestra.plugin.weaviate;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.weaviate.client.Config;
import io.weaviate.client.WeaviateAuthClient;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.v1.auth.exception.AuthException;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Collections;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class WeaviateConnection extends Task implements WeaviateConnectionInterface {
    private String url;

    private String apiKey;

    @Builder.Default
    private Map<String, String> headers = Collections.emptyMap();

    protected WeaviateClient connect(RunContext context) throws AuthException, IllegalVariableEvaluationException {
        String renderedUrl = context.render(url);
        int schemeSeparatorIdx = renderedUrl.indexOf("://");
        String scheme = schemeSeparatorIdx == -1 ? "https" : renderedUrl.substring(0, schemeSeparatorIdx);
        @SuppressWarnings({"unchecked", "rawtypes"})
        Config config = new Config(
            scheme,
            renderedUrl.substring(schemeSeparatorIdx == -1 ? 0 : schemeSeparatorIdx + 3),
            context.render((Map) headers)
        );

        if (apiKey == null) {
            return new WeaviateClient(config);
        }

        return WeaviateAuthClient.apiKey(config, context.render(apiKey));
    }
}
