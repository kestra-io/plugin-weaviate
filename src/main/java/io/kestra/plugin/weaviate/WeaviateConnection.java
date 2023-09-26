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

import javax.validation.constraints.NotBlank;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class WeaviateConnection extends Task implements WeaviateConnectionInterface {

    @Builder.Default
    private String scheme = "https";

    @NotBlank
    private String host;

    @NotBlank
    private String apiKey;

    protected WeaviateClient connect(RunContext context) throws AuthException, IllegalVariableEvaluationException {
        Config config = new Config(context.render(scheme), context.render(host));

        if (apiKey == null) {
            return new WeaviateClient(config);
        }

        return WeaviateAuthClient.apiKey(config, context.render(apiKey));
    }
}
