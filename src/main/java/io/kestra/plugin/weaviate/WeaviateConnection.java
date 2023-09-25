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
import java.util.List;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class WeaviateConnection extends Task implements WeaviateConnectionInterface {

    @Builder.Default
    public String scheme = "https";

    @NotBlank
    public String host;

    @NotBlank
    public String apiKey;

    protected WeaviateClient connect(RunContext context) throws AuthException, IllegalVariableEvaluationException {
        Config config = new Config(scheme, host);

        return WeaviateAuthClient.apiKey(config, context.render(apiKey));
    }

}
