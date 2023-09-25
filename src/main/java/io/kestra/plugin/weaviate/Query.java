package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(title = "Query a Weaviate database with SurrealQL.")
@Plugin(
	examples = {
		@Example(
			title = "Send a GraphQL query to a Weaviate database",
			code = {

			}
		)
	}
)
public class Query extends WeaviateConnection implements RunnableTask<Query.Output>, QueryInterface {
    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        return null;
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
    }
}
