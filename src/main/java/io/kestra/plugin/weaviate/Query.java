package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import io.weaviate.client.v1.graphql.query.fields.Field;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

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

    protected String className;

//    protected Map<String, Object> properties;
    protected Object properties;

    protected String id;

    protected Integer batchSize;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        create(client);

        retrieve(client);

        retrieveAll(client);

        update(client);

        delete(client);

        return null;
    }

    private Result<Boolean> delete(WeaviateClient client) {
        return client.data().deleter()
            .withClassName(className)
            .withID(id)
            .run();
    }

    private Result<Boolean> update(WeaviateClient client) {
        return client.data().updater()
            .withMerge()
            .withID(id)
            .withClassName(className)
            .withProperties((Map<String, Object>) properties)
            .run();
    }

    private Result<GraphQLResponse> retrieveAll(WeaviateClient client) {
        return client.graphQL().get()
            .withClassName(className)
            .withFields(Stream.concat(((List<String>) properties).stream(), Stream.of("_additional { id vector }"))
                .map(prop -> Field.builder().name(prop).build())
                .toArray(Field[]::new))
            .withLimit(batchSize)
            .run();
    }

    private Result<List<WeaviateObject>> retrieve(WeaviateClient client) {
        return client.data().objectsGetter()
            .withClassName(className)
            .withID(id)
            .run();
    }

    private Result<WeaviateObject> create(WeaviateClient client) {
        return client.data().creator()
            .withClassName(className)
            .withProperties((Map<String, Object>) properties)
            .run();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {
    }
}
