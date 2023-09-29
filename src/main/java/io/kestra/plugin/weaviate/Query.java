package io.kestra.plugin.weaviate;

import com.google.gson.internal.LinkedTreeMap;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.graphql.model.GraphQLError;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "GraphQL query request to Weaviate database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a GraphQL query request to a Weaviate database, which allows you to retrieve data from the database",
            code = {
                "host: localhost:8080",
                "apiKey: some_api_key",
                "query: " + """
                       {
                          Get {
                            ObjectClassName (
                              limit: 50
                            ) {
                              title,
                              description
                            }
                          }
                        }
                       """
            }
        )
    }
)
public class Query extends WeaviateConnection implements RunnableTask<Query.Output> {

    @Schema(
        title = "GraphQL query which will be executed"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    @NotBlank
    private String query;

	@Schema(
		title = "The way you want to store data",
		description = "FETCH_ONE output the first row\n"
			+ "FETCH output all the row\n"
			+ "STORE store all row in a file\n"
			+ "NONE do nothing"
	)
	@PluginProperty
	@NotNull
	@Builder.Default
	protected FetchType fetchType = FetchType.STORE;

    @Override
    public Query.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        Result<GraphQLResponse> result = client.graphQL()
            .raw()
            .withQuery(runContext.render(query))
            .run();

        if (result.hasErrors() || result.getResult().getErrors() != null) {
            String message = Optional.ofNullable(result.getError())
                .map(weaviateError -> weaviateError.getMessages().stream()
                    .map(WeaviateErrorMessage::getMessage)
                    .collect(Collectors.joining(", ")))
                .orElse(Arrays.stream(result.getResult().getErrors())
                    .map(GraphQLError::getMessage)
                    .collect(Collectors.joining(", ")));

            throw new IOException(message);
        }

        Output.OutputBuilder outputBuilder = Output.builder();

        return (switch (fetchType) {
            case FETCH_ONE -> {
                Map<String, Object> data = extractRow(result);
                int size = (int) data.values().stream()
                    .mapToLong(object -> ((List<Object>) object).size())
                    .sum();
                yield outputBuilder
                    .size(size)
                    .row(data)
                    .build();
            }
            case FETCH -> {
                List<Map<String, Object>> data = extractRows(result);
                int size = (int) data.stream()
                    .map(Map::values)
                    .flatMap(collection -> collection.stream().flatMap(object -> ((List<Object>) object).stream()))
                    .count();
                yield outputBuilder
                    .size(size)
                    .rows(data)
                    .build();
            }
            case STORE -> {
                List<Map<String, Object>> data = extractRows(result);
                int size = (int) data.stream()
                    .map(Map::values)
                    .flatMap(collection -> collection.stream().flatMap(object -> ((List<Object>) object).stream()))
                    .count();

                yield outputBuilder
                    .size(size)
                    .uri(store(data, runContext))
                    .rows(data)
                    .build();
            }
            default -> outputBuilder.build();
        });
    }

    private URI store(List<Map<String, Object>> data, RunContext runContext) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
             OutputStream outputStream = new FileOutputStream(tempFile)) {

            for (var row : data) {
                FileSerde.write(outputStream, row);
            }

            fileWriter.flush();
        }

        return runContext.putTempFile(tempFile);
    }

    private Map<String, Object> extractRow(Result<GraphQLResponse> result) {
        Object data = result.getResult().getData();
        LinkedTreeMap<String, Object> dataMap = (LinkedTreeMap<String, Object>) data;
        return dataMap.values().stream()
            .map(object -> (Map<String, Object>) object)
            .flatMap(stringObjectMap -> stringObjectMap.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private List<Map<String, Object>> extractRows(Result<GraphQLResponse> result) {
        Object data = result.getResult().getData();
        LinkedTreeMap<String, Object> dataMap = (LinkedTreeMap<String, Object>) data;
        return dataMap.values().stream()
            .map(object -> (Map<String, Object>) object).toList();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Map containing the fetched data"
        )
        private Map<String, Object> row;

        @Schema(
            title = "Map containing the fetched data"
        )
        private List<Map<String, Object>> rows;

        @Schema(
            title = "The URI of the stored result",
            description = "Only populated if using the store as true"
        )
        private URI uri;

        @Schema(
            title = "The amount of rows fetched"
        )
        private int size;

    }
}
