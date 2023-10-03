package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
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
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
public class Query extends WeaviateConnection implements RunnableTask<FetchOutput> {

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
    public FetchOutput run(RunContext runContext) throws Exception {
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

        FetchOutput.FetchOutputBuilder outputBuilder = FetchOutput.builder();

        return (switch (fetchType) {
            case FETCH_ONE -> {
                Map<String, Object> data = extractRow(result);
                yield outputBuilder
                    .size(data == null ? 0L : 1L)
                    .row(data)
                    .build();
            }
            case FETCH, STORE -> {
                var rows = extractRows(result);
                outputBuilder = outputBuilder.size((long) rows.size());

                if(fetchType == FetchType.FETCH) {
                    yield outputBuilder.rows(rows).build();
                } else {
                    yield outputBuilder.uri(store(rows, runContext)).build();
                }
            }
            default -> outputBuilder.build();
        });
    }

    private URI store(List<Object> data, RunContext runContext) throws IOException {
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

    // Response structure:
    // result.getResult().getData() = {"GET": {"Class1": [{"prop": "value"}], "Class2": [{"prop2": "value2"}]}}
    // Method will return {
    private Map<String, List<Map<String, Object>>> extractResultByClassName(Result<GraphQLResponse> result) {
        var castResult = (Map<String, Map<String, List<Map<String, Object>>>>) result.getResult().getData();
        return castResult.values().stream().reduce(new HashMap<>(), (acc, map) -> {
            acc.putAll(map);
            return acc;
        }, (m1, m2) -> {
            m1.putAll(m2);
            return m1;
        });
    }

    private Map<String, Object> extractRow(Result<GraphQLResponse> result) {
        return extractResultByClassName(result).values().stream()
            .findFirst()
            .map(Collection::stream)
            .flatMap(Stream::findFirst)
            .orElse(null);
    }

    private List<Object> extractRows(Result<GraphQLResponse> result) {
        return extractResultByClassName(result).entrySet().stream()
            .flatMap(e -> e.getValue().stream()
                .map(object -> Map.entry(e.getKey(), object))
            ).map(Object.class::cast)
            .toList();
    }
}
