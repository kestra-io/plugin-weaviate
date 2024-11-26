package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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
    title = "Query Weaviate database with GraphQL."
)
@Plugin(
    examples = {
        @Example(
            title = "Execute a GraphQL query to fetch data from a Weaviate database.",
            full = true,
            code = """
                id: weaviate_query
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.weaviate.Query
                    url: https://demo-cluster-id.weaviate.network
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    query: |
                      {
                        Get {
                          Question(limit: 5) {
                            question
                            answer
                            category
                          }
                        }
                      }

                """
        ),
        @Example(
            title = "Query data from a Weaviate database using Generative Search with OpenAI",
            full = true,
            code = """
                id: weaviate_generative_search
                namespace: company.team

                tasks:
                  - id: query
                    type: io.kestra.plugin.weaviate.Query
                    url: https://demo-cluster-id.weaviate.network
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    headers:
                      X-OpenAI-Api-Key: "{{ secret('OPENAI_API_KEY') }}"
                    query: |
                      {
                        Get {
                          Question(limit: 5, nearText: {concepts: ["biology"]}) {
                            question
                            answer
                            category
                          }
                        }
                      }
                """
        )
    }
)
public class Query extends WeaviateConnection implements RunnableTask<FetchOutput> {

    @Schema(
        title = "GraphQL query"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    private String query;

	@Schema(
		title = "How you want to store the output data",
		description = "FETCH_ONE outputs only the first row\n"
			+ "FETCH outputs all rows\n"
			+ "STORE stores all rows in a file\n"
			+ "NONE doesn't store any data. It's particularly useful when you execute DDL statements or run queries that insert data into another table e.g. using `SELECT ... INSERT INTO` statements."
	)
	@NotNull
	@Builder.Default
	protected Property<FetchType> fetchType = Property.of(FetchType.STORE);

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

        FetchType renderedFetchType = runContext.render(fetchType).as(FetchType.class).orElseThrow();
        return (switch (renderedFetchType) {
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

                if(FetchType.FETCH.equals(renderedFetchType)) {
                    yield outputBuilder.rows(rows).build();
                } else {
                    yield outputBuilder.uri(store(rows, runContext)).build();
                }
            }
            default -> outputBuilder.build();
        });
    }

    private URI store(List<Object> data, RunContext runContext) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
             var output = new BufferedWriter(new FileWriter(tempFile), FileSerde.BUFFER_SIZE)) {

            var flux = Flux.fromIterable(data);
            FileSerde.writeAll(output, flux).block();

            fileWriter.flush();
        }

        return runContext.storage().putFile(tempFile);
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
