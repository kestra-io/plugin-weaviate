package io.kestra.plugin.weaviate;

import com.google.gson.internal.LinkedTreeMap;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.graphql.model.GraphQLResponse;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import java.io.*;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
            title = "Send a GraphQL raw query request to a Weaviate database, which allows you to retrieve data from the database",
            code = {
                "host: localhost:8080",
                "apiKey: some_api_key"
            }
        )
    }
)
public class RawQuery extends WeaviateConnection implements RunnableTask<RawQuery.Output>, RawQueryInterface {

    @NotBlank
    protected String query;

    @Builder.Default
    protected boolean store = false;

    @Override
    public RawQuery.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        Result<GraphQLResponse> result = client.graphQL().raw().withQuery(runContext.render(query)).run();

        if (result.hasErrors()) {
            String message = result.getError().getMessages().stream()
                .map(WeaviateErrorMessage::getMessage)
                .collect(Collectors.joining(", "));

            throw new IOException(message);
        }

        Map<String, Object> data = extractData(result);
        Output.OutputBuilder outputBuilder = Output.builder().data(data);

        if (store) {
            outputBuilder.uri(store(data, runContext));
        }

        int size = data.values().stream().map(object -> (List<Object>) object).findAny().map(List::size).orElse(0);

        return outputBuilder.size(size).build();
    }

    private URI store(Map<String, Object> data, RunContext runContext) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
             OutputStream outputStream = new FileOutputStream(tempFile)) {

            for (var row : data.entrySet()) {
                FileSerde.write(outputStream, row);
            }

            fileWriter.flush();
        }

        return runContext.putTempFile(tempFile);
    }

    private Map<String, Object> extractData(Result<GraphQLResponse> result) {
        String responseKey = "Get";

        Object data = result.getResult().getData();
        LinkedTreeMap<String, Object> dataMap = (LinkedTreeMap<String, Object>) data;
        return (Map<String, Object>) dataMap.getOrDefault(responseKey, new LinkedHashMap<>());
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Map containing the fetched data"
        )
        private Map<String, Object> data;

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
