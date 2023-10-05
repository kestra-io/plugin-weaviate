package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.utils.Rethrow;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Batch-insert data to a Weaviate database.",
    description = "Data can be either in an ION-serialized file format or as a list of key-value pairs. If the schema doesn't exist yet, it will be created automatically."
)
@Plugin(
    examples = {
        @Example(
            title = "Send batch object creation request to a Weaviate database",
            full = true,
            code = """
                id: weaviate_batch_load
                namespace: dev

                tasks:
                  - id: batch_load
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: "https://demo-cluster-id.weaviate.network"
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: WeaviateDemo
                    objects: 
                      - textField: "some text"
                        numField: 24
                      - textField: "another text"
                        numField: 42
                """
        ),
        @Example(
            title = "Send batch object creation request to a Weaviate database using an ION input file e.g. passed from output of another task",
            full = true,
            code = """
                id: weaviate_batch_insert
                namespace: dev

                tasks:
                  - id: extract
                    type: io.kestra.plugin.fs.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/ion/ingest.ion

                  - id: batch_insert
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: "https://demo-cluster-id.weaviate.network"
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Titles
                    objects: "{{ outputs.extract.uri }}"
                """
        )
    }
)
public class BatchCreate extends WeaviateConnection implements RunnableTask<BatchCreate.Output> {

    @Schema(
        title = "Class name where you want to insert data"
    )
    @PluginProperty(dynamic = true)
    private String className;

    @Schema(
        title = "Objects to create with their properties",
        description = "ION File URI or the list of objects to insert",
        anyOf = {
            String.class,
            Map[].class
        }
    )
    @NotNull
    private Object objects;

    @Override
    public BatchCreate.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        List<WeaviateObject> weaviateObjects = new ArrayList<>();

        if (objects instanceof List) {
            weaviateObjects = ((List<Map<String, Object>>) objects).stream()
                .map(throwFunction(param -> WeaviateObject.builder()
                    .id(UUID.randomUUID().toString())
                    .className(className)
                    .properties(runContext.render(param))
                    .build()
                )).toList();
        } else if (objects instanceof String uri) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                runContext.uriToInputStream(URI.create(runContext.render(uri)))
            ))) {
                weaviateObjects = Flowable.create(FileSerde.reader(reader, Map.class), BackpressureStrategy.BUFFER)
                    .map(map -> WeaviateObject.builder()
                        .id(UUID.randomUUID().toString())
                        .className(runContext.render(className))
                        .properties(map)
                        .build()
                    ).toList().blockingGet();
            }
        }

        Result<ObjectGetResponse[]> result;
        try (ObjectsBatcher objectsBatcher = client.batch()
            .objectsBatcher()) {
            result = objectsBatcher
                .withObjects(weaviateObjects.toArray(WeaviateObject[]::new))
                .run();
        }

        if (result.hasErrors()) {
            String message = result.getError().getMessages().stream()
                .map(WeaviateErrorMessage::getMessage)
                .collect(Collectors.joining(", "));

            throw new IOException(message);
        }

        ObjectGetResponse[] responses = result.getResult();
        return Output.builder()
            .uri(store(responses, runContext))
            .createdCount(result.getResult().length)
            .build();
    }

    private URI store(ObjectGetResponse[] responses, RunContext runContext) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        try (
            BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
            OutputStream outputStream = new FileOutputStream(tempFile)
        ) {
            List<Map.Entry<String, Object>> data = Arrays.stream(responses)
                .map(ObjectGetResponse::getProperties)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .toList();

            for (Map.Entry<String, Object> row : data) {
                FileSerde.write(outputStream, row);
            }

            fileWriter.flush();
        }

        return runContext.putTempFile(tempFile);
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The URI of stored data",
            description = "The contents of the file will be the data ingested into Weaviate."
        )
        private URI uri;

        @Schema(
            title = "The number of created objects"
        )
        private int createdCount;

    }
}
