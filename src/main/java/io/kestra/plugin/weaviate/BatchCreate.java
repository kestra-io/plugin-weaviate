package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
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

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Batch insert for Weaviate database.",
    description = "Batch insert in Weaviate database. If schema does not exist, schema will automatically be created"
)
@Plugin(
    examples = {
        @Example(
            title = "Send batch object creation request to a Weaviate database",
            code = {
                "host: localhost:8080",
                "apiKey: some_api_key",
                "className: WeaviateObject",
                "objects: " +
                    "   textField: \"text\"",
                "   numField: 32"
            }
        )
    }
)
public class BatchCreate extends WeaviateConnection implements RunnableTask<VoidOutput> {

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
    public VoidOutput run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        List<WeaviateObject> weaviateObjects = new ArrayList<>();

        if (objects instanceof List) {
            weaviateObjects = ((List<Map<String, Object>>) objects).stream()
                .map(param -> WeaviateObject.builder()
                    .id(UUID.randomUUID().toString())
                    .className(className)
                    .properties(param)
                    .build()
                ).toList();
        } else if (objects instanceof String uri) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                runContext.uriToInputStream(URI.create(uri))
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

        return null;
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
            description = "Content of file will be the created objects data"
        )
        private URI uri;

        @Schema(
            title = "The number of created objects"
        )
        private int createdCount;

    }
}
