package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

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
            Map[].class,
            List.class,
        }
    )
    @NotNull
    private Object objects;

    private StorageInterface storageInterface;

    @Override
    public BatchCreate.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        List<WeaviateObject> weaviateObjects = new ArrayList<>();

        if (objects instanceof List objectList) {
            if (!objectList.isEmpty() && objectList.get(0) instanceof Map<?,?>) {
                objectList.stream()
                    .map(param -> WeaviateObject.builder()
                        .className(className)
                        .properties((Map<String, Object>) param)
                        .build())
                    .forEach(object -> weaviateObjects.add((WeaviateObject) object));
            }
        } else if (objects instanceof Map[]) {
            Arrays.stream((Map<String, Object>[]) objects)
                .map(param -> WeaviateObject.builder()
                    .className(className)
                    .properties(param)
                    .build())
                .forEach(weaviateObjects::add);
        }

        if (objects instanceof String) {
            URI uri = URI.create((String) objects);
            String outputFileContent = IOUtils.toString(storageInterface.get(uri), Charsets.UTF_8);
            Map parameters = JacksonMapper.ofYaml().readValue(outputFileContent, Map.class);

            WeaviateObject weaviateObject = WeaviateObject.builder()
                .className(runContext.render(className))
                .properties(parameters)
                .build();

            weaviateObjects.add(weaviateObject);
        }

        Result<ObjectGetResponse[]> result = client.batch()
            .objectsBatcher()
            .withObjects(weaviateObjects.toArray(WeaviateObject[]::new))
            .run();

        if (result.hasErrors()) {
            String message = result.getError().getMessages().stream()
                .map(WeaviateErrorMessage::getMessage)
                .collect(Collectors.joining(", "));

            throw new IOException(message);
        }

        ObjectGetResponse[] responses = result.getResult();
        return Output.builder()
            .className(Arrays.stream(result.getResult()).map(ObjectGetResponse::getClassName).toList())
            .properties(Arrays.stream(result.getResult()).map(ObjectGetResponse::getProperties).toList())
            .uri(store(responses, runContext))
            .createdCounts(result.getResult().length)
            .build();
    }

    private URI store(ObjectGetResponse[] responses, RunContext runContext) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
             OutputStream outputStream = new FileOutputStream(tempFile)) {

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
            title = "The class name of the created objects"
        )
        private List<String> className;

        @Schema(
            title = "The URI of stored data",
            description = "Content of file will be the created objects data"
        )
        private URI uri;

        @Schema(
            title = "The properties of the created objects"
        )
        private List<Map<String, Object>> properties;

        @Schema(
            title = "The number of created objects"
        )
        private int createdCounts;

    }
}
