package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import jakarta.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Batch insert Weaviate objects",
    description = "Loads multiple objects into a class from a Kestra ION file or an inline list. Each object gets a generated UUID. If the class is missing, Weaviate auto-schema (if enabled) will create it; otherwise the call fails."
)
@Plugin(
    examples = {
        @Example(
            title = "Send batch object creation request to a Weaviate database.",
            full = true,
            code = """
                id: weaviate_batch_load
                namespace: company.team

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
            title = "Send batch object creation request to a Weaviate database using an ION input file e.g. passed from output of another task.",
            full = true,
            code = """
                id: weaviate_batch_insert
                namespace: company.team

                tasks:
                  - id: extract
                    type: io.kestra.plugin.core.http.Download
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
public class BatchCreate extends WeaviateConnection implements RunnableTask<VoidOutput> {

    @Schema(
        title = "Class name where you want to insert data"
    )
    private Property<String> className;

    @Schema(
        title = "Objects to insert with properties",
        description = "Either a Kestra storage URI to an ION array or an inline list of maps. Each entry becomes one Weaviate object.",
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
                .map(throwFunction(param -> WeaviateObject.builder()
                    .id(UUID.randomUUID().toString())
                    .className(runContext.render(className).as(String.class).orElse(null))
                    .properties(runContext.render(param))
                    .build()
                )).toList();
        } else if (objects instanceof String uri) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                runContext.storage().getFile(URI.create(runContext.render(uri)))
            ))) {
                weaviateObjects = FileSerde.readAll(reader, Map.class)
                    .map(throwFunction(map -> WeaviateObject.builder()
                        .id(UUID.randomUUID().toString())
                        .className(runContext.render(className).as(String.class).orElse(null))
                        .properties(map)
                        .build()
                    )).collectList().block();
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
}
