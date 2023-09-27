package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Batch create request to Weaviate database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send batch object creation request to a Weaviate database",
            code = {
                "host: localhost:8080",
                "apiKey: some_api_key",
                "className: WeaviateObject"
            }
        )
    }
)
public class BatchCreate extends WeaviateConnection implements RunnableTask<BatchCreate.Output>, BatchCreateInterface {

    protected String className;

    @Builder.Default
    protected List<Map<String, Object>> parameters = List.of(new HashMap<>());

    @Override
    public BatchCreate.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        List<WeaviateObject> objects = new ArrayList<>();

        for (Map<String, Object> properties : parameters) {
            objects.add(WeaviateObject.builder().className(className).properties(properties).build());
        }

        Result<ObjectGetResponse[]> result = client.batch().objectsBatcher().withObjects(objects.toArray(WeaviateObject[]::new)).run();

        if (result.hasErrors()) {
            String message = result.getError().getMessages().stream()
                .map(WeaviateErrorMessage::getMessage)
                .collect(Collectors.joining(", "));

            throw new IOException(message);
        }

        List<ObjectGetResponse> responses = List.of(result.getResult());

        return Output.builder()
            .className(responses.stream().map(ObjectGetResponse::getClassName).toList())
            .properties(responses.stream().map(ObjectGetResponse::getProperties).toList())
            .createdCounts(responses.size())
            .build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The class name of the created objects"
        )
        private List<String> className;

        @Schema(
            title = "The properties of the created objects"
        )
        private List<Map<String, Object>> properties;

        @Schema(
            title = "The number of created objects"
        )
        private long createdCounts;

    }
}
