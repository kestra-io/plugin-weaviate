package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.schema.model.Property;
import io.weaviate.client.v1.schema.model.WeaviateClass;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Create class schema in Weaviate database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send schema creation request to a Weaviate database",
            code = {
                "host: localhost:8080",
                "apiKey: some_api_key",
                "className: WeaviateObject",
                "parameters:\n" +
                "    fieldName:\n" +
                "       - text",
                "       - string"
            }
        )
    }
)
public class SchemaCreate extends WeaviateConnection implements RunnableTask<SchemaCreate.Output> {

    @Schema(
        title = "Class name where your data will be stored"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    protected String className;

    @Schema(
        title = "Fields which will be store data in class",
        description = "Requires specified field name and list of data type that will be stored in this field"
    )
    @PluginProperty(dynamic = true)
    protected Map<String, List<String>> parameters;

    @Override
    public SchemaCreate.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        List<Property> properties = parameters.entrySet().stream()
            .map(SchemaCreate::buildProperty)
            .toList();

        WeaviateClass weaviateClass = WeaviateClass.builder()
            .className(className)
            .properties(properties).build();

        Result<Boolean> result = client.schema()
            .classCreator()
            .withClass(weaviateClass)
            .run();


        if (result.hasErrors()) {
            String message = result.getError().getMessages().stream()
                .map(WeaviateErrorMessage::getMessage)
                .collect(Collectors.joining(", "));

            throw new IOException(message);
        }

        return Output.builder().success(result.getResult()).build();
    }

    private static Property buildProperty(Map.Entry<String, List<String>> entry) {
        return Property.builder().name(entry.getKey()).dataType(entry.getValue()).build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Indicates whether the schema creation was successful"
        )
        private Boolean success;

    }
}
