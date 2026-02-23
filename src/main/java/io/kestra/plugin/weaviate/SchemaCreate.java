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
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import lombok.experimental.SuperBuilder;

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
    title = "Create a Weaviate class schema",
    description = "Creates a new Weaviate class with the provided properties. Fails if the class already exists; data types must match Weaviate schema types."
)
@Plugin(
    examples = {
        @Example(
            title = "Send schema creation request to a Weaviate database.",
            full = true,
            code = """
                id: create_weaviate_schema
                namespace: company.team

                tasks:
                  - id: schema
                    type: io.kestra.plugin.weaviate.SchemaCreate
                    url: "https://demo-cluster-id.weaviate.network"
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Movies
                    fields:
                      name:
                        - string
                      description:
                        - string
                      category:
                        - string"""
        )
    }
)
public class SchemaCreate extends WeaviateConnection implements RunnableTask<SchemaCreate.Output> {

    @Schema(
        title = "Class name where your data will be stored"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    private String className;

    @Schema(
        title = "Properties to add to the class",
        description = "Map of property name to a list of Weaviate data types (e.g. string, int). All listed properties are created on the new class."
    )
    private io.kestra.core.models.property.Property<Map<String, List<String>>> fields;

    @Override
    public SchemaCreate.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);

        List<Property> properties = runContext.render(fields).asMap(String.class, List.class)
            .entrySet().stream()
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
            title = "Whether the schema creation succeeded"
        )
        private Boolean success;

    }
}
