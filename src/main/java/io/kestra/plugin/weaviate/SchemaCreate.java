package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.schema.model.Property;
import io.weaviate.client.v1.schema.model.WeaviateClass;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(title = "Create class schema in Weaviate database.")
@Plugin(examples = {
    @Example(title = "Send schema creation request to a Weaviate database", code = {

    })
})
public class SchemaCreate extends WeaviateConnection implements RunnableTask<SchemaCreate.Output>, SchemaCreateInterface {

    protected String className;

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

        return Output.builder().success(result.getResult()).hasErrors(result.hasErrors()).build();
    }

    private static Property buildProperty(Map.Entry<String, List<String>> entry) {
        return Property.builder().name(entry.getKey()).dataType(entry.getValue()).build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        private Boolean success;

        private Boolean hasErrors;

    }
}
