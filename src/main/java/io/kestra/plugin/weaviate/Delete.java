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
import io.weaviate.client.v1.batch.model.BatchDeleteOutput;
import io.weaviate.client.v1.batch.model.BatchDeleteResponse;
import io.weaviate.client.v1.filters.Operator;
import io.weaviate.client.v1.filters.WhereFilter;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete request to Weaviate database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send delete request to a Weaviate database. Id or properties should be specified.",
            code = {
                "host: localhost:8080",
                "apiKey: some_api_key",
                "className: WeaviateObject",
                "properties:\n" +
                    "fieldName: field value to be deleted by"
            }
        )
    }
)
public class Delete extends WeaviateConnection implements RunnableTask<Delete.Output> {

    @NotBlank
    @PluginProperty(dynamic = true)
    protected String className;

    @PluginProperty(dynamic = true)
    protected String id;

    protected Map<String, String> properties;

    @Override
    public Delete.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);
        String renderedClassName = runContext.render(className);

        if (id != null) {
            Result<Boolean> result = client.data()
                .deleter()
                .withClassName(renderedClassName)
                .withID(id)
                .run();

            return Output.builder()
                .className(renderedClassName)
                .success(result.getResult())
                .build();
        }

        if (properties == null) {
            throw new IllegalStateException("No properties or id were specified");
        }

        WhereFilter filter = WhereFilter.builder()
            .path(properties.keySet().toArray(String[]::new))
            .operator(Operator.Like)
            .valueText(properties.values().toArray(String[]::new))
            .build();

        Result<BatchDeleteResponse> result = client.batch()
            .objectsBatchDeleter()
            .withOutput(BatchDeleteOutput.VERBOSE)
            .withClassName(renderedClassName)
            .withWhere(filter)
            .run();

        if (result.hasErrors()) {
            String message = result.getError().getMessages().stream()
                .map(WeaviateErrorMessage::getMessage)
                .collect(Collectors.joining(", "));

            throw new IOException(message);
        }

        BatchDeleteResponse response = result.getResult();

        return Output.builder()
            .className(renderedClassName)
            .success(!result.hasErrors())
            .deletedCounts(response.getResults().getSuccessful())
            .ids(Arrays.stream(response.getResults().getObjects()).map(BatchDeleteResponse.ResultObject::getId).toList())
            .build();
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Class name of deleted object"
        )
        private String className;

        @Schema(
            title = "If the delete was successful"
        )
        private Boolean success;

        @Schema(
            title = "Number of deleted objects"
        )
        private long deletedCounts;

        @Schema(
            title = "IDs of deleted objects"
        )
        private List<String> ids;
    }
}
