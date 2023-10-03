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
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
                "filter:\n" +
                    "fieldName: field value to be deleted by"
            }
        )
    }
)
public class Delete extends WeaviateConnection implements RunnableTask<Delete.Output> {
    @Schema(
        title = "Class name for which you want to delete data"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String className;

    @Schema(
        title = "id of object to delete"
    )
    @PluginProperty(dynamic = true)
    private String id;

    @Schema(
        title = "Attributes to filter by for deletion"
    )
    @PluginProperty(dynamic = true)
    private Map<String, Object> filter;

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
                .deletedCount(result.getResult() == Boolean.TRUE ? 1 : 0)
                .build();
        }

        if (this.filter == null) {
            throw new IllegalStateException("No properties or id were specified");
        }


        WhereFilter filter = WhereFilter.builder()
            .operator(Operator.And)
            .operands(
                runContext.render(this.filter).entrySet().stream()
                    .map(e -> toWhereFilter(e.getKey(), e.getValue()))
                    .toArray(WhereFilter[]::new)
            )
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
            .deletedCount(response.getResults().getSuccessful())
            .ids(Arrays.stream(response.getResults().getObjects()).map(BatchDeleteResponse.ResultObject::getId).toList())
            .build();
    }

    private WhereFilter toWhereFilter(String path, Object value) {
        WhereFilter.WhereFilterBuilder builder = WhereFilter.builder()
            .path(path)
            .operator(Operator.Like);

        if(value instanceof String typedValue) {
            builder.valueText(typedValue);
        } else if(value instanceof Boolean typedValue) {
            builder.operator(Operator.Equal).valueBoolean(typedValue);
        } else if(value instanceof Date typedValue) {
            builder.valueDate(typedValue);
        } else if(value instanceof Integer typedValue) {
            builder.operator(Operator.Equal).valueNumber((double) typedValue);
        } else if(value instanceof Double typedValue) {
            builder.operator(Operator.Equal).valueNumber(typedValue);
        } else {
            builder.valueText(value.toString());
        }

        return builder.build();
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
        private long deletedCount;

        @Schema(
            title = "IDs of deleted objects"
        )
        private List<String> ids;
    }
}
