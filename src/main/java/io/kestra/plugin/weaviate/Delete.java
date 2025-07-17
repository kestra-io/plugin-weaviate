package io.kestra.plugin.weaviate;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateErrorMessage;
import io.weaviate.client.v1.batch.model.BatchDeleteOutput;
import io.weaviate.client.v1.batch.model.BatchDeleteResponse;
import io.weaviate.client.v1.filters.Operator;
import io.weaviate.client.v1.filters.WhereFilter;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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
    title = "Delete specific objects in a Weaviate database."
)
@Plugin(
    examples = {
        @Example(
            title = "Send delete request to a Weaviate database. Use object ID or other properties.",
            full = true,
            code = """
                   id: weaviate_delete_flow
                   namespace: company.team

                   tasks:
                     - id: delete
                       type: io.kestra.plugin.weaviate.Delete
                       url: https://demo-cluster-id.weaviate.network
                       className: WeaviateObject
                       filter:
                         fieldName: field value to be deleted by
                   """
        )
    }
)
public class Delete extends WeaviateConnection implements RunnableTask<Delete.Output> {
    private static Logger logger = LoggerFactory.getLogger(Delete.class);

    @Schema(
        title = "Class name for which you want to delete data"
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String className;

    @Schema(
        title = "Id of the object to delete"
    )
    @PluginProperty(dynamic = true)
    private String objectId;

    @Schema(
        title = "Attributes to filter by for deletion"
    )
    private Property<Map<String, Object>> filter;

    @Override
    public Delete.Output run(RunContext runContext) throws Exception {
        WeaviateClient client = connect(runContext);
        String renderedClassName = runContext.render(className);

        if (objectId != null) {
            Result<Boolean> result = client.data()
                .deleter()
                .withClassName(renderedClassName)
                .withID(objectId)
                .run();

            return Output.builder()
                .className(renderedClassName)
                .success(result.getResult())
                .deletedCount(result.getResult() == Boolean.TRUE ? 1 : 0)
                .build();
        }

        WhereFilter filter = null;
        if (this.filter != null) {
            filter = WhereFilter.builder()
                .operator(Operator.And)
                .operands(
                    runContext.render(this.filter).asMap(String.class, Object.class)
                        .entrySet()
                        .stream()
                        .map(e -> toWhereFilter(e.getKey(), e.getValue()))
                        .toArray(WhereFilter[]::new)
                )
                .build();
        } else {
            filter = WhereFilter.builder()
                .path("_id")
                .operator(Operator.NotEqual)
                .valueText("")
                .build();
        }

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

        List<String> ids = Collections.emptyList();
        if (response.getResults() != null && response.getResults().getObjects() != null) {
            ids = Arrays.stream(response.getResults().getObjects())
                .map(BatchDeleteResponse.ResultObject::getId)
                .toList();
        }

        return Output.builder()
            .className(renderedClassName)
            .success(!result.hasErrors())
            .deletedCount(response.getResults().getSuccessful())
            .ids(ids)
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
            title = "Class name of the deleted object"
        )
        private String className;

        @Schema(
            title = "Whether the delete operation was successful"
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
