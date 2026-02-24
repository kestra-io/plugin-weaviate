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
    title = "Delete objects from a Weaviate class",
    description = "Deletes a single object by ID or multiple objects with an AND filter. If no filter is provided, all objects in the class are deleted, so use cautiously."
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
                       # safest: delete a single known object
                       objectId: "{{ outputs.previous.id }}"
                       # alternative: delete by AND filter on fields
                       # filter:
                       #   status: archived
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
        title = "Object ID to delete",
        description = "When set, deletes only this object and ignores any filter."
    )
    @PluginProperty(dynamic = true)
    private String objectId;

    @Schema(
        title = "Filter attributes for deletion",
        description = "Map of field name to value combined with AND; if omitted, a catch-all filter removes every object in the class."
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
            title = "Class name where objects were deleted"
        )
        private String className;

        @Schema(
            title = "Whether the delete operation succeeded"
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
