package io.kestra.plugin.weaviate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.data.DeleteManyResponse;
import io.weaviate.client6.v1.api.collections.query.Filter;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete objects from a Weaviate collection",
    description = """
        Deletes a single object by ID, or every object matching `filter` and/or `filters`.
        Deleting a whole collection's objects requires `deleteAll: true`, so an empty filter can never wipe a collection by accident."""
)
@Plugin(
    examples = {
        @Example(
            title = "Delete one object by ID.",
            full = true,
            code = """
                id: weaviate_delete_by_id
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.weaviate.Delete
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: WeaviateObject
                    objectId: "{{ outputs.previous.row.uuid }}"
                """
        ),
        @Example(
            title = "Delete archived objects older than a date.",
            full = true,
            code = """
                id: weaviate_delete_by_filter
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.weaviate.Delete
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: WeaviateObject
                    filters:
                      - property: status
                        operator: EQUAL
                        value: archived
                      - property: updatedAt
                        operator: LESS_THAN
                        value: "2025-01-01T00:00:00Z"
                        valueType: DATE
                """
        )
    }
)
public class Delete extends WeaviateConnection implements RunnableTask<Delete.Output> {
    static final String NIL_UUID = "00000000-0000-0000-0000-000000000000";
    static final int MAX_RETURNED_IDS = 10_000;
    private static final int MAX_DELETE_ROUNDS = 10_000;

    @Schema(title = "Collection (class) to delete from")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> className;

    @Schema(
        title = "Object ID to delete",
        description = "When set, deletes only this object; `filter`, `filters` and `deleteAll` must not be set."
    )
    @PluginProperty(group = "main")
    private Property<String> objectId;

    @Schema(
        title = "Simple property filter",
        description = "Map of property to value, combined with AND. Text values use LIKE (so `*` wildcards work); other values use EQUAL."
    )
    @PluginProperty(group = "processing")
    private Property<Map<String, Object>> filter;

    @Schema(
        title = "Structured property filters",
        description = "List of conditions combined with `filtersOperator`. When `filter` is also set, both must match."
    )
    @PluginProperty(group = "processing")
    private Property<List<FilterCondition>> filters;

    @Schema(title = "How to combine `filters`")
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<FilterCondition.LogicalOperator> filtersOperator = Property.ofValue(FilterCondition.LogicalOperator.AND);

    @Schema(
        title = "Delete every object in the collection",
        description = "Required to delete without `objectId` or filters. The collection and its schema are kept."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> deleteAll = Property.ofValue(false);

    @Schema(
        title = "Fail when Weaviate rejects a deletion",
        description = """
            When false, the task succeeds and reports rejected deletions through `success: false` and a warning.
            Deleting an `objectId` that does not exist is not a rejection: the task succeeds with `deletedCount: 0`."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> failOnError = Property.ofValue(true);

    @Schema(title = "Tenant", description = "Tenant to delete from, for multi-tenant collections.")
    @PluginProperty(group = "advanced")
    private Property<String> tenant;

    @Override
    public Delete.Output run(RunContext runContext) throws Exception {
        var rClassName = runContext.render(className).as(String.class).orElseThrow(() -> new IllegalArgumentException("`className` is required."));
        var rObjectId = runContext.render(objectId).as(String.class).filter(s -> !s.isBlank()).orElse(null);
        boolean rDeleteAll = runContext.render(deleteAll).as(Boolean.class).orElse(false);
        boolean rFailOnError = runContext.render(failOnError).as(Boolean.class).orElse(true);
        var rTenant = runContext.render(tenant).as(String.class).orElse(null);
        var rFilter = WeaviateFilters.build(
            runContext.render(filter).asMap(String.class, Object.class),
            runContext.render(filters).asList(FilterCondition.class),
            runContext.render(filtersOperator).as(FilterCondition.LogicalOperator.class).orElse(FilterCondition.LogicalOperator.AND)
        );

        var deleteFilter = resolveFilter(rObjectId, rFilter, rDeleteAll);

        try (WeaviateClient client = connect(runContext)) {
            var collection = client.collections.use(rClassName);
            if (rTenant != null) {
                collection = collection.withTenant(rTenant);
            }

            if (rObjectId != null) {
                // The server answers 204 whether or not the object existed, so check first to report an accurate count.
                boolean deleted = collection.data.exists(rObjectId) && collection.data.deleteById(rObjectId);
                runContext.metric(Counter.of("deleted", deleted ? 1 : 0));
                return Output.builder()
                    .className(rClassName)
                    .success(deleted)
                    .deletedCount(deleted ? 1 : 0)
                    .ids(deleted ? List.of(rObjectId) : List.of())
                    .idsTruncated(false)
                    .build();
            }

            // Each call deletes at most QUERY_MAXIMUM_RESULTS objects, so repeat until nothing matches.
            long deleted = 0;
            long failed = 0;
            List<String> ids = new ArrayList<>();
            boolean truncated = false;
            for (int round = 0; round < MAX_DELETE_ROUNDS; round++) {
                var response = collection.data.deleteMany(deleteFilter, o -> o.verbose(true));
                deleted += response.successful();
                failed += response.failed();
                if (response.objects() != null) {
                    for (DeleteManyResponse.DeletedObject object : response.objects()) {
                        if (object.successful()) {
                            if (ids.size() < MAX_RETURNED_IDS) {
                                ids.add(object.uuid());
                            } else {
                                truncated = true;
                            }
                        }
                    }
                }
                if (response.matches() == 0 || response.successful() == 0) {
                    break;
                }
            }

            runContext.metric(Counter.of("deleted", deleted));
            if (failed > 0) {
                if (rFailOnError) {
                    throw new IllegalStateException(
                        "Weaviate rejected " + failed + " deletions from '" + rClassName + "' (" + deleted + " deleted). Set `failOnError: false` to continue anyway."
                    );
                }
                runContext.logger().warn("Deleted {} objects from '{}', {} deletions were rejected", deleted, rClassName, failed);
            } else {
                runContext.logger().info("Deleted {} objects from '{}'", deleted, rClassName);
            }

            return Output.builder()
                .className(rClassName)
                .success(failed == 0)
                .deletedCount(deleted)
                .ids(ids)
                .idsTruncated(truncated)
                .build();
        }
    }

    static Filter resolveFilter(String objectId, Filter filter, boolean deleteAll) {
        if (objectId != null) {
            if (filter != null || deleteAll) {
                throw new IllegalArgumentException("`objectId` cannot be combined with `filter`, `filters` or `deleteAll`.");
            }
            return null;
        }
        if (filter != null) {
            if (deleteAll) {
                throw new IllegalArgumentException("`deleteAll` cannot be combined with `filter` or `filters`.");
            }
            return filter;
        }
        if (!deleteAll) {
            throw new IllegalArgumentException(
                "Set `objectId`, `filter` or `filters` to choose what to delete, or set `deleteAll: true` to delete every object in the collection."
            );
        }
        return Filter.uuid().ne(NIL_UUID);
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Collection where objects were deleted")
        private String className;

        @Schema(title = "Whether every matching object was deleted")
        private Boolean success;

        @Schema(title = "Number of deleted objects")
        private long deletedCount;

        @Schema(title = "IDs of deleted objects", description = "Capped at " + MAX_RETURNED_IDS + " entries; see `idsTruncated`.")
        private List<String> ids;

        @Schema(title = "Whether `ids` was capped")
        private boolean idsTruncated;
    }
}
