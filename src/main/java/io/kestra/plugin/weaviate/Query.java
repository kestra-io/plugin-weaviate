package io.kestra.plugin.weaviate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.query.BaseQueryOptions;
import io.weaviate.client6.v1.api.collections.query.Filter;
import io.weaviate.client6.v1.api.collections.query.Metadata;
import io.weaviate.client6.v1.api.collections.query.NearVector;
import io.weaviate.client6.v1.api.collections.query.QueryResponse;
import io.weaviate.client6.v1.api.collections.query.Target;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Search objects in a Weaviate collection",
    description = "Runs a fetch, BM25, hybrid, near-text or near-vector search over gRPC and returns matching objects. " +
        "Each row is `{uuid, properties, metadata, vector?, vectors?}`. Defaults to storing all rows to Kestra storage as ION; " +
        "other fetch modes return rows inline. Requires Weaviate 1.32 or later."
)
@Plugin(
    examples = {
        @Example(
            title = "Fetch objects with a filter and return them inline.",
            full = true,
            code = """
                id: weaviate_fetch
                namespace: company.team

                tasks:
                  - id: fetch
                    type: io.kestra.plugin.weaviate.Query
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Question
                    fetchType: FETCH
                    limit: 5
                    returnProperties:
                      - question
                      - answer
                    filters:
                      - property: category
                        operator: EQUAL
                        value: SCIENCE
                """
        ),
        @Example(
            title = "Keyword (BM25) search, boosting matches in the question property.",
            full = true,
            code = """
                id: weaviate_bm25
                namespace: company.team

                tasks:
                  - id: bm25
                    type: io.kestra.plugin.weaviate.Query
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Question
                    searchType: BM25
                    query: animal
                    queryProperties:
                      - question^2
                      - answer
                    returnMetadata:
                      - SCORE
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Semantic search with a server-side vectorizer, passing the vectorizer API key as a header.",
            full = true,
            code = """
                id: weaviate_near_text
                namespace: company.team

                tasks:
                  - id: near_text
                    type: io.kestra.plugin.weaviate.Query
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    headers:
                      X-OpenAI-Api-Key: "{{ secret('OPENAI_API_KEY') }}"
                    className: Question
                    searchType: NEAR_TEXT
                    query: biology
                    limit: 5
                    returnMetadata:
                      - DISTANCE
                    fetchType: FETCH
                """
        ),
        @Example(
            title = "Hybrid search that blends keyword and vector scores, then stores all rows as ION.",
            full = true,
            code = """
                id: weaviate_hybrid
                namespace: company.team

                tasks:
                  - id: hybrid
                    type: io.kestra.plugin.weaviate.Query
                    url: http://localhost:8080
                    className: Question
                    searchType: HYBRID
                    query: large animals
                    alpha: 0.75
                    limit: 20
                """
        ),
        @Example(
            title = "Similarity search with a vector computed upstream.",
            full = true,
            code = """
                id: weaviate_near_vector
                namespace: company.team

                inputs:
                  - id: embedding
                    type: ARRAY
                    itemType: FLOAT

                tasks:
                  - id: near_vector
                    type: io.kestra.plugin.weaviate.Query
                    url: http://localhost:8080
                    className: Question
                    searchType: NEAR_VECTOR
                    vector: "{{ inputs.embedding }}"
                    distance: 0.4
                    includeVector: true
                    fetchType: FETCH
                """
        )
    }
)
public class Query extends WeaviateConnection implements RunnableTask<FetchOutput> {

    @Schema(title = "Collection (class) to search")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> className;

    @Schema(
        title = "Search type",
        description = "FETCH_OBJECTS lists objects without ranking. BM25 is keyword search on `query`. HYBRID blends BM25 and vector search on `query` " +
            "(and optionally `vector`). NEAR_TEXT vectorizes `query` with the collection's vectorizer. NEAR_VECTOR searches by `vector`."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "main")
    private Property<SearchType> searchType = Property.ofValue(SearchType.FETCH_OBJECTS);

    @Schema(title = "Search text", description = "Required for BM25, HYBRID and NEAR_TEXT.")
    @PluginProperty(group = "main")
    private Property<String> query;

    @Schema(
        title = "Query vector",
        description = "Required for NEAR_VECTOR. Optional for HYBRID, where it replaces vectorizing `query`."
    )
    @PluginProperty(group = "main")
    private Property<List<Double>> vector;

    @Schema(
        title = "Hybrid balance",
        description = "HYBRID only. 0 is pure keyword search, 1 is pure vector search. Defaults to the server default (0.7)."
    )
    @PluginProperty(group = "processing")
    private Property<Double> alpha;

    @Schema(
        title = "Named vector to search",
        description = "For collections with named vectors: the vector used by NEAR_TEXT, NEAR_VECTOR and HYBRID."
    )
    @PluginProperty(group = "processing")
    private Property<String> targetVector;

    @Schema(
        title = "Properties to search for BM25 and HYBRID",
        description = "Restricts keyword search to these properties. Append `^N` to boost a property, e.g. `title^2`."
    )
    @PluginProperty(group = "processing")
    private Property<List<String>> queryProperties;

    @Schema(
        title = "Maximum number of objects to return",
        description = "Defaults to the server's `QUERY_DEFAULTS_LIMIT`, so FETCH_OBJECTS without `limit` does not return every object. " +
            "Capped by the server's `QUERY_MAXIMUM_RESULTS`."
    )
    @PluginProperty(group = "processing")
    private Property<Integer> limit;

    @Schema(title = "Number of objects to skip")
    @PluginProperty(group = "processing")
    private Property<Integer> offset;

    @Schema(title = "Maximum vector distance", description = "NEAR_TEXT and NEAR_VECTOR only. Cannot be combined with `certainty`.")
    @PluginProperty(group = "processing")
    private Property<Double> distance;

    @Schema(title = "Minimum certainty", description = "NEAR_TEXT and NEAR_VECTOR only, between 0 and 1. Cannot be combined with `distance`.")
    @PluginProperty(group = "processing")
    private Property<Double> certainty;

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

    @Schema(title = "Properties to return", description = "Defaults to all non-reference properties.")
    @PluginProperty(group = "processing")
    private Property<List<String>> returnProperties;

    @Schema(title = "Metadata to return", description = "Returned under each row's `metadata`.")
    @PluginProperty(group = "processing")
    private Property<List<MetadataField>> returnMetadata;

    @Schema(title = "Return object vectors", description = "Adds `vector` (default vector) and `vectors` (named vectors) to each row.")
    @Builder.Default
    @PluginProperty(group = "processing")
    private Property<Boolean> includeVector = Property.ofValue(false);

    @Schema(title = "Tenant", description = "Tenant to search in, for multi-tenant collections.")
    @PluginProperty(group = "advanced")
    private Property<String> tenant;

    @Schema(
        title = "Select fetch behavior",
        description = "Defaults to STORE (writes all rows to Kestra storage as ION and returns the URI). FETCH returns all rows inline, FETCH_ONE returns the first row, NONE skips result materialization."
    )
    @NotNull
    @Builder.Default
    @PluginProperty(group = "processing")
    protected Property<FetchType> fetchType = Property.ofValue(FetchType.STORE);

    @Override
    public FetchOutput run(RunContext runContext) throws Exception {
        Search search = Search.builder()
            .className(runContext.render(className).as(String.class).orElseThrow())
            .searchType(runContext.render(searchType).as(SearchType.class).orElse(SearchType.FETCH_OBJECTS))
            .query(runContext.render(query).as(String.class).orElse(null))
            .vector(runContext.render(vector).asList(Double.class))
            .alpha(runContext.render(alpha).as(Double.class).orElse(null))
            .targetVector(runContext.render(targetVector).as(String.class).orElse(null))
            .queryProperties(runContext.render(queryProperties).asList(String.class))
            .limit(runContext.render(limit).as(Integer.class).orElse(null))
            .offset(runContext.render(offset).as(Integer.class).orElse(null))
            .distance(runContext.render(distance).as(Double.class).orElse(null))
            .certainty(runContext.render(certainty).as(Double.class).orElse(null))
            .returnProperties(runContext.render(returnProperties).asList(String.class))
            .returnMetadata(runContext.render(returnMetadata).asList(MetadataField.class))
            .includeVector(runContext.render(includeVector).as(Boolean.class).orElse(false))
            .filter(
                WeaviateFilters.build(
                    runContext.render(filter).asMap(String.class, Object.class),
                    runContext.render(filters).asList(FilterCondition.class),
                    runContext.render(filtersOperator).as(FilterCondition.LogicalOperator.class).orElse(FilterCondition.LogicalOperator.AND)
                )
            )
            .build();
        search.validate();

        String renderedTenant = runContext.render(tenant).as(String.class).orElse(null);
        FetchType renderedFetchType = runContext.render(fetchType).as(FetchType.class).orElseThrow();

        List<Map<String, Object>> rows;
        try (WeaviateClient client = connect(runContext)) {
            CollectionHandle<Map<String, Object>> collection = client.collections.use(search.className());
            if (renderedTenant != null) {
                collection = collection.withTenant(renderedTenant);
            }
            rows = search.execute(collection).objects().stream()
                .map(WeaviateRows::toRow)
                .toList();
        }

        runContext.metric(Counter.of("records", rows.size()));
        runContext.logger().debug("{} search on '{}' returned {} objects", search.searchType(), search.className(), rows.size());

        FetchOutput.FetchOutputBuilder output = FetchOutput.builder();
        return switch (renderedFetchType) {
            case FETCH_ONE -> output.size(rows.isEmpty() ? 0L : 1L).row(rows.isEmpty() ? null : rows.getFirst()).build();
            case FETCH -> output.size((long) rows.size()).rows(new ArrayList<>(rows)).build();
            case STORE -> output.size((long) rows.size()).uri(store(rows, runContext)).build();
            default -> output.build();
        };
    }

    private URI store(List<Map<String, Object>> rows, RunContext runContext) throws IOException {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new FileOutputStream(tempFile)) {
            FileSerde.writeAll(output, Flux.fromIterable(rows)).block();
            output.flush();
        }
        return runContext.storage().putFile(tempFile);
    }

    public enum SearchType {
        FETCH_OBJECTS,
        BM25,
        HYBRID,
        NEAR_TEXT,
        NEAR_VECTOR
    }

    public enum MetadataField {
        DISTANCE(Metadata.DISTANCE),
        CERTAINTY(Metadata.CERTAINTY),
        SCORE(Metadata.SCORE),
        EXPLAIN_SCORE(Metadata.EXPLAIN_SCORE),
        CREATION_TIME(Metadata.CREATION_TIME_UNIX),
        LAST_UPDATE_TIME(Metadata.LAST_UPDATE_TIME_UNIX);

        private final Metadata metadata;

        MetadataField(Metadata metadata) {
            this.metadata = metadata;
        }
    }

    /**
     * Rendered search parameters. Kept separate from the task so validation and dispatch can be unit-tested
     * without a server.
     */
    @Builder
    record Search(
        String className,
        SearchType searchType,
        String query,
        List<Double> vector,
        Double alpha,
        String targetVector,
        List<String> queryProperties,
        Integer limit,
        Integer offset,
        Double distance,
        Double certainty,
        List<String> returnProperties,
        List<MetadataField> returnMetadata,
        boolean includeVector,
        Filter filter) {
        /**
         * Rejects parameter combinations that the chosen search type ignores or needs, before any network call.
         */
        void validate() {
            if (className == null || className.isBlank()) {
                throw new IllegalArgumentException("`className` is required.");
            }
            boolean hasQuery = query != null && !query.isBlank();
            boolean hasVector = vector != null && !vector.isEmpty();
            boolean hasQueryProperties = queryProperties != null && !queryProperties.isEmpty();

            switch (searchType) {
                case FETCH_OBJECTS -> {
                    reject(hasQuery, "query");
                    reject(hasVector, "vector");
                    reject(alpha != null, "alpha");
                    reject(distance != null, "distance");
                    reject(certainty != null, "certainty");
                    reject(hasQueryProperties, "queryProperties");
                    reject(targetVector != null, "targetVector");
                }
                case BM25 -> {
                    require(hasQuery, "query");
                    reject(hasVector, "vector");
                    reject(alpha != null, "alpha");
                    reject(distance != null, "distance");
                    reject(certainty != null, "certainty");
                    reject(targetVector != null, "targetVector");
                }
                case HYBRID -> {
                    require(hasQuery, "query");
                    reject(distance != null, "distance");
                    reject(certainty != null, "certainty");
                    if (alpha != null && (alpha < 0 || alpha > 1)) {
                        throw new IllegalArgumentException("`alpha` must be between 0 and 1, got " + alpha + ".");
                    }
                }
                case NEAR_TEXT -> {
                    require(hasQuery, "query");
                    reject(hasVector, "vector");
                    reject(alpha != null, "alpha");
                    reject(hasQueryProperties, "queryProperties");
                }
                case NEAR_VECTOR -> {
                    require(hasVector, "vector");
                    reject(hasQuery, "query");
                    reject(alpha != null, "alpha");
                    reject(hasQueryProperties, "queryProperties");
                }
            }

            if (distance != null && certainty != null) {
                throw new IllegalArgumentException("`distance` and `certainty` cannot be used together.");
            }
            if (certainty != null && (certainty < 0 || certainty > 1)) {
                throw new IllegalArgumentException("`certainty` must be between 0 and 1, got " + certainty + ".");
            }
            if (limit != null && limit <= 0) {
                throw new IllegalArgumentException("`limit` must be greater than 0, got " + limit + ".");
            }
            if (offset != null && offset < 0) {
                throw new IllegalArgumentException("`offset` must not be negative, got " + offset + ".");
            }
        }

        private void require(boolean present, String property) {
            if (!present) {
                throw new IllegalArgumentException("`" + property + "` is required for searchType " + searchType + ".");
            }
        }

        private void reject(boolean present, String property) {
            if (present) {
                throw new IllegalArgumentException("`" + property + "` is not supported for searchType " + searchType + ".");
            }
        }

        QueryResponse<Map<String, Object>> execute(CollectionHandle<Map<String, Object>> collection) {
            var q = collection.query;
            return switch (searchType) {
                case FETCH_OBJECTS -> q.fetchObjects(b -> common(b));
                case BM25 -> q.bm25(query, b ->
                {
                    if (queryProperties != null && !queryProperties.isEmpty()) {
                        b.queryProperties(queryProperties);
                    }
                    return common(b);
                });
                case HYBRID -> q.hybrid(
                    targetVector != null && (vector == null || vector.isEmpty()) ? Target.text(targetVector, query) : Target.text(List.of(query)),
                    b ->
                    {
                        if (alpha != null) {
                            b.alpha(alpha.floatValue());
                        }
                        if (queryProperties != null && !queryProperties.isEmpty()) {
                            b.queryProperties(queryProperties);
                        }
                        if (vector != null && !vector.isEmpty()) {
                            b.nearVector(
                                targetVector != null
                                    ? NearVector.of(Target.vector(targetVector, toFloats(vector)))
                                    : NearVector.of(toFloats(vector))
                            );
                        }
                        return common(b);
                    }
                );
                case NEAR_TEXT -> q.nearText(
                    targetVector != null ? Target.text(targetVector, query) : Target.text(List.of(query)),
                    b ->
                    {
                        if (distance != null) {
                            b.distance(distance.floatValue());
                        }
                        if (certainty != null) {
                            b.certainty(certainty.floatValue());
                        }
                        return common(b);
                    }
                );
                case NEAR_VECTOR -> q.nearVector(
                    targetVector != null ? Target.vector(targetVector, toFloats(vector)) : Target.vector(toFloats(vector)),
                    b ->
                    {
                        if (distance != null) {
                            b.distance(distance.floatValue());
                        }
                        if (certainty != null) {
                            b.certainty(certainty.floatValue());
                        }
                        return common(b);
                    }
                );
            };
        }

        private <B extends BaseQueryOptions.Builder<B, ?>> B common(B builder) {
            if (limit != null) {
                builder.limit(limit);
            }
            if (offset != null) {
                builder.offset(offset);
            }
            if (filter != null) {
                builder.filters(filter);
            }
            if (returnProperties != null && !returnProperties.isEmpty()) {
                builder.returnProperties(returnProperties);
            }
            if (returnMetadata != null && !returnMetadata.isEmpty()) {
                builder.returnMetadata(returnMetadata.stream().map(m -> m.metadata).toList());
            }
            if (includeVector) {
                if (targetVector != null) {
                    builder.includeVector(targetVector);
                } else {
                    builder.includeVector();
                }
            }
            return builder;
        }

        static float[] toFloats(List<Double> values) {
            float[] result = new float[values.size()];
            for (int i = 0; i < values.size(); i++) {
                result[i] = values.get(i).floatValue();
            }
            return result;
        }
    }
}
