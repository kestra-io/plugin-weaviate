package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.FilterCondition.Operator;
import io.kestra.plugin.weaviate.Query.MetadataField;
import io.kestra.plugin.weaviate.Query.SearchType;

import io.weaviate.client6.v1.api.collections.VectorConfig;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryTest extends WeaviateTest {
    private static final String CAT = "00000000-0000-0000-0000-000000000001";
    private static final String DOG = "00000000-0000-0000-0000-000000000002";
    private static final String CAR = "00000000-0000-0000-0000-000000000003";

    @Inject
    private RunContextFactory runContextFactory;

    /**
     * Three objects with 3-dimensional self-provided vectors: two animals close to each other, one vehicle far away.
     */
    private RunContext seed() throws Exception {
        createSelfProvidedCollection(
            io.weaviate.client6.v1.api.collections.Property.text("title"),
            io.weaviate.client6.v1.api.collections.Property.text("kind"),
            io.weaviate.client6.v1.api.collections.Property.integer("legs"),
            io.weaviate.client6.v1.api.collections.Property.date("seen"),
            io.weaviate.client6.v1.api.collections.Property.textArray("tags")
        );
        RunContext runContext = runContextFactory.of();
        batchCreate()
            .uuidKey(Property.ofValue("id"))
            .vectorKey(Property.ofValue("vector"))
            .objects(
                List.of(
                    Map.of(
                        "id", CAT, "title", "the cat sleeps", "kind", "animal", "legs", 4, "seen", "2024-01-01T00:00:00Z",
                        "tags", List.of("pet", "small"), "vector", List.of(1.0, 0.0, 0.0)
                    ),
                    Map.of(
                        "id", DOG, "title", "the dog barks at the cat", "kind", "animal", "legs", 4, "seen", "2024-06-01T00:00:00Z",
                        "tags", List.of("pet"), "vector", List.of(0.9, 0.1, 0.0)
                    ),
                    Map.of(
                        "id", CAR, "title", "a fast red car", "kind", "vehicle", "legs", 0, "seen", "2025-01-01T00:00:00Z",
                        "tags", List.of("machine"), "vector", List.of(0.0, 0.0, 1.0)
                    )
                )
            )
            .build()
            .run(runContext);
        return runContext;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> rows(FetchOutput output) {
        return output.getRows().stream().map(r -> (Map<String, Object>) r).toList();
    }

    private static List<Object> uuids(FetchOutput output) {
        return rows(output).stream().map(r -> r.get("uuid")).toList();
    }

    @Test
    public void fetchOneReturnsFirstRow() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query().fetchType(Property.ofValue(FetchType.FETCH_ONE)).build().run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getUri(), nullValue());
        assertThat(output.getRow().get("uuid"), is(in(List.of(CAT, DOG, CAR))));
        assertThat(output.getRow().get("properties"), instanceOf(Map.class));
        assertThat(output.getRow().get("metadata"), is(Map.of()));
    }

    @Test
    public void fetchReturnsAllRowsWithIsoDates() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query().build().run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRow(), nullValue());
        assertThat(uuids(output), containsInAnyOrder(CAT, DOG, CAR));
        Map<String, Object> cat = rows(output).stream().filter(r -> r.get("uuid").equals(CAT)).findFirst().orElseThrow();
        assertThat(properties(cat).get("seen"), is("2024-01-01T00:00:00Z"));
        assertThat(properties(cat).get("legs"), is(4L));
        assertThat(properties(cat).get("tags"), is(List.of("pet", "small")));
    }

    @Test
    public void storeWritesIon() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query().fetchType(Property.ofValue(FetchType.STORE)).build().run(runContext);

        assertThat(output.getSize(), is(3L));
        assertThat(output.getRows(), nullValue());
        assertThat(output.getRow(), nullValue());
        List<Map> stored = readObjectsFromStream(runContext.storage().getFile(output.getUri()));
        assertThat(stored.stream().map(r -> r.get("uuid")).toList(), containsInAnyOrder(CAT, DOG, CAR));
    }

    @Test
    public void noneReturnsNothing() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query().fetchType(Property.ofValue(FetchType.NONE)).build().run(runContext);

        assertThat(output.getSize(), nullValue());
        assertThat(output.getRows(), nullValue());
    }

    @Test
    public void limitOffsetAndReturnProperties() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query()
            .limit(Property.ofValue(2))
            .offset(Property.ofValue(1))
            .returnProperties(Property.ofValue(List.of("title")))
            .build()
            .run(runContext);

        assertThat(output.getSize(), is(2L));
        rows(output).forEach(r -> assertThat(properties(r).keySet(), is(java.util.Set.of("title"))));
    }

    @Test
    public void simpleAndStructuredFilters() throws Exception {
        RunContext runContext = seed();

        FetchOutput simple = query().filter(Property.ofValue(Map.of("kind", "anim*"))).build().run(runContext);
        assertThat(uuids(simple), containsInAnyOrder(CAT, DOG));

        FetchOutput or = query()
            .filters(
                Property.ofValue(
                    List.of(
                        FilterCondition.builder().property("title").operator(Operator.LIKE).value("*car*").build(),
                        FilterCondition.builder().property("tags").operator(Operator.CONTAINS_ANY).value(List.of("small")).build()
                    )
                )
            )
            .filtersOperator(Property.ofValue(FilterCondition.LogicalOperator.OR))
            .build()
            .run(runContext);
        assertThat(uuids(or), containsInAnyOrder(CAT, CAR));

        FetchOutput typed = query()
            .filters(
                Property.ofValue(
                    List.of(
                        FilterCondition.builder().property("seen").operator(Operator.GREATER_THAN).value("2024-03-01T00:00:00Z")
                            .valueType(FilterCondition.ValueType.DATE).build(),
                        FilterCondition.builder().property("legs").operator(Operator.GREATER_THAN).value(0).build()
                    )
                )
            )
            .build()
            .run(runContext);
        assertThat(uuids(typed), contains(DOG));

        FetchOutput byId = query()
            .filters(Property.ofValue(List.of(FilterCondition.builder().property("_id").operator(Operator.EQUAL).value(CAR).build())))
            .build()
            .run(runContext);
        assertThat(uuids(byId), contains(CAR));
    }

    @Test
    public void bm25RanksByScore() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query()
            .searchType(Property.ofValue(SearchType.BM25))
            .query(Property.ofValue("cat"))
            .queryProperties(Property.ofValue(List.of("title")))
            .returnMetadata(Property.ofValue(List.of(MetadataField.SCORE)))
            .build()
            .run(runContext);

        assertThat(uuids(output), contains(CAT, DOG));
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) rows(output).getFirst().get("metadata");
        assertThat(metadata, hasKey("score"));
    }

    @Test
    public void nearVectorReturnsDistance() throws Exception {
        RunContext runContext = seed();

        FetchOutput output = query()
            .searchType(Property.ofValue(SearchType.NEAR_VECTOR))
            .vector(Property.ofValue(List.of(1.0, 0.0, 0.0)))
            .distance(Property.ofValue(0.5))
            .returnMetadata(Property.ofValue(List.of(MetadataField.DISTANCE)))
            .build()
            .run(runContext);

        assertThat(uuids(output), contains(CAT, DOG));
        @SuppressWarnings("unchecked")
        Map<String, Object> metadata = (Map<String, Object>) rows(output).getFirst().get("metadata");
        assertThat(((Number) metadata.get("distance")).doubleValue(), closeTo(0.0, 0.001));
    }

    @Test
    public void hybridAlphaBalancesKeywordAndVector() throws Exception {
        RunContext runContext = seed();

        // Pure keyword: only objects mentioning "car" score.
        FetchOutput keyword = query()
            .searchType(Property.ofValue(SearchType.HYBRID))
            .query(Property.ofValue("car"))
            .vector(Property.ofValue(List.of(1.0, 0.0, 0.0)))
            .alpha(Property.ofValue(0.0))
            .limit(Property.ofValue(1))
            .build()
            .run(runContext);
        assertThat(uuids(keyword), contains(CAR));

        // Pure vector: the object closest to the vector wins regardless of text.
        FetchOutput vector = query()
            .searchType(Property.ofValue(SearchType.HYBRID))
            .query(Property.ofValue("car"))
            .vector(Property.ofValue(List.of(1.0, 0.0, 0.0)))
            .alpha(Property.ofValue(1.0))
            .limit(Property.ofValue(1))
            .build()
            .run(runContext);
        assertThat(uuids(vector), contains(CAT));
    }

    @Test
    public void nearTextUsesCollectionVectorizer() throws Exception {
        createCollection(
            c -> c
                .properties(io.weaviate.client6.v1.api.collections.Property.text("title"))
                .vectorConfig(VectorConfig.text2vecModel2Vec())
        );
        RunContext runContext = runContextFactory.of();
        batchCreate()
            .objects(
                List.of(
                    Map.of("title", "A kitten is a young cat"),
                    Map.of("title", "Stock markets fell sharply today"),
                    Map.of("title", "The recipe needs flour and sugar")
                )
            )
            .build()
            .run(runContext);

        FetchOutput output = query()
            .searchType(Property.ofValue(SearchType.NEAR_TEXT))
            .query(Property.ofValue("feline pets"))
            .limit(Property.ofValue(1))
            .returnMetadata(Property.ofValue(List.of(MetadataField.DISTANCE)))
            .build()
            .run(runContext);

        assertThat(properties(rows(output).getFirst()).get("title"), is("A kitten is a young cat"));
    }

    @Test
    public void namedVectorsWithTargetVector() throws Exception {
        createCollection(
            c -> c
                .properties(io.weaviate.client6.v1.api.collections.Property.text("title"))
                .vectorConfig(VectorConfig.selfProvided("title_vec"), VectorConfig.selfProvided("other_vec"))
        );
        RunContext runContext = runContextFactory.of();
        batchCreate()
            .uuidKey(Property.ofValue("id"))
            .vectorKey(Property.ofValue("vector"))
            .targetVector(Property.ofValue("title_vec"))
            .objects(
                List.of(
                    Map.of("id", CAT, "title", "cat", "vector", List.of(1.0, 0.0)),
                    Map.of("id", CAR, "title", "car", "vector", List.of(0.0, 1.0))
                )
            )
            .build()
            .run(runContext);

        FetchOutput output = query()
            .searchType(Property.ofValue(SearchType.NEAR_VECTOR))
            .vector(Property.ofValue(List.of(0.0, 1.0)))
            .targetVector(Property.ofValue("title_vec"))
            .includeVector(Property.ofValue(true))
            .limit(Property.ofValue(1))
            .build()
            .run(runContext);

        Map<String, Object> row = rows(output).getFirst();
        assertThat(row.get("uuid"), is(CAR));
        assertThat(row.get("vectors"), is(Map.of("title_vec", List.of(0.0f, 1.0f))));

        FetchOutput fetched = query()
            .searchType(Property.ofValue(SearchType.FETCH_OBJECTS))
            .filters(Property.ofValue(List.of(FilterCondition.builder().property("_id").operator(FilterCondition.Operator.EQUAL).value(CAT).build())))
            .targetVector(Property.ofValue("title_vec"))
            .includeVector(Property.ofValue(true))
            .build()
            .run(runContext);

        assertThat(rows(fetched).getFirst().get("vectors"), is(Map.of("title_vec", List.of(1.0f, 0.0f))));
    }

    @Test
    public void validationFailsBeforeConnecting() {
        Query task = Query.builder()
            .url("http://localhost:1")
            .className(Property.ofValue(CLASS_NAME))
            .searchType(Property.ofValue(SearchType.NEAR_VECTOR))
            .build();

        var e = assertThrows(IllegalArgumentException.class, () -> task.run(runContextFactory.of()));
        assertThat(e.getMessage(), containsString("`vector` is required"));
    }
}
