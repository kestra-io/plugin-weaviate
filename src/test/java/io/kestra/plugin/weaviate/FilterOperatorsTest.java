package io.kestra.plugin.weaviate;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.FilterCondition.Operator;
import io.kestra.plugin.weaviate.FilterCondition.ValueType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Every filter operator and group against a collection created with the index options they need.
 */
public class FilterOperatorsTest extends WeaviateTest {
    private static final Map<String, Object> BERLIN = Map.of("latitude", 52.52, "longitude", 13.40);
    private static final Map<String, Object> TOKYO = Map.of("latitude", 35.68, "longitude", 139.69);

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void everyOperatorAndGroup() throws Exception {
        RunContext runContext = runContextFactory.of();
        var before = Instant.now().minusSeconds(60);

        schemaCreate()
            .fields(
                Property.ofValue(
                    Map.of(
                        "name", Map.of("type", "text", "tokenization", "field"),
                        "tags", Map.of("type", "text[]", "tokenization", "field"),
                        "price", Map.of("type", "number", "indexRangeFilters", true),
                        "stock", "int",
                        "active", "boolean",
                        "location", "geoCoordinates",
                        "note", "text"
                    )
                )
            )
            .invertedIndex(
                Property.ofValue(
                    SchemaCreate.InvertedIndexOptions.builder().indexTimestamps(true).indexNullState(true).indexPropertyLength(true).build()
                )
            )
            .build()
            .run(runContext);

        batchCreate()
            .objects(
                List.of(
                    Map.of("name", "berlin-store", "tags", List.of("a", "b"), "price", 10.0, "stock", 5, "active", true, "location", BERLIN, "note", "x"),
                    Map.of(
                        "name", "paris-store", "tags", List.of("b", "c"), "price", 20.0, "stock", 0, "active", false, "location",
                        Map.of("latitude", 48.85, "longitude", 2.35)
                    ),
                    Map.of("name", "tokyo-store", "tags", List.of("c"), "price", 30.0, "stock", 7, "active", true, "location", TOKYO, "note", "long note here")
                )
            )
            .build()
            .run(runContext);

        assertThat(names(runContext, leaf("name", Operator.EQUAL, "paris-store")), is(Set.of("paris-store")));
        assertThat(names(runContext, leaf("active", Operator.NOT_EQUAL, true)), is(Set.of("paris-store")));
        assertThat(names(runContext, leaf("price", Operator.GREATER_THAN, 10)), is(Set.of("paris-store", "tokyo-store")));
        assertThat(names(runContext, leaf("price", Operator.GREATER_THAN_EQUAL, 20)), is(Set.of("paris-store", "tokyo-store")));
        assertThat(names(runContext, leaf("price", Operator.LESS_THAN, 20)), is(Set.of("berlin-store")));
        assertThat(names(runContext, leaf("price", Operator.LESS_THAN_EQUAL, 20)), is(Set.of("berlin-store", "paris-store")));
        assertThat(names(runContext, leaf("name", Operator.LIKE, "par*")), is(Set.of("paris-store")));
        assertThat(names(runContext, leaf("tags", Operator.CONTAINS_ANY, List.of("a", "c"))), is(Set.of("berlin-store", "paris-store", "tokyo-store")));
        assertThat(names(runContext, leaf("tags", Operator.CONTAINS_ALL, List.of("b", "c"))), is(Set.of("paris-store")));
        assertThat(names(runContext, leaf("tags", Operator.CONTAINS_NONE, List.of("a"))), is(Set.of("paris-store", "tokyo-store")));
        assertThat(names(runContext, leaf("note", Operator.IS_NULL, true)), is(Set.of("paris-store")));
        assertThat(names(runContext, leaf("note", Operator.IS_NULL, false)), is(Set.of("berlin-store", "tokyo-store")));
        assertThat(names(runContext, leaf("len(note)", Operator.GREATER_THAN, 5)), is(Set.of("tokyo-store")));
        assertThat(
            names(runContext, FilterCondition.builder().property("_creationTimeUnix").operator(Operator.GREATER_THAN_EQUAL).value(before.toString()).valueType(ValueType.DATE).build()),
            is(Set.of("berlin-store", "paris-store", "tokyo-store"))
        );

        // Paris is about 880 km from Berlin, Tokyo about 8900 km.
        assertThat(names(runContext, geo(BERLIN, 500_000)), is(Set.of("berlin-store")));
        assertThat(names(runContext, geo(BERLIN, 1_000_000)), is(Set.of("berlin-store", "paris-store")));

        assertThat(names(runContext, FilterCondition.builder().not(leaf("active", Operator.EQUAL, true)).build()), is(Set.of("paris-store")));
        var nested = FilterCondition.builder()
            .and(
                List.of(
                    leaf("price", Operator.GREATER_THAN_EQUAL, 10),
                    FilterCondition.builder().or(List.of(leaf("tags", Operator.CONTAINS_ANY, List.of("a")), geo(TOKYO, 100_000))).build()
                )
            )
            .build();
        assertThat(names(runContext, nested), is(Set.of("berlin-store", "tokyo-store")));
        var notOr = FilterCondition.builder()
            .not(FilterCondition.builder().or(List.of(leaf("stock", Operator.EQUAL, 0), leaf("name", Operator.EQUAL, "tokyo-store"))).build())
            .build();
        assertThat(names(runContext, notOr), is(Set.of("berlin-store")));
    }

    @Test
    public void filtersNeedTheirIndexes() throws Exception {
        RunContext runContext = runContextFactory.of();
        schemaCreate().fields(Property.ofValue(Map.of("name", "text", "note", "text"))).build().run(runContext);
        batchCreate().objects(List.of(Map.of("name", "a", "note", "x"))).build().run(runContext);

        var e = assertThrows(Exception.class, () -> names(runContext, leaf("note", Operator.IS_NULL, true)));
        assertThat(e.getMessage(), containsString("indexNullState"));
    }

    private static FilterCondition leaf(String property, Operator operator, Object value) {
        return FilterCondition.builder().property(property).operator(operator).value(value).build();
    }

    private static FilterCondition geo(Map<String, Object> point, int meters) {
        var value = new java.util.HashMap<>(point);
        value.put("distance", meters);
        return FilterCondition.builder().property("location").operator(Operator.WITHIN_GEO_RANGE).value(value).build();
    }

    @SuppressWarnings("unchecked")
    private Set<String> names(RunContext runContext, FilterCondition condition) throws Exception {
        FetchOutput output = query()
            .filters(Property.ofValue(List.of(condition)))
            .returnProperties(Property.ofValue(List.of("name")))
            .limit(Property.ofValue(100))
            .build()
            .run(runContext);
        return output.getRows().stream().map(r -> (String) properties((Map<String, Object>) r).get("name")).collect(Collectors.toSet());
    }
}
