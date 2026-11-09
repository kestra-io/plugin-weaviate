package io.kestra.plugin.weaviate;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.plugin.weaviate.FilterCondition.LogicalOperator;
import io.kestra.plugin.weaviate.FilterCondition.Operator;
import io.kestra.plugin.weaviate.FilterCondition.ValueType;

import io.weaviate.client6.v1.api.collections.query.Filter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WeaviateFiltersTest {
    private static FilterCondition condition(String property, Operator operator, Object value) {
        return FilterCondition.builder().property(property).operator(operator).value(value).build();
    }

    private static FilterCondition condition(String property, Operator operator, Object value, ValueType type) {
        return FilterCondition.builder().property(property).operator(operator).value(value).valueType(type).build();
    }

    private static String build(FilterCondition... conditions) {
        return WeaviateFilters.build(null, List.of(conditions), LogicalOperator.AND).toString();
    }

    @Test
    void returnsNullWithoutConditions() {
        assertThat(WeaviateFilters.build(null, null, null), nullValue());
        assertThat(WeaviateFilters.build(Map.of(), List.of(), LogicalOperator.OR), nullValue());
    }

    @Test
    void simpleMapUsesLikeForTextAndEqualOtherwise() {
        Map<String, Object> simple = new LinkedHashMap<>();
        simple.put("title", "succ*");
        simple.put("length", 10);
        simple.put("ratio", 0.5);
        simple.put("bool", true);

        String filter = WeaviateFilters.build(simple, null, null).toString();

        assertThat(filter, is("Filter(Filter(title Like succ*) And Filter(length Equal 10) And Filter(ratio Equal 0.5) And Filter(bool Equal true))"));
    }

    @Test
    void simpleMapDatesUseEqual() {
        String filter = WeaviateFilters.build(Map.of("createdOn", Instant.parse("2024-01-02T03:04:05Z")), null, null).toString();

        assertThat(filter, is("Filter(createdOn Equal 2024-01-02T03:04:05Z)"));
    }

    @Test
    void singleSimpleEntryIsNotWrapped() {
        assertThat(WeaviateFilters.build(Map.of("title", "a"), null, null).toString(), is("Filter(title Like a)"));
    }

    @Test
    void comparisonOperators() {
        assertThat(build(condition("n", Operator.EQUAL, 1)), is("Filter(n Equal 1)"));
        assertThat(build(condition("n", Operator.NOT_EQUAL, 1)), is("Filter(n NotEqual 1)"));
        assertThat(build(condition("n", Operator.GREATER_THAN, 1)), is("Filter(n GreaterThan 1)"));
        assertThat(build(condition("n", Operator.GREATER_THAN_EQUAL, 1.5)), is("Filter(n GreaterThanEqual 1.5)"));
        assertThat(build(condition("n", Operator.LESS_THAN, 1)), is("Filter(n LessThan 1)"));
        assertThat(build(condition("n", Operator.LESS_THAN_EQUAL, "b")), is("Filter(n LessThenEqual b)"));
        assertThat(build(condition("t", Operator.LIKE, "*x*")), is("Filter(t Like *x*)"));
    }

    @Test
    void containsOperatorsByElementType() {
        assertThat(build(condition("tags", Operator.CONTAINS_ANY, List.of("a", "b"))), is("Filter(tags ContainsAny [a, b])"));
        assertThat(build(condition("nums", Operator.CONTAINS_ALL, List.of(1, 2))), is("Filter(nums ContainsAll [1, 2])"));
        assertThat(build(condition("nums", Operator.CONTAINS_NONE, List.of(1.5, 2.5))), is("Filter(nums ContainsNone [1.5, 2.5])"));
        assertThat(build(condition("flags", Operator.CONTAINS_ANY, List.of(true))), is("Filter(flags ContainsAny [true])"));
        assertThat(build(condition("tags", Operator.CONTAINS_ANY, "single")), is("Filter(tags ContainsAny [single])"));
    }

    @Test
    void isNullDefaultsToTrue() {
        assertThat(build(condition("t", Operator.IS_NULL, null)), is("Filter(t IsNull true)"));
        assertThat(build(condition("t", Operator.IS_NULL, false)), is("Filter(t IsNull false)"));
        assertThat(build(condition("t", Operator.IS_NULL, "false")), is("Filter(t IsNull false)"));
    }

    @Test
    void valueTypeCoercion() {
        assertThat(build(condition("d", Operator.GREATER_THAN, "2024-01-01", ValueType.DATE)), is("Filter(d GreaterThan 2024-01-01T00:00:00Z)"));
        assertThat(build(condition("d", Operator.EQUAL, "2024-01-01T10:00:00+02:00", ValueType.DATE)), is("Filter(d Equal 2024-01-01T10:00:00+02:00)"));
        assertThat(build(condition("n", Operator.EQUAL, 3, ValueType.NUMBER)), is("Filter(n Equal 3.0)"));
        assertThat(build(condition("n", Operator.EQUAL, "7", ValueType.INT)), is("Filter(n Equal 7)"));
        assertThat(build(condition("s", Operator.EQUAL, 42, ValueType.TEXT)), is("Filter(s Equal 42)"));
        assertThat(build(condition("b", Operator.EQUAL, "true", ValueType.BOOLEAN)), is("Filter(b Equal true)"));
        assertThat(
            build(condition("ref", Operator.EQUAL, "0B9E8A4A-3B1C-4C1E-9F7A-1C2D3E4F5A6B", ValueType.UUID)),
            is("Filter(ref Equal 0b9e8a4a-3b1c-4c1e-9f7a-1c2d3e4f5a6b)")
        );
        assertThat(build(condition("tags", Operator.CONTAINS_ANY, List.of("1", "2"), ValueType.INT)), is("Filter(tags ContainsAny [1, 2])"));
    }

    @Test
    void datesAlwaysIncludeSeconds() {
        // The v6 client's OffsetDateTime.toString() would send 2024-01-01T00:00Z, which the server rejects.
        assertThat(
            build(condition("d", Operator.CONTAINS_ANY, List.of("2024-01-01T00:00:00Z"), ValueType.DATE)),
            is("Filter(d ContainsAny [2024-01-01T00:00:00Z])")
        );
        assertThat(WeaviateFilters.build(Map.of("d", Instant.parse("2024-01-01T00:00:00Z")), null, null).toString(), is("Filter(d Equal 2024-01-01T00:00:00Z)"));
    }

    @Test
    void specialProperties() {
        String id = "0b9e8a4a-3b1c-4c1e-9f7a-1c2d3e4f5a6b";
        assertThat(build(condition("_id", Operator.EQUAL, id)), is("Filter(_id Equal " + id + ")"));
        assertThat(build(condition("_id", Operator.CONTAINS_ANY, List.of(id))), is("Filter(_id ContainsAny [" + id + "])"));
        assertThat(
            build(condition("_creationTimeUnix", Operator.GREATER_THAN, "2024-01-01T00:00:00Z")),
            is("Filter(_creationTimeUnix GreaterThan 2024-01-01T00:00:00Z)")
        );
        assertThat(
            build(condition("_lastUpdateTimeUnix", Operator.LESS_THAN, "2024-01-01T00:00:00Z")),
            is("Filter(_lastUpdateTimeUnix LessThan 2024-01-01T00:00:00Z)")
        );
    }

    @Test
    void combinesConditionsWithOperator() {
        List<FilterCondition> conditions = List.of(condition("a", Operator.EQUAL, 1), condition("b", Operator.EQUAL, 2));

        assertThat(WeaviateFilters.build(null, conditions, LogicalOperator.OR).toString(), is("Filter(Filter(a Equal 1) Or Filter(b Equal 2))"));
        assertThat(WeaviateFilters.build(null, conditions, null).toString(), is("Filter(Filter(a Equal 1) And Filter(b Equal 2))"));
    }

    @Test
    void combinesSimpleAndStructuredWithAnd() {
        Filter filter = WeaviateFilters.build(
            Map.of("title", "x"),
            List.of(condition("a", Operator.EQUAL, 1), condition("b", Operator.EQUAL, 2)),
            LogicalOperator.OR
        );

        assertThat(filter.toString(), is("Filter(Filter(title Like x) And Filter(Filter(a Equal 1) Or Filter(b Equal 2)))"));
    }

    @Test
    void errorsNameTheConditionIndex() {
        var e = assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.EQUAL, 1), condition("n", Operator.LIKE, 5)));
        assertThat(e.getMessage(), startsWith("Invalid `filters[1]`: LIKE on 'n' needs a text value"));
    }

    @Test
    void rejectsInvalidInput() {
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.EQUAL, null)));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.GREATER_THAN, true)));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.CONTAINS_ANY, List.of())));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.CONTAINS_ANY, List.of("a", 1))));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.IS_NULL, "maybe")));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.EQUAL, "x", ValueType.UUID)));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.EQUAL, "x", ValueType.DATE)));
        assertThrows(IllegalArgumentException.class, () -> build(condition("a", Operator.EQUAL, 1.5, ValueType.INT)));
        assertThrows(IllegalArgumentException.class, () -> build(condition("_id", Operator.LIKE, "x")));
        assertThrows(IllegalArgumentException.class, () -> build(condition("_creationTimeUnix", Operator.LIKE, "x")));
        assertThrows(IllegalArgumentException.class, () -> build(condition("_creationTimeUnix", Operator.EQUAL, "yesterday")));
        assertThrows(IllegalArgumentException.class, () -> build(FilterCondition.builder().operator(Operator.EQUAL).value(1).build()));
        assertThrows(IllegalArgumentException.class, () -> build(FilterCondition.builder().property("a").value(1).build()));
        var e = assertThrows(IllegalArgumentException.class, () -> WeaviateFilters.build(Map.of("n", List.of()), null, null));
        assertThat(e.getMessage(), containsString("'n'"));
    }

    @Test
    void normalizeProducesClientTypes() {
        assertThat(WeaviateFilters.normalize(5, null), is(5L));
        assertThat(WeaviateFilters.normalize(5.5f, null), is(5.5d));
        assertThat(WeaviateFilters.normalize(List.of(1, 2), null), is(List.of(1L, 2L)));
        assertThat(WeaviateFilters.normalize(List.of(1, 2.5), null), is(List.of(1.0d, 2.5d)));
        assertThat(build(condition("a", Operator.CONTAINS_ANY, List.of(1, 2.5))), notNullValue());
        assertThat(WeaviateFilters.normalize("2024-01-01T00:00:00Z", ValueType.DATE), is(OffsetDateTime.parse("2024-01-01T00:00:00Z")));
    }
}
