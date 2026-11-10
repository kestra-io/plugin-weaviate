package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kestra.plugin.weaviate.Query.SearchType;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class QueryValidationTest {
    private static final List<Double> VECTOR = List.of(0.1, 0.2);

    private static Query.Search.SearchBuilder search(SearchType type) {
        return Query.Search.builder().className("C").searchType(type);
    }

    static Stream<Arguments> valid() {
        return Stream.of(
            arguments(search(SearchType.FETCH_OBJECTS).limit(10).offset(0)),
            arguments(search(SearchType.BM25).query("q").queryProperties(List.of("title^2"))),
            arguments(search(SearchType.HYBRID).query("q").alpha(0.0)),
            arguments(search(SearchType.HYBRID).query("q").alpha(1.0).vector(VECTOR).targetVector("v")),
            arguments(search(SearchType.NEAR_TEXT).query("q").distance(0.3)),
            arguments(search(SearchType.NEAR_TEXT).query("q").certainty(0.8).targetVector("v")),
            arguments(search(SearchType.NEAR_VECTOR).vector(VECTOR).distance(0.2)),
            arguments(search(SearchType.FETCH_OBJECTS).targetVector("v").includeVector(true)),
            arguments(search(SearchType.BM25).query("q").targetVector("v").includeVector(true))
        );
    }

    @ParameterizedTest
    @MethodSource
    void valid(Query.Search.SearchBuilder builder) {
        assertDoesNotThrow(() -> builder.build().validate());
    }

    static Stream<Arguments> invalid() {
        return Stream.of(
            arguments(Query.Search.builder().searchType(SearchType.FETCH_OBJECTS), "`className` is required"),
            arguments(search(SearchType.FETCH_OBJECTS).query("q"), "`query` is not supported for searchType FETCH_OBJECTS"),
            arguments(search(SearchType.FETCH_OBJECTS).vector(VECTOR), "`vector` is not supported"),
            arguments(search(SearchType.FETCH_OBJECTS).distance(0.1), "`distance` is not supported"),
            arguments(search(SearchType.FETCH_OBJECTS).queryProperties(List.of("a")), "`queryProperties` is not supported"),
            arguments(search(SearchType.BM25), "`query` is required for searchType BM25"),
            arguments(search(SearchType.NEAR_VECTOR).vector(java.util.Arrays.asList(0.1, null)), "`vector` must not contain null values"),
            arguments(search(SearchType.BM25).query("q").alpha(0.5), "`alpha` is not supported"),
            arguments(search(SearchType.BM25).query("q").certainty(0.5), "`certainty` is not supported"),
            arguments(search(SearchType.BM25).query("q").targetVector("v"), "set `includeVector: true`"),
            arguments(search(SearchType.FETCH_OBJECTS).targetVector("v"), "set `includeVector: true`"),
            arguments(search(SearchType.HYBRID).query(" "), "`query` is required for searchType HYBRID"),
            arguments(search(SearchType.HYBRID).query("q").alpha(1.5), "`alpha` must be between 0 and 1"),
            arguments(search(SearchType.HYBRID).query("q").distance(0.5), "`distance` is not supported"),
            arguments(search(SearchType.NEAR_TEXT), "`query` is required for searchType NEAR_TEXT"),
            arguments(search(SearchType.NEAR_TEXT).query("q").vector(VECTOR), "`vector` is not supported"),
            arguments(search(SearchType.NEAR_TEXT).query("q").distance(0.1).certainty(0.9), "cannot be used together"),
            arguments(search(SearchType.NEAR_VECTOR), "`vector` is required for searchType NEAR_VECTOR"),
            arguments(search(SearchType.NEAR_VECTOR).vector(List.of()), "`vector` is required"),
            arguments(search(SearchType.NEAR_VECTOR).vector(VECTOR).query("q"), "`query` is not supported"),
            arguments(search(SearchType.NEAR_VECTOR).vector(VECTOR).certainty(2.0), "`certainty` must be between 0 and 1"),
            arguments(search(SearchType.FETCH_OBJECTS).limit(0), "`limit` must be greater than 0"),
            arguments(search(SearchType.FETCH_OBJECTS).offset(-1), "`offset` must not be negative"),
            arguments(search(SearchType.FETCH_OBJECTS).query("{ Get { Question { title } } }"), "GraphQL is no longer supported"),
            arguments(search(SearchType.BM25).query("\n  {\n    Get {\n      Question(limit: 2) { title }\n    }\n  }"), "GraphQL is no longer supported")
        );
    }

    @ParameterizedTest
    @MethodSource
    void invalid(Query.Search.SearchBuilder builder, String message) {
        var e = assertThrows(IllegalArgumentException.class, () -> builder.build().validate());
        assertThat(e.getMessage(), containsString(message));
    }
}
