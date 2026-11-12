package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.Quantization;
import io.weaviate.client6.v1.api.collections.vectorizers.SelfProvidedVectorizer;
import io.weaviate.client6.v1.api.collections.vectorizers.Text2VecCohereVectorizer;
import io.weaviate.client6.v1.api.collections.vectorizers.Text2VecModel2VecVectorizer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaTypeMappingTest {
    @ParameterizedTest
    @CsvSource(
        {
            "text, text",
            "string, text",
            "String, text",
            "string[], text[]",
            "text[], text[]",
            "int, int",
            "int[], int[]",
            "number, number",
            "number[], number[]",
            "boolean, boolean",
            "boolean[], boolean[]",
            "date, date",
            "date[], date[]",
            "uuid, uuid",
            "uuid[], uuid[]",
            "blob, blob",
            "geoCoordinates, geoCoordinates",
            "phoneNumber, phoneNumber"
        }
    )
    void mapsDataTypes(String input, String expected) {
        Property property = SchemaCreate.buildProperty("p", input, null);

        assertThat(property.propertyName(), is("p"));
        assertThat(property.dataTypes(), is(List.of(expected)));
    }

    @Test
    void acceptsStringOrSingleElementList() {
        List<Property> properties = SchemaCreate.buildProperties(Map.of("a", "text", "b", List.of("int")), null);

        assertThat(properties.stream().map(p -> p.dataTypes().getFirst()).toList(), containsInAnyOrder("text", "int"));
    }

    @Test
    void rejectsUnsupportedTypes() {
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", "object", null));
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", "Article", null));
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperties(Map.of("p", List.of("text", "int")), null));
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperties(Map.of("p", 5), null));
    }

    @Test
    void buildsConfiguredProperties() {
        var wordCount = SchemaCreate.buildProperty("wordCount", Map.of("type", "int", "indexRangeFilters", true, "description", "Length"), null);
        assertThat(wordCount.dataTypes(), is(List.of("int")));
        assertThat(wordCount.indexRangeFilters(), is(true));
        assertThat(wordCount.description(), is("Length"));
        assertThat(wordCount.indexFilterable(), nullValue());

        var body = SchemaCreate.buildProperty(
            "body",
            Map.of("type", "text", "indexFilterable", "false", "indexSearchable", true),
            null
        );
        assertThat(body.indexFilterable(), is(false));
        assertThat(body.indexSearchable(), is(true));

        assertThat(SchemaCreate.buildProperties(Map.of("a", "text", "b", Map.of("type", "date[]")), null), hasSize(2));
    }

    @Test
    void buildsTokenizationTextAnalyzerAndInvertedIndex() {
        var name = SchemaCreate.buildProperty("name", Map.of("type", "text", "tokenization", "field", "textAnalyzer", Map.of("foldAscii", true, "keepAscii", List.of("é"))), null);
        assertThat(name.tokenization(), is(io.weaviate.client6.v1.api.collections.Tokenization.FIELD));
        assertThat(name.textAnalyzer().foldAscii(), is(true));
        assertThat(name.textAnalyzer().keepAscii(), is(List.of("é")));

        var index = SchemaCreate.buildInvertedIndex(
            SchemaCreate.InvertedIndexOptions.builder()
                .indexTimestamps(true)
                .indexNullState(true)
                .indexPropertyLength(false)
                .bm25(SchemaCreate.Bm25Options.builder().b(0.7).k1(1.1).build())
                .stopwords(SchemaCreate.StopwordsOptions.builder().preset("en").additions(List.of("kestra")).removals(List.of("a")).build())
                .cleanupIntervalSeconds(30)
                .build()
        );
        assertThat(index.indexTimestamps(), is(true));
        assertThat(index.indexNulls(), is(true));
        assertThat(index.indexPropertyLength(), is(false));
        assertThat((double) index.bm25().b(), closeTo(0.7, 1e-6));
        assertThat(index.cleanupIntervalSeconds(), is(30));
    }

    @Test
    void buildsQuantization() {
        var rq = SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.RQ).bits(1).build());
        assertThat(rq.isRQ(), is(true));
        assertThat(rq.asRQ().bits(), is(1));
        var pq = SchemaCreate.buildQuantization(
            SchemaCreate.QuantizationOptions.builder()
                .type(SchemaCreate.QuantizationType.PQ)
                .segments(4)
                .centroids(128)
                .build()
        );
        assertThat(pq.isPQ(), is(true));
        assertThat(SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.NONE).build()).isUncompressed(), is(true));
        assertThat(SchemaCreate.buildQuantization(null), nullValue());

        var config = SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_COHERE, null, "content", null, rq);
        assertThat(((Text2VecCohereVectorizer) config.getValue()).quantization().isRQ(), is(true));
    }

    @Test
    void rejectsInvalidQuantization() {
        var noType = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().bits(8).build()));
        assertThat(noType.getMessage(), containsString("`quantization.type` is required"));
        var wrongOption = assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.BQ).bits(8).build())
        );
        assertThat(wrongOption.getMessage(), containsString("`quantization.bits` is only supported with [RQ], not BQ"));
        var badBits = assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.RQ).bits(4).build())
        );
        assertThat(badBits.getMessage(), containsString("must be 8 or 1"));
    }

    @Test
    void rejectsInvalidPropertyOptions() {
        var unknown = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("type", "int", "rangeable", true), null));
        assertThat(unknown.getMessage(), containsString("unknown options [rangeable]"));
        var noType = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("indexFilterable", false), null));
        assertThat(noType.getMessage(), containsString("must set `type`"));
        var badTokenization = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("type", "text", "tokenization", "words"), null));
        assertThat(badTokenization.getMessage(), containsString("unknown `tokenization` 'words'"));
        var badAnalyzer = assertThrows(
            IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("type", "text", "textAnalyzer", Map.of("fold", true)), null)
        );
        assertThat(badAnalyzer.getMessage(), containsString("unknown `textAnalyzer` option 'fold'"));
        var badFlag = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("type", "int", "indexFilterable", "no"), null));
        assertThat(badFlag.getMessage(), containsString("must be true or false"));
    }

    @Test
    void buildsVectorConfig() {
        var none = SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.NONE, null, null);
        assertThat(none.getKey(), is("default"));
        assertThat(none.getValue(), instanceOf(SelfProvidedVectorizer.class));

        var named = SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_MODEL2VEC, null, "body");
        assertThat(named.getKey(), is("body"));
        assertThat(named.getValue(), instanceOf(Text2VecModel2VecVectorizer.class));

        assertThat(SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_OPENAI, "text-embedding-3-small", null).getKey(), is("default"));

        var cohere = SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_COHERE, "embed-v4.0", "content");
        assertThat(cohere.getKey(), is("content"));
        assertThat(((Text2VecCohereVectorizer) cohere.getValue()).model(), is("embed-v4.0"));
        assertThat(SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_COHERE, null, null).getValue(), instanceOf(Text2VecCohereVectorizer.class));
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_MODEL2VEC, "m", null));

        var sourced = SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_COHERE, "embed-v4.0", "content", List.of("title", "body"), null);
        assertThat(((Text2VecCohereVectorizer) sourced.getValue()).sourceProperties(), is(List.of("title", "body")));
        assertThat(((Text2VecCohereVectorizer) sourced.getValue()).model(), is("embed-v4.0"));
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.NONE, null, null, List.of("title"), null));
        assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildVectorConfig(SchemaCreate.Vectorizer.TEXT2VEC_COHERE, null, null, List.of(), null));
    }

    @ParameterizedTest
    @EnumSource(value = SchemaCreate.Vectorizer.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void everyVectorizerKeepsSourcePropertiesAndQuantization(SchemaCreate.Vectorizer vectorizer) throws Exception {
        var rq = SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.RQ).build());

        var config = SchemaCreate.buildVectorConfig(vectorizer, null, "body", List.of("title"), rq).getValue();

        assertThat(((Quantization) config.getClass().getMethod("quantization").invoke(config)).isRQ(), is(true));
        assertThat(config.getClass().getMethod("sourceProperties").invoke(config), is(List.of("title")));
    }

    @Test
    void rejectsEmptyTenantNamesAndTenantsWithoutMultiTenancy() {
        var empty = assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCreate.tenantNames(SchemaCreate.MultiTenancyOptions.builder().tenants(java.util.Arrays.asList("a", " ")).build())
        );
        assertThat(empty.getMessage(), containsString("must not contain empty names"));

        var disabled = assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCreate.tenantNames(SchemaCreate.MultiTenancyOptions.builder().enabled(false).tenants(List.of("a")).build())
        );
        assertThat(disabled.getMessage(), containsString("remove `enabled: false`"));

        assertThat(SchemaCreate.tenantNames(SchemaCreate.MultiTenancyOptions.builder().tenants(List.of("a", "b", "a")).build()), is(List.of("a", "b")));
        assertThat(SchemaCreate.tenantNames(null), is(List.of()));
    }

    @Test
    void validatesTextAnalyzerShapes() {
        var notAMap = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("type", "text", "textAnalyzer", "ascii"), null));
        assertThat(notAMap.getMessage(), containsString("`textAnalyzer` must be a map"));

        var keepNotList = assertThrows(
            IllegalArgumentException.class,
            () -> SchemaCreate.buildProperty("p", Map.of("type", "text", "textAnalyzer", Map.of("keepAscii", "é")), null)
        );
        assertThat(keepNotList.getMessage(), containsString("`textAnalyzer.keepAscii` must be a list"));

        var preset = SchemaCreate.buildProperty("p", Map.of("type", "text", "textAnalyzer", Map.of("stopwordPreset", "en")), null);
        assertThat(preset.textAnalyzer().stopwordPreset(), is("en"));
    }

    @Test
    void validatesPqSettings() {
        for (var bad : List.of(
            SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.PQ).centroids(257).build(),
            SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.PQ).centroids(0).build(),
            SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.PQ).segments(-1).build(),
            SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.SQ).segments(4).build(),
            SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.RQ).centroids(256).build()
        )) {
            assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildQuantization(bad), bad.toString());
        }
        var pq = SchemaCreate.buildQuantization(SchemaCreate.QuantizationOptions.builder().type(SchemaCreate.QuantizationType.PQ).segments(0).centroids(256).build());
        assertThat(pq.isPQ(), is(true));
    }

    @Test
    void detectsMultiTenancyInADefinition() {
        assertThat(SchemaCreate.multiTenancyEnabled(Map.of("multiTenancyConfig", Map.of("enabled", true))), is(true));
        assertThat(SchemaCreate.multiTenancyEnabled(Map.of("multiTenancyConfig", Map.of("enabled", "true"))), is(true));
        assertThat(SchemaCreate.multiTenancyEnabled(Map.of("multiTenancyConfig", Map.of("enabled", false))), is(false));
        assertThat(SchemaCreate.multiTenancyEnabled(Map.of("class", "X")), is(false));
    }
}
