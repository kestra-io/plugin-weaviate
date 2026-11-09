package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.weaviate.client6.v1.api.collections.Property;
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
            Map.of("type", "text", "indexFilterable", "false", "indexSearchable", true, "skipVectorization", false, "vectorizePropertyName", false),
            null
        );
        assertThat(body.indexFilterable(), is(false));
        assertThat(body.indexSearchable(), is(true));
        assertThat(body.skipVectorization(), is(false));
        assertThat(body.vectorizePropertyName(), is(false));

        assertThat(SchemaCreate.buildProperties(Map.of("a", "text", "b", Map.of("type", "date[]")), null), hasSize(2));
    }

    @Test
    void rejectsInvalidPropertyOptions() {
        var unknown = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("type", "int", "rangeable", true), null));
        assertThat(unknown.getMessage(), containsString("unknown options [rangeable]"));
        var noType = assertThrows(IllegalArgumentException.class, () -> SchemaCreate.buildProperty("p", Map.of("indexFilterable", false), null));
        assertThat(noType.getMessage(), containsString("must set `type`"));
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
    }
}
