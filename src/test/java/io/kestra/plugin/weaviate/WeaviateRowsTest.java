package io.kestra.plugin.weaviate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;

import io.weaviate.client6.v1.api.collections.GeoCoordinates;
import io.weaviate.client6.v1.api.collections.Vectors;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class WeaviateRowsTest {
    private static final String ID = "0b9e8a4a-3b1c-4c1e-9f7a-1c2d3e4f5a6b";

    private static WeaviateObject<Map<String, Object>> object(Map<String, Object> properties, Vectors... vectors) {
        return WeaviateObject.of(o ->
        {
            o.uuid(ID).properties(properties);
            if (vectors.length > 0) {
                o.vectors(vectors);
            }
            return o;
        });
    }

    @Test
    void convertsPropertiesToSerializableTypes() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("text", "a");
        properties.put("int", 3L);
        properties.put("date", OffsetDateTime.parse("2024-01-02T03:04:05Z"));
        properties.put("ref", UUID.fromString(ID));
        properties.put("geo", new GeoCoordinates(1.5f, 2.5f));
        properties.put("nested", Map.of("when", OffsetDateTime.parse("2024-01-02T03:04:05+02:00")));
        properties.put("list", List.of(UUID.fromString(ID)));

        Map<String, Object> row = WeaviateRows.toRow(object(properties));

        assertThat(row.get("uuid"), is(ID));
        assertThat(row.get("metadata"), is(Map.of()));
        assertThat(row, not(hasKey("vector")));
        @SuppressWarnings("unchecked")
        Map<String, Object> converted = (Map<String, Object>) row.get("properties");
        assertThat(converted.get("text"), is("a"));
        assertThat(converted.get("int"), is(3L));
        assertThat(converted.get("date"), is("2024-01-02T03:04:05Z"));
        assertThat(converted.get("ref"), is(ID));
        assertThat(converted.get("geo"), is(Map.of("latitude", 1.5f, "longitude", 2.5f)));
        assertThat(converted.get("nested"), is(Map.of("when", "2024-01-02T03:04:05+02:00")));
        assertThat(converted.get("list"), is(List.of(ID)));
    }

    @Test
    void splitsDefaultAndNamedVectors() {
        Map<String, Object> row = WeaviateRows.toRow(
            object(
                Map.of(),
                Vectors.of(new float[] { 1f, 2f }),
                Vectors.of("title", new float[] { 3f }),
                Vectors.of("colbert", new float[][] { { 4f }, { 5f } })
            )
        );

        assertThat(row.get("vector"), is(List.of(1f, 2f)));
        assertThat(row.get("vectors"), is(Map.of("title", List.of(3f), "colbert", List.of(List.of(4f), List.of(5f)))));
    }

    @Test
    void rowsRoundTripThroughIonAndJson() throws Exception {
        Map<String, Object> properties = Map.of(
            "date", OffsetDateTime.parse("2024-01-02T03:04:05Z"),
            "count", 2L,
            "tags", List.of("a", "b")
        );
        Map<String, Object> row = WeaviateRows.toRow(object(properties, Vectors.of(new float[] { 0.5f })));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FileSerde.writeAll(out, Flux.just(row)).block();
        List<Map> read = FileSerde.readAll(new ByteArrayInputStream(out.toByteArray()), Map.class).collectList().block();

        assertThat(read, hasSize(1));
        assertThat(read.getFirst().get("uuid"), is(ID));
        assertThat(((Map<?, ?>) read.getFirst().get("properties")).get("date"), is("2024-01-02T03:04:05Z"));
        assertThat(((Map<?, ?>) read.getFirst().get("properties")).get("tags"), is(List.of("a", "b")));

        String json = JacksonMapper.ofJson().writeValueAsString(row);
        assertThat(json, containsString("\"vector\":[0.5]"));
    }
}
