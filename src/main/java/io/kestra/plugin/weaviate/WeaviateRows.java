package io.kestra.plugin.weaviate;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import io.weaviate.client6.v1.api.collections.GeoCoordinates;
import io.weaviate.client6.v1.api.collections.PhoneNumber;
import io.weaviate.client6.v1.api.collections.VectorIndex;
import io.weaviate.client6.v1.api.collections.Vectors;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import io.weaviate.client6.v1.api.collections.query.QueryMetadata;

/**
 * Converts query results into plain maps that Kestra can serialize to JSON outputs and ION files.
 */
final class WeaviateRows {
    private WeaviateRows() {
    }

    /**
     * Returns {@code {uuid, properties, metadata, vector?, vectors?}}. Metadata only holds the fields
     * the server returned; {@code vector} is the default vector and {@code vectors} holds named ones.
     */
    static Map<String, Object> toRow(WeaviateObject<Map<String, Object>> object) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("uuid", object.uuid());
        row.put("properties", object.properties() == null ? Map.of() : convertMap(object.properties()));
        row.put("metadata", metadata(object));

        Vectors vectors = object.vectors();
        if (vectors != null && vectors.size() > 0) {
            Map<String, Object> all = new LinkedHashMap<>();
            vectors.asMap().forEach((name, vector) -> all.put(name, convert(vector)));
            Object defaultVector = all.remove(VectorIndex.DEFAULT_VECTOR_NAME);
            if (defaultVector != null) {
                row.put("vector", defaultVector);
            }
            if (!all.isEmpty()) {
                row.put("vectors", all);
            }
        }
        return row;
    }

    private static Map<String, Object> metadata(WeaviateObject<Map<String, Object>> object) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        QueryMetadata query = object.queryMetadata();
        if (query != null) {
            putIfPresent(metadata, "distance", query.distance());
            putIfPresent(metadata, "certainty", query.certainty());
            putIfPresent(metadata, "score", query.score());
            putIfPresent(metadata, "explainScore", query.explainScore());
        }
        putIfPresent(metadata, "creationTimeUnix", object.createdAt());
        putIfPresent(metadata, "lastUpdateTimeUnix", object.lastUpdatedAt());
        return metadata;
    }

    private static void putIfPresent(Map<String, Object> map, String key, Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    private static Map<String, Object> convertMap(Map<?, ?> map) {
        Map<String, Object> result = new LinkedHashMap<>();
        map.forEach((k, v) -> result.put(String.valueOf(k), convert(v)));
        return result;
    }

    static Object convert(Object value) {
        return switch (value) {
            case null -> null;
            case String s -> s;
            case Boolean b -> b;
            case Number n -> n;
            case OffsetDateTime date -> date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            case UUID uuid -> uuid.toString();
            case float[] vector -> toList(vector);
            case float[][] multi -> Arrays.stream(multi).map(WeaviateRows::toList).toList();
            case GeoCoordinates geo -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("latitude", geo.latitude());
                m.put("longitude", geo.longitude());
                yield m;
            }
            case PhoneNumber phone -> {
                Map<String, Object> m = new LinkedHashMap<>();
                putIfPresent(m, "input", phone.rawInput());
                putIfPresent(m, "internationalFormatted", phone.internationalFormatted());
                putIfPresent(m, "countryCode", phone.countryCode());
                putIfPresent(m, "defaultCountry", phone.defaultCountry());
                putIfPresent(m, "national", phone.national());
                putIfPresent(m, "nationalFormatted", phone.nationalFormatted());
                putIfPresent(m, "valid", phone.valid());
                yield m;
            }
            case Map<?, ?> map -> convertMap(map);
            case Collection<?> list -> list.stream().map(WeaviateRows::convert).toList();
            case Object[] array -> Arrays.stream(array).map(WeaviateRows::convert).toList();
            default -> value.toString();
        };
    }

    private static List<Float> toList(float[] vector) {
        List<Float> list = new ArrayList<>(vector.length);
        for (float f : vector) {
            list.add(f);
        }
        return list;
    }
}
