package io.kestra.plugin.weaviate;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.CollectionHandle;
import io.weaviate.client6.v1.api.collections.Vectors;
import io.weaviate.client6.v1.api.collections.WeaviateObject;
import io.weaviate.client6.v1.api.collections.data.InsertManyResponse;
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
    title = "Batch insert Weaviate objects",
    description = "Loads objects into a collection over gRPC from a Kestra ION file or an inline list, in chunks of `batchSize`. " +
        "Each object gets a generated UUID unless `uuidKey` is set. If the collection is missing, Weaviate auto-schema (if enabled) creates it. " +
        "By default the task fails when any object is rejected; set `failOnError: false` to report rejected objects in the output instead."
)
@Plugin(
    examples = {
        @Example(
            title = "Insert an inline list of objects.",
            full = true,
            code = """
                id: weaviate_batch_load
                namespace: company.team

                tasks:
                  - id: batch_load
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: WeaviateDemo
                    objects:
                      - textField: "some text"
                        numField: 24
                      - textField: "another text"
                        numField: 42
                """
        ),
        @Example(
            title = "Insert objects from an ION file produced by another task.",
            full = true,
            code = """
                id: weaviate_batch_insert
                namespace: company.team

                tasks:
                  - id: extract
                    type: io.kestra.plugin.core.http.Download
                    uri: https://huggingface.co/datasets/kestra/datasets/raw/main/ion/ingest.ion

                  - id: batch_insert
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Titles
                    objects: "{{ outputs.extract.uri }}"
                """
        ),
        @Example(
            title = "Insert objects with their own IDs and precomputed vectors, and keep going if some objects are rejected.",
            full = true,
            code = """
                id: weaviate_batch_vectors
                namespace: company.team

                tasks:
                  - id: batch_insert
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: http://localhost:8080
                    className: Documents
                    uuidKey: id
                    vectorKey: embedding
                    failOnError: false
                    objects:
                      - id: 0b9e8a4a-3b1c-4c1e-9f7a-1c2d3e4f5a6b
                        title: First document
                        embedding: [0.12, 0.45, 0.91]
                """
        )
    }
)
public class BatchCreate extends WeaviateConnection implements RunnableTask<BatchCreate.Output> {
    static final int MAX_REPORTED_ERRORS = 100;

    @Schema(title = "Collection (class) to insert into")
    @NotNull
    @PluginProperty(group = "main")
    private Property<String> className;

    @Schema(
        title = "Objects to insert with properties",
        description = "Either a Kestra storage URI to an ION file or an inline list of maps. Each entry becomes one Weaviate object.",
        anyOf = {
            String.class,
            Map[].class
        }
    )
    @NotNull
    @PluginProperty(dynamic = true, group = "main")
    private Object objects;

    @Schema(
        title = "Key holding the object UUID",
        description = "When set, this key is removed from each object and used as its UUID; objects without it get a generated UUID. " +
            "Inserting an existing UUID replaces that object."
    )
    @PluginProperty(group = "processing")
    private Property<String> uuidKey;

    @Schema(
        title = "Key holding the object vector",
        description = "When set, this key is removed from each object and used as its vector (a list of numbers, or a list of lists for multi-vectors). " +
            "Use this for collections without a vectorizer."
    )
    @PluginProperty(group = "processing")
    private Property<String> vectorKey;

    @Schema(title = "Name of the vector `vectorKey` fills", description = "For collections with named vectors. Defaults to the default vector.")
    @PluginProperty(group = "processing")
    private Property<String> targetVector;

    @Schema(title = "Number of objects sent per request")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> batchSize = Property.ofValue(500);

    @Schema(
        title = "Fail when an object is rejected",
        description = "When false, rejected objects are listed in the output `errors` (up to " + MAX_REPORTED_ERRORS + ") and the task succeeds."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Boolean> failOnError = Property.ofValue(true);

    @Schema(title = "Tenant", description = "Tenant to insert into, for multi-tenant collections.")
    @PluginProperty(group = "advanced")
    private Property<String> tenant;

    @Override
    public Output run(RunContext runContext) throws Exception {
        String renderedClassName = runContext.render(className).as(String.class).orElseThrow();
        String renderedUuidKey = runContext.render(uuidKey).as(String.class).orElse(null);
        String renderedVectorKey = runContext.render(vectorKey).as(String.class).orElse(null);
        String renderedTargetVector = runContext.render(targetVector).as(String.class).orElse(null);
        String renderedTenant = runContext.render(tenant).as(String.class).orElse(null);
        int renderedBatchSize = runContext.render(batchSize).as(Integer.class).orElse(500);
        boolean renderedFailOnError = runContext.render(failOnError).as(Boolean.class).orElse(true);
        if (renderedBatchSize < 1 || renderedBatchSize > 10_000) {
            throw new IllegalArgumentException("`batchSize` must be between 1 and 10000, got " + renderedBatchSize + ".");
        }

        ObjectConverter converter = new ObjectConverter(renderedUuidKey, renderedVectorKey, renderedTargetVector);
        Result result = new Result();

        try (WeaviateClient client = connect(runContext)) {
            CollectionHandle<Map<String, Object>> collection = client.collections.use(renderedClassName);
            if (renderedTenant != null) {
                collection = collection.withTenant(renderedTenant);
            }
            CollectionHandle<Map<String, Object>> target = collection;

            if (objects instanceof List<?> list) {
                Flux<Map<String, Object>> flux = Flux.fromIterable(list)
                    .map(item ->
                    {
                        if (!(item instanceof Map<?, ?> map)) {
                            throw new IllegalArgumentException("Each entry of `objects` must be a map, got " + item);
                        }
                        try {
                            return runContext.render(castMap(map));
                        } catch (Exception e) {
                            throw new IllegalArgumentException("Failed to render object: " + e.getMessage(), e);
                        }
                    });
                insert(flux, renderedBatchSize, target, converter, result);
            } else if (objects instanceof String uri) {
                try (
                    BufferedReader reader = new BufferedReader(
                        new InputStreamReader(runContext.storage().getFile(URI.create(runContext.render(uri))), StandardCharsets.UTF_8)
                    )
                ) {
                    Flux<Map<String, Object>> flux = FileSerde.readAll(reader, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {
                    });
                    insert(flux, renderedBatchSize, target, converter, result);
                }
            } else {
                throw new IllegalArgumentException("`objects` must be a storage URI or a list of maps.");
            }
        }

        runContext.metric(Counter.of("records", result.inserted));
        runContext.metric(Counter.of("failed", result.failed));
        runContext.logger().info("Inserted {} objects into '{}', {} rejected", result.inserted, renderedClassName, result.failed);

        if (result.failed > 0 && renderedFailOnError) {
            throw new IllegalStateException(
                result.failed + " of " + (result.inserted + result.failed) + " objects were rejected by Weaviate. First errors: " +
                    result.errors.stream().limit(5).map(e -> "#" + e.getIndex() + " " + e.getMessage()).toList()
            );
        }

        return Output.builder()
            .insertedCount(result.inserted)
            .failedCount(result.failed)
            .errors(result.errors)
            .build();
    }

    private static void insert(Flux<Map<String, Object>> source, int batchSize, CollectionHandle<Map<String, Object>> collection, ObjectConverter converter,
        Result result) {
        for (List<Map<String, Object>> chunk : source.buffer(batchSize).toIterable()) {
            List<WeaviateObject<Map<String, Object>>> batch = new ArrayList<>(chunk.size());
            for (Map<String, Object> map : chunk) {
                batch.add(converter.toObject(map));
            }

            InsertManyResponse response = collection.data.insertMany(batch);
            List<InsertManyResponse.InsertObject> responses = response.responses();
            for (int i = 0; i < responses.size(); i++) {
                InsertManyResponse.InsertObject r = responses.get(i);
                if (r.successful()) {
                    result.inserted++;
                } else {
                    result.failed++;
                    if (result.errors.size() < MAX_REPORTED_ERRORS) {
                        result.errors.add(new ObjectError(result.offset + i, r.uuid(), r.error()));
                    }
                }
            }
            result.offset += batch.size();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }

    private static final class Result {
        long inserted;
        long failed;
        long offset;
        final List<ObjectError> errors = new ArrayList<>();
    }

    /**
     * Turns one input map into a Weaviate object: pulls out the UUID and vector keys and converts
     * values into types the v6 client can marshal.
     */
    record ObjectConverter(String uuidKey, String vectorKey, String targetVector) {
        WeaviateObject<Map<String, Object>> toObject(Map<String, Object> input) {
            Map<String, Object> properties = new LinkedHashMap<>(input);
            Object rawUuid = uuidKey != null ? properties.remove(uuidKey) : null;
            Object rawVector = vectorKey != null ? properties.remove(vectorKey) : null;

            String uuid;
            if (rawUuid == null) {
                uuid = UUID.randomUUID().toString();
            } else {
                try {
                    uuid = UUID.fromString(rawUuid.toString()).toString();
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Value of `" + uuidKey + "` is not a valid UUID: " + rawUuid);
                }
            }

            Map<String, Object> converted = convertProperties(properties);
            Vectors vectors = rawVector == null ? null : toVectors(rawVector);
            return WeaviateObject.of(o ->
            {
                o.uuid(uuid).properties(converted);
                if (vectors != null) {
                    o.vectors(vectors);
                }
                return o;
            });
        }

        private Vectors toVectors(Object raw) {
            if (!(raw instanceof List<?> list) || list.isEmpty()) {
                throw new IllegalArgumentException("Value of `" + vectorKey + "` must be a non-empty list of numbers, got " + raw);
            }
            if (list.getFirst() instanceof List<?>) {
                float[][] multi = new float[list.size()][];
                for (int i = 0; i < list.size(); i++) {
                    if (!(list.get(i) instanceof List<?> row)) {
                        throw new IllegalArgumentException("Value of `" + vectorKey + "` mixes numbers and lists.");
                    }
                    multi[i] = toFloats(row);
                }
                return targetVector != null ? Vectors.of(targetVector, multi) : Vectors.of(multi);
            }
            float[] single = toFloats(list);
            return targetVector != null ? Vectors.of(targetVector, single) : Vectors.of(single);
        }

        private float[] toFloats(List<?> values) {
            float[] result = new float[values.size()];
            for (int i = 0; i < values.size(); i++) {
                if (!(values.get(i) instanceof Number n)) {
                    throw new IllegalArgumentException("Value of `" + vectorKey + "` must only contain numbers, got " + values.get(i));
                }
                result[i] = n.floatValue();
            }
            return result;
        }
    }

    static Map<String, Object> convertProperties(Map<String, Object> properties) {
        Map<String, Object> result = new LinkedHashMap<>();
        properties.forEach((key, value) ->
        {
            Object converted = convertValue(value);
            if (converted != null) {
                result.put(key, converted);
            }
        });
        return result;
    }

    /**
     * Maps ION/JSON values onto the types accepted by the v6 gRPC marshaller: dates become RFC3339 strings,
     * nested maps and lists are converted recursively, and anything else unknown becomes its string form.
     * Dates are formatted here because the client's OffsetDateTime.toString() drops zero seconds, which the server rejects.
     */
    @SuppressWarnings("unchecked")
    static Object convertValue(Object value) {
        return switch (value) {
            case null -> null;
            case String s -> s;
            case Boolean b -> b;
            case Number n -> n;
            case UUID u -> u;
            case OffsetDateTime odt -> rfc3339(odt);
            case ZonedDateTime zdt -> rfc3339(zdt.toOffsetDateTime());
            case Instant instant -> rfc3339(instant.atOffset(ZoneOffset.UTC));
            case LocalDateTime ldt -> rfc3339(ldt.atOffset(ZoneOffset.UTC));
            case LocalDate ld -> rfc3339(ld.atStartOfDay().atOffset(ZoneOffset.UTC));
            case Date date -> rfc3339(date.toInstant().atOffset(ZoneOffset.UTC));
            case Map<?, ?> map -> convertProperties((Map<String, Object>) map);
            case Collection<?> list -> list.stream().map(BatchCreate::convertValue).filter(Objects::nonNull).toList();
            default -> value.toString();
        };
    }

    private static String rfc3339(OffsetDateTime date) {
        return date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of objects inserted")
        private final long insertedCount;

        @Schema(title = "Number of objects rejected")
        private final long failedCount;

        @Schema(title = "Rejected objects", description = "Up to " + MAX_REPORTED_ERRORS + " entries, in input order.")
        private final List<ObjectError> errors;
    }

    @Getter
    @AllArgsConstructor
    public static class ObjectError {
        @Schema(title = "Zero-based position of the object in the input")
        private final long index;

        @Schema(title = "UUID of the rejected object")
        private final String uuid;

        @Schema(title = "Error returned by Weaviate")
        private final String message;
    }
}
