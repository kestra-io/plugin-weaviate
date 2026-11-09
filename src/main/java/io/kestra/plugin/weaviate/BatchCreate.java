package io.kestra.plugin.weaviate;

import java.net.URI;
import java.time.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import com.fasterxml.jackson.core.type.TypeReference;

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
import io.weaviate.client6.v1.api.collections.batch.BatchContext;
import io.weaviate.client6.v1.api.collections.batch.TaskHandle;
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
    description = """
        Loads objects into a collection over gRPC from a Kestra ION file or an inline list, in chunks of `batchSize`.
        Each object gets a generated UUID unless `uuidKey` is set. If the collection is missing, Weaviate auto-schema (if enabled) creates it.
        By default the task fails when any object is rejected; set `failOnError: false` to report rejected objects in the output instead."""
)
@Plugin(
    examples = {
        @Example(
            title = "Load a large file in parallel requests of fixed size; AUTO (the default) would use server-side batching on Weaviate 1.36+.",
            full = true,
            code = """
                id: weaviate_batch_parallel
                namespace: company.team

                inputs:
                  - id: file
                    type: FILE

                tasks:
                  - id: load
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: http://localhost:8080
                    className: Product
                    objects: "{{ inputs.file }}"
                    mode: FIXED_SIZE
                    batchSize: 200
                    concurrentRequests: 4
                """
        ),
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
        description = """
            When set, this key is removed from each object and used as its UUID; objects without it get a generated UUID.
            Inserting an existing UUID replaces that object."""
    )
    @PluginProperty(group = "processing")
    private Property<String> uuidKey;

    @Schema(
        title = "Key holding the object vector",
        description = """
            When set, this key is removed from each object and used as its vector (a list of numbers, or a list of lists for multi-vectors).
            Use this for collections without a vectorizer."""
    )
    @PluginProperty(group = "processing")
    private Property<String> vectorKey;

    @Schema(title = "Name of the vector `vectorKey` fills", description = "For collections with named vectors. Defaults to the default vector.")
    @PluginProperty(group = "processing")
    private Property<String> targetVector;

    @Schema(
        title = "How objects are sent",
        description = """
            AUTO (default) uses SERVER_SIDE on Weaviate 1.36 or later and DYNAMIC on older servers.
            SERVER_SIDE streams objects and lets Weaviate set the pace from its load; `batchSize` is only the initial size.
            DYNAMIC adapts the batch size to how long each request takes, starting from `batchSize`.
            FIXED_SIZE sends `batchSize` objects per request, `concurrentRequests` at a time.
            RATE_LIMIT sends at most `objectsPerMinute`, e.g. to stay under a vectorizer provider's rate limit.
            See https://docs.weaviate.io/weaviate/manage-objects/import."""
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<BatchMode> mode = Property.ofValue(BatchMode.AUTO);

    @Schema(title = "Number of objects sent per request", description = "Between 1 and 10000.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> batchSize = Property.ofValue(500);

    @Schema(title = "Requests sent in parallel (FIXED_SIZE)", description = "Between 1 and 32.")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<Integer> concurrentRequests = Property.ofValue(1);

    @Schema(title = "Maximum objects sent per minute (RATE_LIMIT)", description = "Required with RATE_LIMIT.")
    @PluginProperty(group = "advanced")
    private Property<Integer> objectsPerMinute;

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
        var rClassName = runContext.render(className).as(String.class).orElseThrow(() -> new IllegalArgumentException("`className` is required."));
        var rUuidKey = runContext.render(uuidKey).as(String.class).orElse(null);
        var rVectorKey = runContext.render(vectorKey).as(String.class).orElse(null);
        var rTargetVector = runContext.render(targetVector).as(String.class).orElse(null);
        var rTenant = runContext.render(tenant).as(String.class).orElse(null);
        int rBatchSize = runContext.render(batchSize).as(Integer.class).orElse(500);
        boolean rFailOnError = runContext.render(failOnError).as(Boolean.class).orElse(true);
        var rMode = runContext.render(mode).as(BatchMode.class).orElse(BatchMode.AUTO);
        int rConcurrentRequests = runContext.render(concurrentRequests).as(Integer.class).orElse(1);
        var rObjectsPerMinute = runContext.render(objectsPerMinute).as(Integer.class).orElse(null);
        validate(rMode, rBatchSize, rConcurrentRequests, rObjectsPerMinute);

        var converter = new ObjectConverter(rUuidKey, rVectorKey, rTargetVector);
        var result = new Result();

        try (WeaviateClient client = connect(runContext)) {
            var collection = client.collections.use(rClassName);
            if (rTenant != null) {
                collection = collection.withTenant(rTenant);
            }
            var target = collection;
            var effectiveMode = rMode == BatchMode.AUTO ? autoMode(client) : rMode;
            if (rMode == BatchMode.SERVER_SIDE) {
                requireServerSide(client);
            }
            if (rMode == BatchMode.AUTO) {
                runContext.logger().info("Batching mode AUTO resolved to {}", effectiveMode);
            }
            var sender = sender(effectiveMode, rBatchSize, rConcurrentRequests, rObjectsPerMinute);
            result.mode = effectiveMode;
            var concurrent = effectiveMode == BatchMode.SERVER_SIDE || (effectiveMode == BatchMode.FIXED_SIZE && rConcurrentRequests > 1);
            var primeAutoSchema = concurrent && !client.collections.exists(rClassName);
            if (primeAutoSchema) {
                runContext.logger().info("Collection '{}' does not exist yet; inserting the first object alone so auto-schema creates it once", rClassName);
            }

            if (objects instanceof List<?> list) {
                var flux = Flux.fromIterable(list)
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
                deliver(flux, primeAutoSchema, sender, target, converter, result);
            } else if (objects instanceof String uri) {
                // Read the raw stream: internal storage may hold binary Ion, which a text Reader would corrupt.
                try (var is = runContext.storage().getFile(URI.create(runContext.render(uri)))) {
                    var flux = FileSerde.readAll(is, new TypeReference<Map<String, Object>>() {
                    });
                    deliver(flux, primeAutoSchema, sender, target, converter, result);
                }
            } else {
                throw new IllegalArgumentException("`objects` must be a storage URI or a list of maps.");
            }
        }

        runContext.metric(Counter.of("records", result.inserted));
        runContext.metric(Counter.of("failed", result.failed));
        runContext.logger().info("Inserted {} objects into '{}', {} rejected", result.inserted, rClassName, result.failed);

        if (result.failed > 0 && rFailOnError) {
            throw new IllegalStateException(
                result.failed + " of " + (result.inserted + result.failed) + " objects were rejected by Weaviate. First errors: " +
                    result.errors.stream().limit(5).map(e -> "#" + e.getIndex() + " " + e.getMessage()).toList()
            );
        }

        return Output.builder()
            .insertedCount(result.inserted)
            .failedCount(result.failed)
            .errors(result.errors)
            .mode(result.mode)
            .batchSize(rBatchSize)
            .concurrentRequests(result.mode == BatchMode.FIXED_SIZE ? rConcurrentRequests : 1)
            .requestCount(result.mode == BatchMode.SERVER_SIDE ? null : result.requests)
            .build();
    }

    static final String SERVER_SIDE_MIN_VERSION = "1.36";

    /** Server-side batching is GA from Weaviate 1.36; older servers, or ones that don't report a gRPC message size, use DYNAMIC. */
    static BatchMode autoMode(WeaviateClient client) {
        try {
            var meta = client.meta();
            return supportsServerSide(meta.version()) && meta.grpcMaxMessageSize() != null ? BatchMode.SERVER_SIDE : BatchMode.DYNAMIC;
        } catch (java.io.IOException | RuntimeException e) {
            return BatchMode.DYNAMIC;
        }
    }

    // client6 6.3.2 opens the stream on older servers too, and then waits forever for a reply that never comes.
    private static void requireServerSide(WeaviateClient client) throws java.io.IOException {
        var version = client.meta().version();
        if (!supportsServerSide(version)) {
            throw new IllegalArgumentException(
                "SERVER_SIDE batching needs Weaviate " + SERVER_SIDE_MIN_VERSION + " or later, but the server is " + version + ". Use mode AUTO to fall back to DYNAMIC."
            );
        }
    }

    static boolean supportsServerSide(String version) {
        if (version == null) {
            return false;
        }
        var parts = version.replaceFirst("^v", "").split("[.-]");
        try {
            int major = Integer.parseInt(parts[0]);
            int minor = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
            return major > 1 || (major == 1 && minor >= 36);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    static void validate(BatchMode mode, int batchSize, int concurrentRequests, Integer objectsPerMinute) {
        if (batchSize < 1 || batchSize > 10_000) {
            throw new IllegalArgumentException("`batchSize` must be between 1 and 10000, got " + batchSize + ".");
        }
        if (concurrentRequests < 1 || concurrentRequests > 32) {
            throw new IllegalArgumentException("`concurrentRequests` must be between 1 and 32, got " + concurrentRequests + ".");
        }
        if (concurrentRequests > 1 && mode != BatchMode.FIXED_SIZE) {
            throw new IllegalArgumentException("`concurrentRequests` is only supported with FIXED_SIZE, not " + mode + ".");
        }
        if (objectsPerMinute != null && mode != BatchMode.RATE_LIMIT) {
            throw new IllegalArgumentException("`objectsPerMinute` is only supported with RATE_LIMIT, not " + mode + ".");
        }
        if (mode == BatchMode.RATE_LIMIT && (objectsPerMinute == null || objectsPerMinute < 1)) {
            throw new IllegalArgumentException("RATE_LIMIT needs `objectsPerMinute` of at least 1.");
        }
    }

    static Sender sender(BatchMode mode, int batchSize, int concurrentRequests, Integer objectsPerMinute) {
        validate(mode, batchSize, concurrentRequests, objectsPerMinute);
        return switch (mode) {
            case AUTO -> throw new IllegalStateException("AUTO is resolved before sending.");
            case DYNAMIC -> (source, collection, converter, result) -> sendDynamic(source, batchSize, collection, converter, result);
            case FIXED_SIZE -> (source, collection, converter, result) -> sendFixedSize(source, batchSize, concurrentRequests, collection, converter, result);
            case RATE_LIMIT -> {
                // A request never carries more than a minute's worth of objects, so the pace stays even.
                var size = Math.min(batchSize, objectsPerMinute);
                yield (source, collection, converter, result) -> sendRateLimited(source, size, objectsPerMinute, collection, converter, result);
            }
            case SERVER_SIDE -> (source, collection, converter, result) -> sendServerSide(source, batchSize, collection, converter, result);
        };
    }

    /**
     * When auto-schema has to create the collection, concurrent requests race to create it and Weaviate rejects the losers'
     * objects ("class name ... already exists"). Sending the first object alone creates the collection exactly once.
     */
    private static void deliver(Flux<Map<String, Object>> source, boolean primeAutoSchema, Sender sender, CollectionHandle<Map<String, Object>> collection,
        ObjectConverter converter, Result result) throws Exception {
        if (!primeAutoSchema) {
            sender.send(source, collection, converter, result);
            return;
        }
        var iterator = source.toIterable().iterator();
        if (!iterator.hasNext()) {
            return;
        }
        insertChunk(List.of(iterator.next()), collection, converter, result);
        sender.send(Flux.fromIterable(() -> iterator), collection, converter, result);
    }

    @FunctionalInterface
    interface Sender {
        void send(Flux<Map<String, Object>> source, CollectionHandle<Map<String, Object>> collection, ObjectConverter converter, Result result)
            throws Exception;
    }

    private static void sendFixedSize(Flux<Map<String, Object>> source, int batchSize, int concurrency, CollectionHandle<Map<String, Object>> collection,
        ObjectConverter converter, Result result) throws Exception {
        if (concurrency == 1) {
            for (List<Map<String, Object>> chunk : source.buffer(batchSize).toIterable()) {
                insertChunk(chunk, collection, converter, result);
            }
            return;
        }
        // Chunks are converted in input order (so offsets are stable) and inserted in parallel, at most `concurrency` at once.
        var executor = Executors.newFixedThreadPool(concurrency);
        var permits = new Semaphore(concurrency);
        var futures = new ArrayList<Future<?>>();
        try {
            for (List<Map<String, Object>> chunk : source.buffer(batchSize).toIterable()) {
                var batch = toObjects(chunk, converter);
                long offset;
                synchronized (result) {
                    offset = result.offset;
                    result.offset += batch.size();
                }
                permits.acquire();
                futures.add(executor.submit(() ->
                {
                    try {
                        tally(collection.data.insertMany(batch), offset, result);
                    } finally {
                        permits.release();
                    }
                    return null;
                }));
            }
            for (var future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    throw e.getCause() instanceof Exception cause ? cause : e;
                }
            }
        } finally {
            executor.shutdownNow();
        }
        synchronized (result) {
            result.errors.sort(java.util.Comparator.comparingLong(ObjectError::getIndex));
        }
    }

    static final long DYNAMIC_FAST_NANOS = 1_000_000_000L;
    static final long DYNAMIC_SLOW_NANOS = 5_000_000_000L;

    /** Doubles the batch size after a request under 1 s and halves it after one over 5 s, between 1 and 10000. */
    static int nextDynamicSize(int size, long elapsedNanos) {
        if (elapsedNanos < DYNAMIC_FAST_NANOS) {
            return Math.min(size * 2, 10_000);
        }
        if (elapsedNanos > DYNAMIC_SLOW_NANOS) {
            return Math.max(size / 2, 1);
        }
        return size;
    }

    private static void sendDynamic(Flux<Map<String, Object>> source, int initialSize, CollectionHandle<Map<String, Object>> collection,
        ObjectConverter converter, Result result) {
        var size = initialSize;
        var buffer = new ArrayList<Map<String, Object>>(size);
        for (Map<String, Object> map : source.toIterable()) {
            buffer.add(map);
            if (buffer.size() >= size) {
                long start = System.nanoTime();
                insertChunk(buffer, collection, converter, result);
                size = nextDynamicSize(size, System.nanoTime() - start);
                buffer = new ArrayList<>(size);
            }
        }
        if (!buffer.isEmpty()) {
            insertChunk(buffer, collection, converter, result);
        }
    }

    private static void sendRateLimited(Flux<Map<String, Object>> source, int batchSize, int objectsPerMinute, CollectionHandle<Map<String, Object>> collection,
        ObjectConverter converter, Result result) throws InterruptedException {
        long start = System.nanoTime();
        long sent = 0;
        for (List<Map<String, Object>> chunk : source.buffer(batchSize).toIterable()) {
            // Wait until sending this chunk keeps the total at or under objectsPerMinute since the start.
            long earliest = start + (long) (sent * 60_000_000_000.0 / objectsPerMinute);
            long wait = earliest - System.nanoTime();
            if (wait > 0) {
                Thread.sleep(wait / 1_000_000, (int) (wait % 1_000_000));
            }
            insertChunk(chunk, collection, converter, result);
            sent += chunk.size();
        }
    }

    private static void sendServerSide(Flux<Map<String, Object>> source, int batchSize, CollectionHandle<Map<String, Object>> collection,
        ObjectConverter converter, Result result) throws Exception {
        var pending = new java.util.ArrayDeque<PendingObject>();
        BatchContext<Map<String, Object>> context;
        try {
            context = collection.batch.start(b -> b.batchSize(batchSize));
        } catch (IllegalStateException e) {
            throw new IllegalStateException(
                "SERVER_SIDE batching needs Weaviate " + SERVER_SIDE_MIN_VERSION + " or later (" + e.getMessage() + "). Use mode AUTO to fall back to DYNAMIC.",
                e
            );
        }
        try (var batch = context) {
            for (Map<String, Object> map : source.toIterable()) {
                var object = converter.toObject(map);
                pending.add(new PendingObject(result.offset++, object.uuid(), batch.add(object)));
                // Settle finished objects as we go, so memory stays bounded on large imports.
                while (!pending.isEmpty() && pending.peek().handle().done().isDone()) {
                    settle(pending.poll(), result);
                }
            }
        }
        while (!pending.isEmpty()) {
            settle(pending.poll(), result);
        }
    }

    private record PendingObject(long index, String uuid, TaskHandle handle) {
    }

    private static void settle(PendingObject object, Result result) throws InterruptedException {
        try {
            object.handle().done().get();
            result.inserted++;
        } catch (ExecutionException e) {
            var cause = e.getCause() == null ? e : e.getCause();
            result.failed++;
            if (result.errors.size() < MAX_REPORTED_ERRORS) {
                result.errors.add(new ObjectError(object.index(), object.uuid(), cause.getMessage()));
            }
        }
    }

    private static List<WeaviateObject<Map<String, Object>>> toObjects(List<Map<String, Object>> chunk, ObjectConverter converter) {
        List<WeaviateObject<Map<String, Object>>> batch = new ArrayList<>(chunk.size());
        for (Map<String, Object> map : chunk) {
            batch.add(converter.toObject(map));
        }
        return batch;
    }

    private static void insertChunk(List<Map<String, Object>> chunk, CollectionHandle<Map<String, Object>> collection, ObjectConverter converter,
        Result result) {
        var batch = toObjects(chunk, converter);
        tally(collection.data.insertMany(batch), result.offset, result);
        result.offset += batch.size();
    }

    private static void tally(InsertManyResponse response, long offset, Result result) {
        var responses = response.responses();
        synchronized (result) {
            result.requests++;
            for (int i = 0; i < responses.size(); i++) {
                var r = responses.get(i);
                if (r.successful()) {
                    result.inserted++;
                } else {
                    result.failed++;
                    if (result.errors.size() < MAX_REPORTED_ERRORS) {
                        result.errors.add(new ObjectError(offset + i, r.uuid(), r.error()));
                    }
                }
            }
        }
    }

    public enum BatchMode {
        AUTO,
        SERVER_SIDE,
        DYNAMIC,
        FIXED_SIZE,
        RATE_LIMIT
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }

    static final class Result {
        BatchMode mode;
        long requests;
        long inserted;
        long failed;
        long offset;
        final List<ObjectError> errors = new ArrayList<>();
    }

    record ObjectConverter(String uuidKey, String vectorKey, String targetVector) {
        WeaviateObject<Map<String, Object>> toObject(Map<String, Object> input) {
            Map<String, Object> properties = new LinkedHashMap<>(input);
            var rawUuid = uuidKey != null ? properties.remove(uuidKey) : null;
            var rawVector = vectorKey != null ? properties.remove(vectorKey) : null;

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

            var converted = convertProperties(properties);
            var vectors = rawVector == null ? null : toVectors(rawVector);
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
            var converted = convertValue(value);
            if (converted != null) {
                result.put(key, converted);
            }
        });
        return result;
    }

    // Dates become RFC3339 text: see WeaviateDates.rfc3339.
    @SuppressWarnings("unchecked")
    static Object convertValue(Object value) {
        var date = WeaviateDates.toOffsetDateTime(value);
        if (date.isPresent()) {
            return WeaviateDates.rfc3339(date.get());
        }
        return switch (value) {
            case null -> null;
            case String s -> s;
            case Boolean b -> b;
            case Number n -> n;
            case UUID u -> u;
            case Map<?, ?> map -> convertProperties((Map<String, Object>) map);
            case Collection<?> list -> list.stream().map(BatchCreate::convertValue).filter(Objects::nonNull).toList();
            default -> value.toString();
        };
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

        @Schema(title = "Batching mode used", description = "With AUTO, the mode it resolved to: SERVER_SIDE or DYNAMIC.")
        private final BatchMode mode;

        @Schema(title = "Objects per request", description = "The configured `batchSize`; the starting size for DYNAMIC and SERVER_SIDE.")
        private final Integer batchSize;

        @Schema(title = "Requests sent in parallel", description = "`concurrentRequests` for FIXED_SIZE, otherwise 1.")
        private final Integer concurrentRequests;

        @Schema(title = "Insert requests sent", description = "Not reported for SERVER_SIDE, where Weaviate sets the batches.")
        private final Long requestCount;
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
