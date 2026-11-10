package io.kestra.plugin.weaviate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.BatchCreate.BatchMode;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BatchModesTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    /** {@code count} objects numbered from 0; the ones at {@code rejected} carry a non-integer and are rejected. */
    private static List<Map<String, Object>> objects(int count, int... rejected) {
        var bad = IntStream.of(rejected).boxed().toList();
        List<Map<String, Object>> objects = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            objects.add(Map.of("n", bad.contains(i) ? (Object) "not a number" : (Object) i));
        }
        return objects;
    }

    @Test
    public void fixedSizeInParallelKeepsInputOrder() throws Exception {
        createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property.integer("n"));
        RunContext runContext = runContextFactory.of();

        var output = batchCreate()
            .mode(Property.ofValue(BatchMode.FIXED_SIZE))
            .batchSize(Property.ofValue(2))
            .concurrentRequests(Property.ofValue(4))
            .failOnError(Property.ofValue(false))
            .objects(objects(20, 15, 3))
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(18L));
        assertThat(output.getFailedCount(), is(2L));
        assertThat(output.getMode(), is(BatchMode.FIXED_SIZE));
        assertThat(output.getRequestCount(), is(10L));
        assertThat(output.getErrors().stream().map(BatchCreate.ObjectError::getIndex).toList(), is(List.of(3L, 15L)));
        assertThat(count(runContext), is(18));
    }

    @Test
    public void fixedSizeInParallelReportsTheFirstErrorsInInputOrder() throws Exception {
        createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property.integer("n"));
        RunContext runContext = runContextFactory.of();
        var rejected = IntStream.range(0, 300).map(i -> i * 2).toArray();

        var output = batchCreate()
            .mode(Property.ofValue(BatchMode.FIXED_SIZE))
            .batchSize(Property.ofValue(10))
            .concurrentRequests(Property.ofValue(8))
            .failOnError(Property.ofValue(false))
            .objects(objects(600, rejected))
            .build()
            .run(runContext);

        assertThat(output.getFailedCount(), is(300L));
        assertThat(
            output.getErrors().stream().map(BatchCreate.ObjectError::getIndex).toList(),
            is(IntStream.range(0, BatchCreate.MAX_REPORTED_ERRORS).mapToObj(i -> (long) i * 2).toList())
        );
    }

    @Test
    public void errorsKeepTheLowestIndexes() {
        var result = new BatchCreate.Result();
        for (var i = 499; i >= 0; i--) {
            BatchCreate.addError(result, new BatchCreate.ObjectError(i, null, "e" + i));
        }
        BatchCreate.finishErrors(result);

        assertThat(
            result.errors.stream().map(BatchCreate.ObjectError::getIndex).toList(),
            is(IntStream.range(0, BatchCreate.MAX_REPORTED_ERRORS).mapToObj(i -> (long) i).toList())
        );
    }

    @Test
    public void rateLimitPacesRequests() throws Exception {
        createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property.integer("n"));
        RunContext runContext = runContextFactory.of();

        long start = System.nanoTime();
        var output = batchCreate()
            .mode(Property.ofValue(BatchMode.RATE_LIMIT))
            .batchSize(Property.ofValue(1))
            .objectsPerMinute(Property.ofValue(60))
            .objects(objects(3))
            .build()
            .run(runContext);
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        assertThat(output.getInsertedCount(), is(3L));
        // One object per second: the third request may not start before 2 seconds.
        assertThat(elapsedMs, greaterThanOrEqualTo(2_000L));
    }

    @Test
    public void serverSideBatching() throws Exception {
        createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property.integer("n"));
        RunContext runContext = runContextFactory.of();

        var output = batchCreate()
            .mode(Property.ofValue(BatchMode.SERVER_SIDE))
            .batchSize(Property.ofValue(10))
            .failOnError(Property.ofValue(false))
            .objects(objects(50, 7))
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(49L));
        assertThat(output.getFailedCount(), is(1L));
        assertThat(output.getMode(), is(BatchMode.SERVER_SIDE));
        assertThat(output.getRequestCount(), nullValue());
        assertThat(output.getErrors().getFirst().getIndex(), is(7L));
        assertThat(output.getErrors().getFirst().getMessage(), not(emptyOrNullString()));
        assertThat(count(runContext), is(49));
    }

    @Test
    public void serverSideBatchingIntoATenant() throws Exception {
        RunContext runContext = runContextFactory.of();
        schemaCreate()
            .fields(Property.ofValue(Map.of("n", "int")))
            .multiTenancy(Property.ofValue(SchemaCreate.MultiTenancyOptions.builder().tenants(List.of("acme", "globex")).build()))
            .build()
            .run(runContext);

        var output = batchCreate().mode(Property.ofValue(BatchMode.SERVER_SIDE)).tenant(Property.ofValue("acme")).objects(objects(30)).build().run(runContext);

        assertThat(output.getInsertedCount(), is(30L));
        assertThat(query().tenant(Property.ofValue("acme")).limit(Property.ofValue(100)).build().run(runContext).getRows(), hasSize(30));
        assertThat(query().tenant(Property.ofValue("globex")).limit(Property.ofValue(100)).build().run(runContext).getRows(), empty());
    }

    @Test
    public void dynamicBatching() throws Exception {
        createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property.integer("n"));
        RunContext runContext = runContextFactory.of();

        var output = batchCreate()
            .mode(Property.ofValue(BatchMode.DYNAMIC))
            .batchSize(Property.ofValue(3))
            .failOnError(Property.ofValue(false))
            .objects(objects(40, 0, 39))
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(38L));
        assertThat(output.getMode(), is(BatchMode.DYNAMIC));
        assertThat(output.getRequestCount(), lessThan(14L)); // fixed batches of 3 would need 14 requests
        assertThat(output.getErrors().stream().map(BatchCreate.ObjectError::getIndex).toList(), is(List.of(0L, 39L)));
        assertThat(count(runContext), is(38));
    }

    @Test
    public void autoUsesServerSideOnThisServer() throws Exception {
        try (var client = client()) {
            assertThat(BatchCreate.autoMode(client), is(BatchMode.SERVER_SIDE));
        }
        createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property.integer("n"));
        RunContext runContext = runContextFactory.of();
        var output = batchCreate().objects(objects(12)).build().run(runContext);
        assertThat(output.getInsertedCount(), is(12L));
        assertThat(output.getMode(), is(BatchMode.SERVER_SIDE));
    }

    /** Without priming, auto-schema rejected a few objects in most runs ("class name ... already exists"). */
    @Test
    public void concurrentModesCreateAMissingCollectionOnce() throws Exception {
        for (var mode : List.of(BatchMode.SERVER_SIDE, BatchMode.FIXED_SIZE, BatchMode.SERVER_SIDE, BatchMode.FIXED_SIZE)) {
            try (var client = client()) {
                client.collections.delete(CLASS_NAME);
            }
            RunContext runContext = runContextFactory.of();
            var output = batchCreate()
                .mode(Property.ofValue(mode))
                .batchSize(Property.ofValue(2))
                .concurrentRequests(Property.ofValue(mode == BatchMode.FIXED_SIZE ? 4 : 1))
                .objects(objects(40))
                .build()
                .run(runContext);
            assertThat(mode + " inserted", output.getInsertedCount(), is(40L));
            assertThat(mode + " stored", count(runContext), is(40));
        }
    }

    @Test
    public void autoModeAndDynamicSizing() {
        assertThat(BatchCreate.supportsServerSide("1.35.9"), is(false));
        assertThat(BatchCreate.supportsServerSide("1.36.0"), is(true));
        assertThat(BatchCreate.supportsServerSide("1.39.7"), is(true));
        assertThat(BatchCreate.supportsServerSide("v1.36.2-rc.1"), is(true));
        assertThat(BatchCreate.supportsServerSide("2.0.0"), is(true));
        assertThat(BatchCreate.supportsServerSide("1.9.0"), is(false));
        assertThat(BatchCreate.supportsServerSide(null), is(false));
        assertThat(BatchCreate.supportsServerSide("unknown"), is(false));

        assertThat(BatchCreate.nextDynamicSize(100, 200_000_000L), is(200));
        assertThat(BatchCreate.nextDynamicSize(8_000, 200_000_000L), is(10_000));
        assertThat(BatchCreate.nextDynamicSize(100, 3_000_000_000L), is(100));
        assertThat(BatchCreate.nextDynamicSize(100, 9_000_000_000L), is(50));
        assertThat(BatchCreate.nextDynamicSize(1, 9_000_000_000L), is(1));
    }

    private static int count(RunContext runContext) throws Exception {
        return query().limit(Property.ofValue(100)).build().run(runContext).getRows().size();
    }

    @Test
    public void rejectsOptionsThatDoNotFitTheMode() {
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> BatchCreate.sender(BatchMode.RATE_LIMIT, 100, 1, null)).getMessage(),
            containsString("RATE_LIMIT needs `objectsPerMinute`")
        );
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> BatchCreate.validate(BatchMode.AUTO, 100, 4, null)).getMessage(),
            containsString("`concurrentRequests` is only supported with FIXED_SIZE")
        );
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> BatchCreate.sender(BatchMode.FIXED_SIZE, 100, 1, 60)).getMessage(),
            containsString("`objectsPerMinute` is only supported with RATE_LIMIT")
        );
        assertThat(
            assertThrows(IllegalArgumentException.class, () -> BatchCreate.sender(BatchMode.FIXED_SIZE, 100, 33, null)).getMessage(),
            containsString("between 1 and 32")
        );
    }
}
