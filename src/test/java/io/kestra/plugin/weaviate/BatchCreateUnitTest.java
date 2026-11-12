package io.kestra.plugin.weaviate;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.kestra.plugin.weaviate.BatchCreate.BatchMode;
import io.kestra.plugin.weaviate.BatchCreate.ObjectConverter;

import io.weaviate.client6.v1.api.collections.data.InsertManyResponse;
import reactor.core.publisher.Flux;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** BatchCreate logic that needs no Weaviate server. */
class BatchCreateUnitTest {
    private static final ObjectConverter PLAIN = new ObjectConverter(null, null, null);

    private static Flux<Map<String, Object>> numbered(int count, AtomicInteger consumed) {
        return Flux.range(0, count).doOnNext(i -> consumed.incrementAndGet()).map(i -> Map.<String, Object>of("n", i));
    }

    /** Accepts every object, except those whose `n` is in `rejected`. */
    private static BatchCreate.Inserter accepting(List<Integer> rejected) {
        return batch -> new InsertManyResponse(
            0,
            batch.stream()
                .map(o -> rejected.contains((Integer) o.properties().get("n"))
                    ? new InsertManyResponse.InsertObject(o.uuid(), false, "rejected")
                    : new InsertManyResponse.InsertObject(o.uuid(), true, null))
                .toList(),
            List.of(),
            List.of()
        );
    }

    @Test
    void parallelFixedSizeStopsSendingAfterAFailure() {
        var consumed = new AtomicInteger();
        var calls = new AtomicInteger();
        BatchCreate.Inserter failing = batch ->
        {
            calls.incrementAndGet();
            throw new IllegalStateException("Weaviate is down");
        };

        var e = assertThrows(
            IllegalStateException.class,
            () -> BatchCreate.sendFixedSize(numbered(100_000, consumed), 10, 4, failing, PLAIN, new BatchCreate.Result())
        );

        assertThat(e.getMessage(), is("Weaviate is down"));
        // At most `concurrency` requests are in flight when the first one fails, plus the one that may be waiting for a permit.
        assertThat(calls.get(), lessThanOrEqualTo(5));
        assertThat(consumed.get(), lessThan(100_000));
    }

    @Test
    void parallelFixedSizeSurfacesALateFailure() {
        var calls = new AtomicInteger();
        BatchCreate.Inserter failsLast = batch ->
        {
            if (calls.incrementAndGet() == 10) {
                throw new IllegalArgumentException("last chunk");
            }
            return accepting(List.of()).insert(batch);
        };

        var e = assertThrows(
            IllegalArgumentException.class,
            () -> BatchCreate.sendFixedSize(numbered(100, new AtomicInteger()), 10, 3, failsLast, PLAIN, new BatchCreate.Result())
        );
        assertThat(e.getMessage(), is("last chunk"));
    }

    @Test
    void parallelFixedSizeCountsEveryObjectAndOrdersErrors() throws Exception {
        var result = new BatchCreate.Result();
        var rejected = IntStream.range(0, 1000).filter(i -> i % 3 == 0).boxed().toList();

        BatchCreate.sendFixedSize(numbered(1000, new AtomicInteger()), 7, 8, accepting(rejected), PLAIN, result);
        BatchCreate.finishErrors(result);

        assertThat(result.inserted, is(1000L - rejected.size()));
        assertThat(result.failed, is((long) rejected.size()));
        assertThat(result.requests, is(143L));
        assertThat(
            result.errors.stream().map(BatchCreate.ObjectError::getIndex).toList(),
            is(rejected.stream().limit(BatchCreate.MAX_REPORTED_ERRORS).map(Integer::longValue).toList())
        );
    }

    @Test
    void sequentialFixedSizeStopsAtTheFirstFailure() {
        var calls = new AtomicInteger();
        BatchCreate.Inserter failing = batch ->
        {
            calls.incrementAndGet();
            throw new IllegalStateException("down");
        };

        assertThrows(IllegalStateException.class, () -> BatchCreate.sendFixedSize(numbered(100, new AtomicInteger()), 10, 1, failing, PLAIN, new BatchCreate.Result()));
        assertThat(calls.get(), is(1));
    }

    @Test
    void errorsStayBoundedWhileCollecting() {
        var result = new BatchCreate.Result();
        for (var i = 0; i < 10_000; i++) {
            BatchCreate.addError(result, new BatchCreate.ObjectError(10_000 - i, null, "e"));
            assertThat(result.errors.size(), lessThan(2 * BatchCreate.MAX_REPORTED_ERRORS));
        }
        BatchCreate.finishErrors(result);

        assertThat(result.errors.size(), is(BatchCreate.MAX_REPORTED_ERRORS));
        assertThat(result.errors.getFirst().getIndex(), is(1L));
        assertThat(result.errors.getLast().getIndex(), is((long) BatchCreate.MAX_REPORTED_ERRORS));
    }

    @Test
    void validatesBatchSize() {
        assertThrows(IllegalArgumentException.class, () -> BatchCreate.validate(BatchMode.FIXED_SIZE, 0, 1, null));
        assertThrows(IllegalArgumentException.class, () -> BatchCreate.validate(BatchMode.FIXED_SIZE, 10_001, 1, null));
        assertThrows(IllegalArgumentException.class, () -> BatchCreate.validate(BatchMode.FIXED_SIZE, 10, 33, null));
        assertThrows(IllegalArgumentException.class, () -> BatchCreate.validate(BatchMode.FIXED_SIZE, 10, 0, null));
        assertThrows(IllegalArgumentException.class, () -> BatchCreate.validate(BatchMode.RATE_LIMIT, 10, 1, 0));
        BatchCreate.validate(BatchMode.FIXED_SIZE, 10_000, 32, null);
        BatchCreate.validate(BatchMode.RATE_LIMIT, 1, 1, 1);
    }

    @Test
    void autoIsResolvedBeforeSending() {
        assertThrows(IllegalStateException.class, () -> BatchCreate.sender(BatchMode.AUTO, 10, 1, null));
    }

    @Test
    void parsesServerVersions() {
        assertThat(BatchCreate.supportsServerSide("1.36.0"), is(true));
        assertThat(BatchCreate.supportsServerSide("v1.39.7"), is(true));
        assertThat(BatchCreate.supportsServerSide("2.0.0-rc.1"), is(true));
        assertThat(BatchCreate.supportsServerSide("1.35.9"), is(false));
        assertThat(BatchCreate.supportsServerSide("garbage"), is(false));
        assertThat(BatchCreate.supportsServerSide(null), is(false));
    }

    @Test
    void convertsSingleAndMultiVectors() {
        var single = new ObjectConverter(null, "v", null).toObject(new LinkedHashMap<>(Map.of("v", List.of(1, 2.5), "a", "x")));
        assertThat(single.vectors().getDefaultSingle(), is(new float[] {1f, 2.5f}));
        assertThat(single.properties(), is(Map.of("a", "x")));

        var multi = new ObjectConverter(null, "v", "colbert").toObject(new LinkedHashMap<>(Map.of("v", List.of(List.of(1, 2), List.of(3, 4)))));
        assertThat(multi.vectors().getMulti("colbert"), is(new float[][] {{1f, 2f}, {3f, 4f}}));
    }

    @Test
    void rejectsMalformedVectors() {
        var converter = new ObjectConverter(null, "v", null);
        for (Object bad : List.of(List.of(), "not a list", List.of(1, "two"), List.of(List.of(1), 2))) {
            var input = new LinkedHashMap<String, Object>();
            input.put("v", bad);
            var e = assertThrows(IllegalArgumentException.class, () -> converter.toObject(input), "vector " + bad);
            assertThat(e.getMessage(), containsString("`v`"));
        }
    }

    @Test
    void keepsTheGivenUuidAndGeneratesOneOtherwise() {
        var id = UUID.randomUUID().toString();
        var converter = new ObjectConverter("id", null, null);

        var given = converter.toObject(new LinkedHashMap<>(Map.of("id", id.toUpperCase(), "a", 1)));
        assertThat(given.uuid(), is(id));
        assertThat(given.properties(), is(Map.of("a", 1)));

        var generated = converter.toObject(new LinkedHashMap<>(Map.of("a", 1)));
        assertThat(UUID.fromString(generated.uuid()), notNullValue());
    }

    @Test
    void convertsOtherValueTypes() {
        var uuid = UUID.randomUUID();
        var list = new ArrayList<Object>();
        list.add("a");
        list.add(null);
        list.add(3);
        var input = new LinkedHashMap<String, Object>();
        input.put("uuid", uuid);
        input.put("date", Date.from(java.time.Instant.parse("2024-01-01T10:00:00Z")));
        input.put("local", LocalDateTime.parse("2024-01-01T10:00:00"));
        input.put("list", list);
        input.put("missing", null);
        input.put("other", new StringBuilder("text"));

        var converted = BatchCreate.convertProperties(input);

        assertThat(converted.get("uuid"), is(uuid));
        assertThat(converted.get("date"), is("2024-01-01T10:00:00Z"));
        assertThat(converted.get("local"), is("2024-01-01T10:00:00Z"));
        assertThat(converted.get("list"), is(List.of("a", 3)));
        assertThat(converted.containsKey("missing"), is(false));
        assertThat(converted.get("other"), is("text"));
    }
}
