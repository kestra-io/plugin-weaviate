package io.kestra.plugin.weaviate;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;

import io.weaviate.client6.v1.api.collections.Property;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BatchCreateTest extends WeaviateTest {
    private static final String ID = "0b9e8a4a-3b1c-4c1e-9f7a-1c2d3e4f5a6b";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    public void insertsRenderedInlineObjects() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("title", "test success"));

        BatchCreate.Output output = batchCreate()
            .objects(List.of(Map.of("title", "{{title}}")))
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(1L));
        assertThat(output.getFailedCount(), is(0L));
        assertThat(output.getErrors(), empty());

        FetchOutput stored = query().fetchType(io.kestra.core.models.property.Property.ofValue(FetchType.STORE)).build().run(runContext);
        assertThat(stored.getSize(), is(1L));
        List<Map> rows = readObjectsFromStream(runContext.storage().getFile(stored.getUri()));
        assertThat(rows.getFirst().get("properties"), is(Map.of("title", "test success")));
    }

    @Test
    public void insertsObjectsFromIonFile() throws Exception {
        String fileName = "weaviate-objects.ion";
        URL resource = BatchCreate.class.getClassLoader().getResource(fileName);
        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + fileName),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );
        RunContext runContext = runContextFactory.of(Map.of("uri", uri.toString()));

        BatchCreate.Output output = batchCreate().objects("{{uri}}").build().run(runContext);

        assertThat(output.getInsertedCount(), is(2L));
        assertThat(fetchAll(runContext).stream().map(r -> properties(r).get("title")).toList(), containsInAnyOrder("a", "b"));
    }

    @Test
    public void usesUuidAndVectorKeys() throws Exception {
        RunContext runContext = runContextFactory.of();

        batchCreate()
            .uuidKey(io.kestra.core.models.property.Property.ofValue("id"))
            .vectorKey(io.kestra.core.models.property.Property.ofValue("embedding"))
            .objects(List.of(Map.of("id", ID, "title", "with vector", "embedding", List.of(0.1, 0.2, 0.3))))
            .build()
            .run(runContext);

        FetchOutput output = query().includeVector(io.kestra.core.models.property.Property.ofValue(true)).build().run(runContext);

        @SuppressWarnings("unchecked")
        Map<String, Object> row = (Map<String, Object>) output.getRows().getFirst();
        assertThat(row.get("uuid"), is(ID));
        assertThat(properties(row), is(Map.of("title", "with vector")));
        assertThat(row.get("vector"), is(List.of(0.1f, 0.2f, 0.3f)));
    }

    @Test
    public void splitsIntoBatches() throws Exception {
        RunContext runContext = runContextFactory.of();

        BatchCreate.Output output = batchCreate()
            .batchSize(io.kestra.core.models.property.Property.ofValue(2))
            .objects(List.of(Map.of("n", 1), Map.of("n", 2), Map.of("n", 3), Map.of("n", 4), Map.of("n", 5)))
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(5L));
        assertThat(fetchAll(runContext), hasSize(5));
    }

    @Test
    public void reportsRejectedObjects() throws Exception {
        createSelfProvidedCollection(Property.integer("count"));
        RunContext runContext = runContextFactory.of();
        List<Map<String, Object>> objects = List.of(Map.of("count", 1), Map.of("count", "not a number"), Map.of("count", 3));

        var e = assertThrows(IllegalStateException.class, () -> batchCreate().objects(objects).build().run(runContext));
        assertThat(e.getMessage(), containsString("1 of 3 objects were rejected"));

        BatchCreate.Output output = batchCreate()
            .failOnError(io.kestra.core.models.property.Property.ofValue(false))
            .objects(objects)
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(2L));
        assertThat(output.getFailedCount(), is(1L));
        assertThat(output.getErrors(), hasSize(1));
        assertThat(output.getErrors().getFirst().getIndex(), is(1L));
        assertThat(output.getErrors().getFirst().getUuid(), notNullValue());
        assertThat(output.getErrors().getFirst().getMessage(), not(emptyOrNullString()));
    }

    @Test
    public void rejectsInvalidUuid() {
        RunContext runContext = runContextFactory.of();

        assertThrows(
            IllegalArgumentException.class, () -> batchCreate()
                .uuidKey(io.kestra.core.models.property.Property.ofValue("id"))
                .objects(List.of(Map.of("id", "not-a-uuid")))
                .build()
                .run(runContext)
        );
    }

    @Test
    public void convertsIonValues() {
        Map<String, Object> converted = BatchCreate.convertProperties(
            new java.util.LinkedHashMap<>(
                Map.of(
                    "when", java.time.Instant.parse("2024-01-01T00:00:00Z"),
                    "day", java.time.LocalDate.parse("2024-01-02"),
                    "nested", Map.of("at", java.time.ZonedDateTime.parse("2024-01-03T00:00:00Z"))
                )
            )
        );

        assertThat(converted.get("when"), is("2024-01-01T00:00:00Z"));
        assertThat(converted.get("day"), is("2024-01-02T00:00:00Z"));
        assertThat(converted.get("nested"), is(Map.of("at", "2024-01-03T00:00:00Z")));
    }

    @Test
    public void insertsWholeMinuteDates() throws Exception {
        createSelfProvidedCollection(Property.date("seen"));
        RunContext runContext = runContextFactory.of();

        BatchCreate.Output output = batchCreate()
            .objects(List.of(Map.of("seen", java.time.Instant.parse("2024-01-01T00:00:00Z"))))
            .build()
            .run(runContext);

        assertThat(output.getInsertedCount(), is(1L));
        assertThat(properties(fetchAll(runContext).getFirst()).get("seen"), is("2024-01-01T00:00:00Z"));
    }
}
