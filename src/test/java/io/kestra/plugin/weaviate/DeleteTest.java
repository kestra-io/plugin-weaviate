package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.FilterCondition.Operator;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DeleteTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    private RunContext seed() throws Exception {
        RunContext runContext = runContextFactory.of();
        batchCreate()
            .objects(
                List.of(
                    Map.of("title", "success", "description", "first description", "length", 10, "bool", true),
                    Map.of("title", "success", "description", "second description", "length", 100, "bool", true),
                    Map.of("title", "success", "description", "second description", "length", 1000, "bool", false)
                )
            )
            .build()
            .run(runContext);
        return runContext;
    }

    @Test
    public void deletesByRenderedId() throws Exception {
        RunContext seeded = seed();
        String id = (String) fetchAll(seeded).getFirst().get("uuid");
        RunContext runContext = runContextFactory.of(Map.of("id", id));

        Delete.Output output = delete().objectId(Property.ofExpression("{{ id }}")).build().run(runContext);

        assertThat(output.getSuccess(), is(true));
        assertThat(output.getClassName(), is(CLASS_NAME));
        assertThat(output.getDeletedCount(), is(1L));
        assertThat(output.getIds(), contains(id));
        assertThat(fetchAll(runContext), hasSize(2));
    }

    @Test
    public void deleteByMissingIdReportsFailure() throws Exception {
        RunContext runContext = seed();

        Delete.Output output = delete().objectId(Property.ofValue("00000000-0000-0000-0000-00000000dead")).build().run(runContext);

        assertThat(output.getSuccess(), is(false));
        assertThat(output.getDeletedCount(), is(0L));
    }

    @Test
    public void deletesBySimpleFilter() throws Exception {
        RunContext runContext = seed();

        Delete.Output output = delete()
            .filter(Property.ofValue(Map.of("title", "success", "description", "* description", "length", 10, "bool", true)))
            .build()
            .run(runContext);

        assertThat(output.getSuccess(), is(true));
        assertThat(output.getDeletedCount(), is(1L));
        assertThat(output.getIds(), hasSize(1));
        assertThat(fetchAll(runContext), hasSize(2));

        output = delete().filter(Property.ofValue(Map.of("title", "success"))).build().run(runContext);
        assertThat(output.getDeletedCount(), is(2L));
        assertThat(fetchAll(runContext), empty());
    }

    @Test
    public void deletesByStructuredFilters() throws Exception {
        RunContext runContext = seed();

        Delete.Output output = delete()
            .filters(
                Property.ofValue(
                    List.of(
                        FilterCondition.builder().property("length").operator(Operator.GREATER_THAN).value(50).build(),
                        FilterCondition.builder().property("bool").operator(Operator.EQUAL).value(true).build()
                    )
                )
            )
            .build()
            .run(runContext);

        assertThat(output.getDeletedCount(), is(1L));
        assertThat(fetchAll(runContext), hasSize(2));
    }

    @Test
    public void requiresDeleteAllWithoutFilter() throws Exception {
        RunContext runContext = seed();

        var e = assertThrows(IllegalArgumentException.class, () -> delete().build().run(runContext));
        assertThat(e.getMessage(), containsString("deleteAll: true"));
        assertThat(fetchAll(runContext), hasSize(3));

        Delete.Output output = delete().deleteAll(Property.ofValue(true)).build().run(runContext);
        assertThat(output.getDeletedCount(), is(3L));
        assertThat(fetchAll(runContext), empty());
    }

    @Test
    public void rejectsAmbiguousCombinations() {
        assertThrows(IllegalArgumentException.class, () -> Delete.resolveFilter("id", WeaviateFilters.build(Map.of("a", 1), null, null), false));
        assertThrows(IllegalArgumentException.class, () -> Delete.resolveFilter("id", null, true));
        assertThrows(IllegalArgumentException.class, () -> Delete.resolveFilter(null, WeaviateFilters.build(Map.of("a", 1), null, null), true));
    }

    @Test
    public void deletesMoreThanQueryMaximumResults() throws Exception {
        // docker-compose-ci.yml sets QUERY_MAXIMUM_RESULTS=100, so one deleteMany call cannot remove all objects.
        RunContext runContext = runContextFactory.of();
        batchCreate()
            .objects(IntStream.range(0, 250).mapToObj(i -> Map.<String, Object> of("n", i)).toList())
            .build()
            .run(runContext);

        Delete.Output output = delete().deleteAll(Property.ofValue(true)).build().run(runContext);

        assertThat(output.getDeletedCount(), is(250L));
        assertThat(output.getIds(), hasSize(250));
        assertThat(output.getSuccess(), is(true));
        try (var client = client()) {
            assertThat(client.collections.use(CLASS_NAME).size(), is(0L));
        }
    }
}
