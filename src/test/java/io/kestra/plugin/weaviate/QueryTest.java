package io.kestra.plugin.weaviate;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class QueryTest extends WeaviateTest {
    private static final String QUERY = """
        {
           Get {
             %s {
                 title
             }
           }
         }
        """;

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    public void testQueryFetchOne() throws Exception {
        RunContext runContext = runContextFactory.of();

        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.builder()
            .url(URL)
            .className(Property.of(CLASS_NAME))
            .objects(parameters)
            .build()
            .run(runContext);

        FetchOutput queryOutput = Query.builder()
            .url(URL)
            .query("""
                {
                       Get {
                         %s (
                           limit: 50
                         ) {
                           title
                           _additional {
                             id
                           }
                         }
                       }
                     }
                """.formatted(CLASS_NAME))
            .fetchType(Property.of(FetchType.FETCH_ONE))
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(1L));
        assertThat(queryOutput.getRows(), is(nullValue()));
        assertThat(queryOutput.getUri(), is(nullValue()));

        assertThat(((Map<String, Object>) queryOutput.getRow().get("_additional")).get("id"), notNullValue());
        assertThat(queryOutput.getRow().get("title"), is("test success"));
    }

    @Test
    public void testQueryFetch() throws Exception {
        RunContext runContext = runContextFactory.of();

        List<Map<String, Object>> objectsToCreate = List.of(
            Map.of("title", "test success"),
            Map.of("title", "test success 2")
        );

        BatchCreate.builder()
            .url(URL)
            .className(Property.of(CLASS_NAME))
            .objects(objectsToCreate)
            .build()
            .run(runContext);

        FetchOutput queryOutput = Query.builder()
            .url(URL)
            .query(QUERY.formatted(CLASS_NAME))
            .fetchType(Property.of(FetchType.FETCH))
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(2L));
        assertThat(queryOutput.getRow(), is(nullValue()));
        assertThat(queryOutput.getUri(), is(nullValue()));

        assertThat(queryOutput.getRows(), containsInAnyOrder(
            Map.entry(CLASS_NAME, Map.of("title", "test success")),
            Map.entry(CLASS_NAME, Map.of("title", "test success 2"))
        ));
    }

    @Test
    public void testQueryStore() throws Exception {
        RunContext runContext = runContextFactory.of();

        List<Map<String, Object>> objectsToCreate = List.of(
            Map.of("title", "test success"),
            Map.of("title", "test success 2")
        );

        BatchCreate.builder()
            .url(URL)
            .className(Property.of(CLASS_NAME))
            .objects(objectsToCreate)
            .build()
            .run(runContext);

        FetchOutput queryOutput = Query.builder()
            .url(URL)
            .query(QUERY.formatted(CLASS_NAME))
            .fetchType(Property.of(FetchType.STORE))
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(2L));
        assertThat(queryOutput.getRow(), is(nullValue()));
        assertThat(queryOutput.getRows(), is(nullValue()));

        var queryOutputContent = readObjectsFromStream(storageInterface.get(TenantService.MAIN_TENANT, null, queryOutput.getUri()));

        assertThat(queryOutputContent, containsInAnyOrder(
            Map.of(CLASS_NAME, Map.of("title", "test success")),
            Map.of(CLASS_NAME, Map.of("title", "test success 2"))
        ));
    }
}
