package io.kestra.plugin.weaviate;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BatchCreateTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    public void testBatchCreateWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("title", "test success"));

        List<Map<String, Object>> objectsToCreate = List.of(Map.of("title", "{{title}}"));

        BatchCreate.builder()
            .url(URL)
            .className(Property.of(CLASS_NAME))
            .objects(objectsToCreate)
            .build()
            .run(runContext);

        String query = """
                           {
                             Get {
                               %s {
                                   title
                               }
                             }
                           }""".formatted(CLASS_NAME);

        FetchOutput output = Query.builder().fetchType(Property.of(FetchType.STORE)).url(URL).query(query).build().run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getUri(), notNullValue());

        assertThat(readObjectsFromStream(runContext.storage().getFile(output.getUri())), is(List.of(Map.of("WeaviateTest", Map.of("title", "test success")))));
    }

    @Test
    public void testBatchCreateWithUri() throws Exception {

        String fileName = "weaviate-objects.ion";
        URL resource = BatchCreate.class.getClassLoader().getResource(fileName);

        URI uri = storageInterface.put(
            TenantService.MAIN_TENANT,
            null,
            new URI("/" + fileName),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(Map.of("uri", uri.toString()));
        VoidOutput batchOutput = BatchCreate.builder()
            .url(URL)
            .className(Property.of(CLASS_NAME))
            .objects("{{uri}}")
            .build()
            .run(runContext);

        String query = """
                           {
                             Get {
                               %s {
                                   title
                               }
                             }
                           }""".formatted(CLASS_NAME);

        FetchOutput output = Query.builder().fetchType(Property.of(FetchType.STORE)).url(URL).query(query).build().run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getUri(), notNullValue());

        List<Map> actual = readObjectsFromStream(runContext.storage().getFile(output.getUri())).stream().map(map -> (Map) map.get("WeaviateTest")).toList();
        List<Map> maps = readObjectsFromStream(resource.openStream());
        assertThat(actual.containsAll(maps) && maps.containsAll(actual), is(true));
    }
}