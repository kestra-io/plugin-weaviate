package io.kestra.plugin.weaviate;

import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class BatchCreateTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    public void testBatchCreateWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of();

        List<Map<String, Object>> objectsToCreate = List.of(Map.of("title", "test success"));

        BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(CLASS_NAME)
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

        FetchOutput output = Query.builder().fetchType(FetchType.STORE).scheme(SCHEME).host(HOST).query(query).build().run(runContext);

        assertThat(output.getSize(), is(1L));
        assertThat(output.getUri(), notNullValue());

        assertThat(readObjectsFromStream(runContext.uriToInputStream(output.getUri())).get(0), is(Map.of("WeaviateTest", objectsToCreate.get(0))));
    }

    @Test
    public void testBatchCreateWithUri() throws Exception {
        RunContext runContext = runContextFactory.of();

        String fileName = "weaviate-objects.ion";
        URL resource = BatchCreate.class.getClassLoader().getResource(fileName);

        URI uri = storageInterface.put(
            null,
            new URI("/" + fileName),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
                                      );

        BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(CLASS_NAME)
            .objects(uri.toString())
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

        FetchOutput output = Query.builder().fetchType(FetchType.STORE).scheme(SCHEME).host(HOST).query(query).build().run(runContext);

        assertThat(output.getSize(), is(2L));
        assertThat(output.getUri(), notNullValue());

        List<Map> actual = readObjectsFromStream(runContext.uriToInputStream(output.getUri())).stream().map(map -> (Map) map.get("WeaviateTest")).toList();
        List<Map> maps = readObjectsFromStream(resource.openStream());
        assertThat(actual, is(maps));
    }
}