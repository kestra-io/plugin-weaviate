package io.kestra.plugin.weaviate;

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

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .url(URL)
            .className(CLASS_NAME)
            .objects(objectsToCreate)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(1));
        assertThat(batchOutput.getUri(), notNullValue());

        assertThat(readObjectsFromStream(runContext.uriToInputStream(batchOutput.getUri())), is(List.of(Map.of("title", "test success"))));
    }

    @Test
    public void testBatchCreateWithUri() throws Exception {

        String fileName = "weaviate-objects.ion";
        URL resource = BatchCreate.class.getClassLoader().getResource(fileName);

        URI uri = storageInterface.put(
            null,
            new URI("/" + fileName),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        RunContext runContext = runContextFactory.of(Map.of("uri", uri.toString()));
        BatchCreate.Output batchOutput = BatchCreate.builder()
            .url(URL)
            .className(CLASS_NAME)
            .objects("{{uri}}")
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(2));
        assertThat(batchOutput.getUri(), notNullValue());

        assertThat(readObjectsFromStream(runContext.uriToInputStream(batchOutput.getUri())), is(readObjectsFromStream(resource.openStream())));
    }
}