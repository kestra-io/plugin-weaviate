package io.kestra.plugin.weaviate;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@MicronautTest
public class BatchCreateTest {

    public static final String SCHEME = "http";
    public static final String HOST = "localhost:8080";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    @Test
    public void testBatchCreateWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "BatchTest_Parameters";
        List<Map<String, Object>> objectsToCreate = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(objectsToCreate)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(1));
        assertThat(batchOutput.getUri(), notNullValue());

        assertThat(readObjectsFromStream(runContext.uriToInputStream(batchOutput.getUri())), is(objectsToCreate));
    }

    @Test
    public void testBatchCreateWithUri() throws Exception {
        RunContext runContext = runContextFactory.of();

        String fileName = "weaviate-objects.ion";
        URL resource = BatchCreate.class.getClassLoader().getResource(fileName);

        URI uri = storageInterface.put(
            new URI("/" + fileName),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
        );

        String className = "BatchTest_URI";

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(uri.toString())
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(2));
        assertThat(batchOutput.getUri(), notNullValue());

        assertThat(readObjectsFromStream(runContext.uriToInputStream(batchOutput.getUri())), is(readObjectsFromStream(resource.openStream())));
    }

    private List<Map> readObjectsFromStream(InputStream inputStream) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return Flowable.create(FileSerde.reader(reader, Map.class), BackpressureStrategy.BUFFER).toList().blockingGet();
        }
    }
}