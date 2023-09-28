package io.kestra.plugin.weaviate;

import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class BatchCreateTest {

    public static final String SCHEME = "http";
    public static final String HOST = "localhost:8080";

    @Inject
    private RunContextFactory runContextFactory;

    @Inject
    private StorageInterface storageInterface;

    private URI putFile(URL resource, String path) throws Exception {
        return storageInterface.put(
            new URI(path),
            new FileInputStream(Objects.requireNonNull(resource).getFile())
                                   );
    }

    @Test
    public void testBatchCreateWithParameters() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "BatchTest";
        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(parameters)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCounts(), is(1));
        assertThat(batchOutput.getClassName(), contains(className));
        assertThat(batchOutput.getProperties(), is(parameters));
        assertThat(batchOutput.getUri(), notNullValue());
    }

    @Test
    public void testBatchCreateWithUri() throws Exception {
        RunContext runContext = runContextFactory.of();

        String prefix = IdUtils.create();

        URL resource = BatchCreate.class.getClassLoader().getResource("flows/query.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource)
            .getFile())));

        URI uri = this.putFile(resource, "/" + prefix + "/storage/query.yml");

        String className = "BatchTest";
        List<Map<String, Object>> parameters = List.of(JacksonMapper.ofYaml().readValue(content, Map.class));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .storageInterface(storageInterface)
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(uri.toString())
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCounts(), is(1));
        assertThat(batchOutput.getClassName(), contains(className));
        assertThat(batchOutput.getProperties(), is(parameters));
        assertThat(batchOutput.getUri(), notNullValue());
        assertThat(batchOutput.getUri(), is(uri));
    }

}