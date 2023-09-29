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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
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

        String className = "BatchTest_Parameters";
        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(parameters)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(1));
        assertThat(batchOutput.getUri(), notNullValue());

        List<Map<String, Object>> parametersFromFile = new ArrayList<>();
        InputStream inputStream = runContext.uriToInputStream(batchOutput.getUri());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String content = CharStreams.toString(new InputStreamReader(inputStream));
            parametersFromFile = List.of(JacksonMapper.ofIon().readValue(content, Map.class));
        }

        assertThat(parametersFromFile, is(parameters));
    }

    @Test
    public void testBatchCreateWithUri() throws Exception {
        RunContext runContext = runContextFactory.of();

        String prefix = IdUtils.create();

        URL resource = BatchCreate.class.getClassLoader().getResource("application.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource)
            .getFile())));

        URI uri = this.putFile(resource, "/" + prefix + "/storage/query.yml");

        String className = "BatchTest_URI";
        List<Map<String, Object>> parameters = List.of(JacksonMapper.ofYaml().readValue(content, Map.class));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(uri.toString())
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(1));
        assertThat(batchOutput.getUri(), notNullValue());

        List<Map<String, Object>> parametersFromFile = new ArrayList<>();
        InputStream inputStream = runContext.uriToInputStream(batchOutput.getUri());
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            content = CharStreams.toString(new InputStreamReader(inputStream));
            parametersFromFile = List.of(JacksonMapper.ofIon().readValue(content, Map.class));
        }

        assertThat(parametersFromFile, is(parameters));
    }

}