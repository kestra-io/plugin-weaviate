package io.kestra.plugin.weaviate;

import com.google.common.io.CharStreams;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@MicronautTest
public class QueryTest {

    public static final String SCHEME = "http";
    public static final String HOST = "localhost:8080";
    private static final String QUERY = """
                       {
  Get {
    QueryTest (
      limit: 50
    ) {
      title
    }
  }
}
                       """;

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
    public void testQuery_Without_Internal_Storage() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "QueryTest";
        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(parameters)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCounts(), is(1));

        assertThat(batchOutput.getClassName(), Matchers.hasItem(className));
        assertThat(batchOutput.getProperties(), is(parameters));

        RawQuery.Output queryOutput = RawQuery.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query(QUERY)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(1));

        assertThat(parameters, Matchers.containsInAnyOrder(((List<Object>) queryOutput.getData().get(className)).get(0)));

        Delete.Output deleteOutput = Delete.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .properties(Map.of("title", "test success"))
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(className, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCounts(), is(1L));
    }

    @Test
    public void testQuery_With_Internal_Storage() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "QueryTest";
        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(parameters)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCounts(), is(1));

        assertThat(batchOutput.getClassName(), Matchers.hasItem(className));
        assertThat(batchOutput.getProperties(), is(parameters));

        RawQuery.Output queryOutput = RawQuery.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query(QUERY)
            .store(true)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(1));

        assertThat(parameters, Matchers.containsInAnyOrder(((List<Object>) queryOutput.getData().get(className)).get(0)));

        String outputFileContent = IOUtils.toString(storageInterface.get(queryOutput.getUri()), Charsets.UTF_8);
        Map rows = JacksonMapper.ofIon().readValue(outputFileContent, Map.class);
        assertThat(rows.get(className), is(queryOutput.getData().get(className)));

        Delete.Output deleteOutput = Delete.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .properties(Map.of("title", "test success"))
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(className, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCounts(), is(1L));
    }



    @Test
    public void testQuery_From_URI() throws Exception {
        RunContext runContext = runContextFactory.of();

        String prefix = IdUtils.create();

        URL resource = QueryTest.class.getClassLoader().getResource("flows/query.yml");
        String content = CharStreams.toString(new InputStreamReader(new FileInputStream(Objects.requireNonNull(resource)
            .getFile())));

        URI uri = this.putFile(resource, "/" + prefix + "/storage/query.yml");

        String className = "QueryTest";
        List<Map<String, Object>> parameters = List.of(JacksonMapper.ofYaml().readValue(content, Map.class));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(uri.getPath())
            .storageInterface(storageInterface)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCounts(), is(1));

        assertThat(batchOutput.getClassName(), Matchers.hasItem(className));
        assertThat(batchOutput.getProperties(), is(parameters));

        RawQuery.Output queryOutput = RawQuery.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query(QUERY)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(1));

        assertThat(parameters, Matchers.contains(((List<Object>) queryOutput.getData().get(className)).get(0)));

        Delete.Output deleteOutput = Delete.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .properties(Map.of("title", parameters.get(0).get("title").toString()))
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(className, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCounts(), is(1L));
    }
}
