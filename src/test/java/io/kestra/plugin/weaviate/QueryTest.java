package io.kestra.plugin.weaviate;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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

    @Test
    public void testQuery() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "QueryTest";
        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .parameters(parameters)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCounts(), greaterThanOrEqualTo(1L));

        assertThat(batchOutput.getClassName(), Matchers.hasItem(className));
        assertThat(batchOutput.getProperties(), is(parameters));

        RawQuery.Output queryOutput = RawQuery.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query(QUERY)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), greaterThanOrEqualTo(1));

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
        assertThat(deleteOutput.getDeletedCounts(), greaterThanOrEqualTo(1L));
    }
}
