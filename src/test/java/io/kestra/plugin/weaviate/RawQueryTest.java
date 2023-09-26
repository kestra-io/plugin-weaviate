package io.kestra.plugin.weaviate;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

@MicronautTest
public class RawQueryTest {

    public static final String SCHEME = "http";
    public static final String HOST = "localhost:8080";
    private static final String QUERY = """
                       {
  Get {
    Article (
      limit: 50
    ) {
      title
      body
    }
  }
}
                       """;

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void testQuery() throws Exception {
        RunContext runContext = runContextFactory.of();

        RawQuery.Output output = RawQuery.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query(QUERY)
            .build()
            .run(runContext);

        Map<String, Object> data = output.getData();
        int rows = output.getSize();
    }
}
