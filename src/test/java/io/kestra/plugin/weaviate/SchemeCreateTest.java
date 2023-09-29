package io.kestra.plugin.weaviate;

import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class SchemeCreateTest {

    public static final String SCHEME = "http";
    public static final String HOST = "localhost:8080";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void testSchemeCreate() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "TestSchema_"+ UUID.randomUUID().toString().replace("-", "").toLowerCase();
        Map<String, List<String>> parameters = Map.of("title", List.of("text"));

        SchemaCreate.Output schemeOutput = SchemaCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .parameters(parameters)
            .build()
            .run(runContext);

        assertThat(schemeOutput.getSuccess(), is(Boolean.TRUE));

        Query.Output queryOutput = Query.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query("""
                   {
                          Get {
                            %s (
                              limit: 10
                            ) {
                              title
                            }
                          }
                        }
                   """.formatted(className))
            .fetchType(FetchType.FETCH_ONE)
            .build()
            .run(runContext);

        Map<String, Object> data = queryOutput.getRow();
        assertThat((List<?>) data.get(className), empty());
    }
}
