package io.kestra.plugin.weaviate;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
public class DeleteTest {

    public static final String SCHEME = "http";
    public static final String HOST = "localhost:8080";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void testDeleteById() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "DeleteTest";
        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(parameters)
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(1));

        Query.Output queryOutput = Query.builder()
            .scheme(SCHEME)
            .host(HOST)
            .query("""
                   {
                          Get {
                            %s (
                              limit: 50
                            ) {
                              _additional {
                                id
                              }
                            }
                          }
                        }
                   """.formatted(className))
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(1));

        Map<String, Object> stringObjectMap = (Map<String, Object>) ((List<Object>) queryOutput.getData().get(className)).get(0);
        String id = (String) ((Map<String, Object>) stringObjectMap.get("_additional")).get("id");

        Delete.Output deleteOutput = Delete.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .id(id)
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(className, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCounts(), is(1L));
    }

    @Test
    public void testDeleteByExpression() throws Exception {
        RunContext runContext = runContextFactory.of();

        String className = "DeleteTest";
        Map<String, String> parameters = Map.of("title", "test success");

        BatchCreate.Output batchOutput = BatchCreate.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .objects(List.of(parameters))
            .build()
            .run(runContext);

        assertThat(batchOutput.getCreatedCount(), is(1));

        Delete.Output deleteOutput = Delete.builder()
            .scheme(SCHEME)
            .host(HOST)
            .className(className)
            .properties(parameters)
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(className, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCounts(), is(1L));
    }

}
