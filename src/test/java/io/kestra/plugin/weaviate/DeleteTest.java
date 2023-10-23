package io.kestra.plugin.weaviate;

import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class DeleteTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void testDeleteById() throws Exception {
        RunContext runContext = runContextFactory.of();

        List<Map<String, Object>> parameters = List.of(Map.of("title", "test success"));

        VoidOutput batchOutput = BatchCreate.builder()
            .url(URL)
            .className(CLASS_NAME)
            .objects(parameters)
            .build()
            .run(runContext);

        FetchOutput queryOutput = Query.builder()
            .url(URL)
            .query("""
                   {
                          Get {
                            %s {
                              _additional {
                                id
                              }
                            }
                          }
                        }
                   """.formatted(CLASS_NAME))
            .fetchType(FetchType.FETCH_ONE)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(1L));

        String id = (String) ((Map<String, Object>) queryOutput.getRow().get("_additional")).get("id");

        Delete.Output deleteOutput = Delete.builder()
            .url(URL)
            .className(CLASS_NAME)
            .id(id)
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(CLASS_NAME, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCount(), is(1L));

        queryOutput = Query.builder()
            .url(URL)
            .query("""
                   {
                     Get {
                         %s {
                           _additional {
                             id
                           }
                         }
                     }
                   }
                """.formatted(CLASS_NAME))
            .fetchType(FetchType.FETCH)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(0L));
    }

    @Test
    public void testDeleteByExpression() throws Exception {
        RunContext runContext = runContextFactory.of();

        var createdObjects = List.of(
            Map.of(
                "title", "success",
                "description", "first description",
                "length", 10,
                "bool", true
            ),
            Map.of(
                "title", "success",
                "description", "second description",
                "length", 100,
                "bool", true
            ),
            Map.of(
                "title", "success",
                "description", "second description",
                "length", 1000,
                "bool", false
            )
        );

        VoidOutput batchOutput = BatchCreate.builder()
            .url(URL)
            .className(CLASS_NAME)
            .objects(createdObjects)
            .build()
            .run(runContext);

        Delete.Output deleteOutput = Delete.builder()
            .url(URL)
            .className(CLASS_NAME)
            .filter(Map.of(
                "title", "success",
                "description", "* description",
                "length", 10,
                "bool", true
            ))
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(CLASS_NAME, is(deleteOutput.getClassName()));
        assertThat(deleteOutput.getDeletedCount(), is(1L));

        FetchOutput queryOutput = Query.builder()
            .url(URL)
            .query("""
                   {
                     Get {
                         %s {
                           _additional {
                             id
                           }
                         }
                     }
                   }
                """.formatted(CLASS_NAME))
            .fetchType(FetchType.FETCH)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(2L));

        deleteOutput = Delete.builder()
            .url(URL)
            .className(CLASS_NAME)
            .filter(Map.of(
                "title", "success"
            ))
            .build()
            .run(runContext);

        assertThat(true, is(deleteOutput.getSuccess()));

        assertThat(deleteOutput.getDeletedCount(), is(2L));

        queryOutput = Query.builder()
            .url(URL)
            .query("""
                   {
                     Get {
                         %s {
                           _additional {
                             id
                           }
                         }
                     }
                   }
                """.formatted(CLASS_NAME))
            .fetchType(FetchType.FETCH)
            .build()
            .run(runContext);

        assertThat(queryOutput.getSize(), is(0L));
    }
}
