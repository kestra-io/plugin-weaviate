package io.kestra.plugin.weaviate;

import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.schema.model.Property;
import io.weaviate.client.v1.schema.model.Tokenization;
import io.weaviate.client.v1.schema.model.WeaviateClass;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SchemaCreateTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void testSchemeCreate() throws Exception {
        RunContext runContext = runContextFactory.of();

        Map<String, List<String>> parameters = Map.of("title", List.of("text"));

        SchemaCreate.Output schemeOutput = SchemaCreate.builder()
            .url(URL)
            .className(CLASS_NAME)
            .fields(parameters)
            .build()
            .run(runContext);

        assertThat(schemeOutput.getSuccess(), is(Boolean.TRUE));

        Result<WeaviateClass> schema = client().schema().classGetter().withClassName(CLASS_NAME).run();

        assertThat(schema.getResult().getProperties(), is(
                List.of(Property.builder()
                    .name("title")
                    .dataType(List.of("text"))
                    .tokenization(Tokenization.WORD)
                    .indexFilterable(true)
                    .indexSearchable(true)
                    .build())
            )
        );
    }
}
