package io.kestra.plugin.weaviate;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;

import io.weaviate.client6.v1.api.WeaviateApiException;
import io.weaviate.client6.v1.api.collections.CollectionConfig;
import io.weaviate.client6.v1.api.collections.vectorizers.SelfProvidedVectorizer;
import io.weaviate.client6.v1.api.collections.vectorizers.Text2VecModel2VecVectorizer;
import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaCreateTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    private static CollectionConfig config() throws Exception {
        try (var client = client()) {
            return client.collections.getConfig(CLASS_NAME).orElseThrow();
        }
    }

    @Test
    public void createsCollectionWithProperties() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("name", CLASS_NAME));

        SchemaCreate.Output output = SchemaCreate.builder()
            .url(URL)
            .grpcPort(Property.ofValue(GRPC_PORT))
            .className(Property.ofExpression("{{ name }}"))
            .fields(Property.ofValue(Map.of("title", List.of("string"), "year", "int", "tags", "text[]")))
            .build()
            .run(runContext);

        assertThat(output.getSuccess(), is(true));
        assertThat(output.getCreated(), is(true));

        CollectionConfig config = config();
        Map<String, List<String>> types = new java.util.HashMap<>();
        config.properties().forEach(p -> types.put(p.propertyName(), p.dataTypes()));
        assertThat(types, is(Map.of("title", List.of("text"), "year", List.of("int"), "tags", List.of("text[]"))));
        assertThat(config.vectors().get("default"), instanceOf(SelfProvidedVectorizer.class));
    }

    @Test
    public void createsVectorizedCollection() throws Exception {
        RunContext runContext = runContextFactory.of();

        schemaCreate()
            .fields(Property.ofValue(Map.of("title", "text")))
            .vectorizer(Property.ofValue(SchemaCreate.Vectorizer.TEXT2VEC_MODEL2VEC))
            .vectorName(Property.ofValue("title_vec"))
            .build()
            .run(runContext);

        assertThat(config().vectors().get("title_vec"), instanceOf(Text2VecModel2VecVectorizer.class));
    }

    @Test
    public void skipsExistingCollectionWhenAsked() throws Exception {
        RunContext runContext = runContextFactory.of();
        schemaCreate().fields(Property.ofValue(Map.of("title", "text"))).build().run(runContext);

        assertThrows(WeaviateApiException.class, () -> schemaCreate().fields(Property.ofValue(Map.of("title", "text"))).build().run(runContext));

        SchemaCreate.Output output = schemaCreate()
            .fields(Property.ofValue(Map.of("title", "text")))
            .skipIfExists(Property.ofValue(true))
            .build()
            .run(runContext);
        assertThat(output.getSuccess(), is(true));
        assertThat(output.getCreated(), is(false));
    }
}
