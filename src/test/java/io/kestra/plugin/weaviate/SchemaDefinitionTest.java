package io.kestra.plugin.weaviate;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.SchemaCreate.MultiTenancyOptions;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * SchemaCreate.definition sends Weaviate's own collection JSON, e.g. from https://weaviate.github.io/weaviate-add-collection/.
 */
public class SchemaDefinitionTest extends WeaviateTest {
    // Shaped like the collection builder's output. rescoreLimit and moduleConfig.skip are the settings client6 6.3.2 drops.
    private static final String DEFINITION = """
        {
          "class": "FromTheBuilder",
          "description": "Created from JSON",
          "invertedIndexConfig": { "indexNullState": true, "indexPropertyLength": true },
          "properties": [
            { "name": "title", "dataType": ["text"], "tokenization": "word" },
            {
              "name": "sku", "dataType": ["text"], "tokenization": "field", "indexSearchable": false,
              "moduleConfig": { "text2vec-model2vec": { "skip": true } }
            },
            {
              "name": "dimensions", "dataType": ["object"],
              "nestedProperties": [
                { "name": "width", "dataType": ["number"] },
                { "name": "unit", "dataType": ["text"] }
              ]
            }
          ],
          "vectorConfig": {
            "default": {
              "vectorizer": { "text2vec-model2vec": { "properties": ["title"] } },
              "vectorIndexType": "hnsw",
              "vectorIndexConfig": { "rq": { "enabled": true, "bits": 8, "rescoreLimit": 50 } }
            }
          }
        }""";

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    public void createsTheCollectionAsWritten() throws Exception {
        RunContext runContext = runContextFactory.of(Map.of("definition", DEFINITION));

        // A JSON string, as pasted from the builder, rendered from an expression.
        var output = schemaCreate()
            .className(null)
            .definition(Property.ofExpression("{{ definition }}"))
            .build()
            .run(runContext);
        assertThat(output.getCreated(), is(true));

        var config = schema("FromTheBuilder");
        assertThat(config.path("description").asText(), is("Created from JSON"));
        assertThat(config.path("invertedIndexConfig").path("indexNullState").asBoolean(), is(true));
        var rq = config.path("vectorConfig").path("default").path("vectorIndexConfig").path("rq");
        assertThat(rq.path("enabled").asBoolean(), is(true));
        assertThat(rq.path("rescoreLimit").asInt(), is(50));
        var sku = property(config, "sku");
        assertThat(sku.path("tokenization").asText(), is("field"));
        assertThat(sku.path("moduleConfig").path("text2vec-model2vec").path("skip").asBoolean(), is(true));
        assertThat(property(config, "dimensions").path("nestedProperties").size(), is(2));

        // The collection works like any other.
        batchCreate()
            .className(Property.ofValue("FromTheBuilder"))
            .objects(List.of(Map.of("title", "desk", "sku", "D-1", "dimensions", Map.of("width", 1.2, "unit", "m"))))
            .build()
            .run(runContext);
        var rows = query().className(Property.ofValue("FromTheBuilder")).build().run(runContext).getRows();
        assertThat(rows, hasSize(1));

        // Rerun: reuses the collection.
        var rerun = schemaCreate()
            .className(null)
            .definition(Property.ofExpression("{{ definition }}"))
            .skipIfExists(Property.ofValue(true))
            .build()
            .run(runContext);
        assertThat(rerun.getCreated(), is(false));

        try (var client = client()) {
            client.collections.delete("FromTheBuilder");
        }
    }

    @Test
    public void classNameOverridesAndTenantsApply() throws Exception {
        RunContext runContext = runContextFactory.of();
        var definition = Map.<String, Object> of(
            "class", "Ignored",
            "multiTenancyConfig", Map.of("enabled", true),
            "properties", List.of(Map.of("name", "text", "dataType", List.of("text")))
        );

        var output = schemaCreate()
            .definition(Property.ofValue(definition))
            .multiTenancy(Property.ofValue(MultiTenancyOptions.builder().tenants(List.of("acme")).build()))
            .build()
            .run(runContext);

        assertThat(output.getTenantsCreated(), is(List.of("acme")));
        assertThat(schema(CLASS_NAME).path("multiTenancyConfig").path("enabled").asBoolean(), is(true));
        assertThat(schema("Ignored").isMissingNode() || schema("Ignored").isEmpty(), is(true));
    }

    @Test
    public void rejectsConflictsAndReportsServerErrors() {
        RunContext runContext = runContextFactory.of();

        var conflict = assertThrows(
            IllegalArgumentException.class,
            () -> schemaCreate().definition(Property.ofValue(Map.of("class", "X"))).fields(Property.ofValue(Map.of("a", "text"))).build().run(runContext)
        );
        assertThat(conflict.getMessage(), containsString("remove fields"));

        var noName = assertThrows(
            IllegalArgumentException.class,
            () -> schemaCreate().className(null).definition(Property.ofValue(Map.of("properties", List.of()))).build().run(runContext)
        );
        assertThat(noName.getMessage(), containsString("must name the collection in `class`"));

        var rejected = assertThrows(
            IllegalStateException.class,
            () -> schemaCreate()
                .definition(Property.ofValue(Map.of("properties", List.of(Map.of("name", "a", "dataType", List.of("notAType"))))))
                .build()
                .run(runContext)
        );
        assertThat(rejected.getMessage(), containsString("HTTP 422"));
        assertThat(rejected.getMessage(), containsString("notAType"));
    }

    private static JsonNode schema(String name) throws Exception {
        var response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create(URL + "/v1/schema/" + name)).build(), HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 404 ? new ObjectMapper().createObjectNode() : new ObjectMapper().readTree(response.body());
    }

    private static JsonNode property(JsonNode config, String name) {
        for (var p : config.path("properties")) {
            if (p.path("name").asText().equals(name)) {
                return p;
            }
        }
        throw new AssertionError("no property " + name);
    }
}
