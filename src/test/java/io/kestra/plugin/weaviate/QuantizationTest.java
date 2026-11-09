package io.kestra.plugin.weaviate;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.weaviate.SchemaCreate.QuantizationOptions;
import io.kestra.plugin.weaviate.SchemaCreate.QuantizationType;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Each quantization type is accepted by Weaviate as configured, and search still works on the compressed index.
 */
public class QuantizationTest extends WeaviateTest {
    @Inject
    private RunContextFactory runContextFactory;

    static List<QuantizationOptions> quantizations() {
        return List.of(
            QuantizationOptions.builder().type(QuantizationType.RQ).build(),
            QuantizationOptions.builder().type(QuantizationType.RQ).bits(1).build(),
            QuantizationOptions.builder().type(QuantizationType.BQ).build(),
            QuantizationOptions.builder().type(QuantizationType.SQ).build(),
            QuantizationOptions.builder().type(QuantizationType.PQ).segments(2).centroids(128).build(),
            QuantizationOptions.builder().type(QuantizationType.NONE).build()
        );
    }

    @ParameterizedTest
    @MethodSource("quantizations")
    public void createsQuantizedCollectionAndSearches(QuantizationOptions quantization) throws Exception {
        RunContext runContext = runContextFactory.of();

        schemaCreate().fields(Property.ofValue(Map.of("name", "text"))).quantization(Property.ofValue(quantization)).build().run(runContext);

        var index = vectorIndexConfig();
        for (var type : List.of("rq", "bq", "sq", "pq")) {
            assertThat(type + " enabled", index.path(type).path("enabled").asBoolean(), is(type.equalsIgnoreCase(quantization.getType().name())));
        }
        if (quantization.getType() == QuantizationType.NONE) {
            assertThat(index.path("skipDefaultQuantization").asBoolean(), is(true));
        }
        if (quantization.getBits() != null) {
            assertThat(index.path("rq").path("bits").asInt(), is(quantization.getBits()));
        }
        if (quantization.getSegments() != null) {
            assertThat(index.path("pq").path("segments").asInt(), is(quantization.getSegments()));
            assertThat(index.path("pq").path("centroids").asInt(), is(quantization.getCentroids()));
        }

        batchCreate()
            .vectorKey(Property.ofValue("vec"))
            .objects(
                List.of(
                    Map.of("name", "north", "vec", List.of(1.0, 0.0, 0.0, 0.0)),
                    Map.of("name", "east", "vec", List.of(0.0, 1.0, 0.0, 0.0)),
                    Map.of("name", "up", "vec", List.of(0.0, 0.0, 1.0, 0.0))
                )
            )
            .build()
            .run(runContext);

        FetchOutput output = query()
            .searchType(Property.ofValue(Query.SearchType.NEAR_VECTOR))
            .vector(Property.ofValue(List.of(0.9, 0.1, 0.0, 0.0)))
            .limit(Property.ofValue(1))
            .build()
            .run(runContext);
        @SuppressWarnings("unchecked")
        var row = (Map<String, Object>) output.getRows().getFirst();
        assertThat(properties(row).get("name"), is("north"));
    }

    private static JsonNode vectorIndexConfig() throws Exception {
        var response = HttpClient.newHttpClient().send(
            HttpRequest.newBuilder(URI.create(URL + "/v1/schema/" + CLASS_NAME)).build(),
            HttpResponse.BodyHandlers.ofString()
        );
        return new ObjectMapper().readTree(response.body()).path("vectorConfig").path("default").path("vectorIndexConfig");
    }
}
