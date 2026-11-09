package io.kestra.plugin.weaviate;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.plugin.weaviate.WeaviateConnection.Endpoint;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WeaviateConnectionTest {
    @Test
    void addsIntegrationHeader() {
        Map<String, String> headers = WeaviateConnection.buildHeaders(null);

        assertThat(headers, hasKey(WeaviateConnection.INTEGRATION_HEADER));
        assertThat(headers.get(WeaviateConnection.INTEGRATION_HEADER), matchesPattern("kestra-plugin-weaviate/\\d.*"));
    }

    @Test
    void keepsUserHeaders() {
        Map<String, String> headers = WeaviateConnection.buildHeaders(Map.of("X-OpenAI-Api-Key", "secret"));

        assertThat(headers, hasEntry("X-OpenAI-Api-Key", "secret"));
        assertThat(headers, hasKey(WeaviateConnection.INTEGRATION_HEADER));
    }

    @Test
    void userCanOverrideIntegrationHeader() {
        Map<String, String> headers = WeaviateConnection.buildHeaders(Map.of(WeaviateConnection.INTEGRATION_HEADER, "custom/1.0"));

        assertThat(headers, hasEntry(WeaviateConnection.INTEGRATION_HEADER, "custom/1.0"));
    }

    @Test
    void userCanOverrideIntegrationHeaderCaseInsensitively() {
        Map<String, String> headers = WeaviateConnection.buildHeaders(Map.of("x-weaviate-client-integration", "custom/1.0"));

        assertThat(headers.size(), is(1));
        assertThat(headers.get(WeaviateConnection.INTEGRATION_HEADER), is("custom/1.0"));
    }

    @Test
    void parsesHttpUrlWithPort() {
        assertThat(Endpoint.parse("http://localhost:8080", null, null), is(new Endpoint("http", "localhost", 8080, "localhost", 50051, false)));
    }

    @Test
    void defaultsToHttpsAndSchemePorts() {
        assertThat(Endpoint.parse("weaviate.example.com", null, null), is(new Endpoint("https", "weaviate.example.com", 443, "weaviate.example.com", 50051, false)));
        assertThat(Endpoint.parse("http://weaviate:80/", null, null).httpPort(), is(80));
        assertThat(Endpoint.parse("http://weaviate", null, null).httpPort(), is(80));
    }

    @Test
    void appliesGrpcOverrides() {
        assertThat(
            Endpoint.parse("https://weaviate.example.com", "grpc.example.com", 443),
            is(new Endpoint("https", "weaviate.example.com", 443, "grpc.example.com", 443, false))
        );
        assertThat(Endpoint.parse("http://localhost:8080", null, 51526).grpcPort(), is(51526));
    }

    @Test
    void detectsWeaviateCloud() {
        assertThat(Endpoint.parse("https://abc123.c0.europe-west3.gcp.weaviate.cloud", null, null).cloud(), is(true));
        assertThat(Endpoint.parse("abc123.weaviate.network", null, null).cloud(), is(true));
        assertThat(Endpoint.parse("https://ABC.WEAVIATE.CLOUD", null, null).cloud(), is(true));
        assertThat(Endpoint.parse("https://weaviate.cloud.example.com", null, null).cloud(), is(false));
    }

    @Test
    void explicitGrpcSettingsDisableCloudDetection() {
        Endpoint endpoint = Endpoint.parse("https://abc.weaviate.cloud", "grpc-abc.weaviate.cloud", null);

        assertThat(endpoint.cloud(), is(false));
        assertThat(endpoint.grpcHost(), is("grpc-abc.weaviate.cloud"));
    }

    @Test
    void rejectsInvalidUrls() {
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("", null, null));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("ftp://host", null, null));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("http://host:8080/v1", null, null));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("http://host:8080", null, 0));
        assertThrows(IllegalArgumentException.class, () -> Endpoint.parse("http://host:8080", null, 70000));
    }

    @Test
    void toStringHidesCredentials() {
        var task = Query.builder()
            .url("http://localhost:8080")
            .apiKey(io.kestra.core.models.property.Property.ofValue("super-secret-key"))
            .headers(io.kestra.core.models.property.Property.ofValue(Map.of("X-OpenAI-Api-Key", "sk-secret")))
            .build();
        assertThat(task.toString(), not(containsString("super-secret-key")));
        assertThat(task.toString(), not(containsString("sk-secret")));
    }
}
