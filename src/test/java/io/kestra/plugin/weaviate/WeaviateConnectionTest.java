package io.kestra.plugin.weaviate;

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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
}
