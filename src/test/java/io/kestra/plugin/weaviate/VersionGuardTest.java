package io.kestra.plugin.weaviate;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Serves a fake pre-1.32 Weaviate so the unsupported-version error can be checked without Docker.
 */
@KestraTest
class VersionGuardTest {
    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;

    @BeforeEach
    void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/v1/.well-known/live", exchange ->
        {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.createContext("/v1/meta", exchange ->
        {
            byte[] body = "{\"hostname\":\"http://[::]:8080\",\"version\":\"1.21.3\",\"modules\":{}}".getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void rejectsServersOlderThan132() {
        Query task = Query.builder()
            .url("http://localhost:" + server.getAddress().getPort())
            .className(Property.ofValue("Anything"))
            .build();

        var e = assertThrows(IllegalStateException.class, () -> task.run(runContextFactory.of(Map.of())));

        assertThat(e.getMessage(), containsString("1.21.3"));
        assertThat(e.getMessage(), containsString("requires Weaviate 1.32 or later"));
        assertThat(e.getMessage(), containsString("plugin-weaviate 1.x"));
    }
}
