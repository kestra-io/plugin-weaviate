package io.kestra.plugin.weaviate;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@link WeaviateConnection#restPost} against an in-process HTTP server, so the exact request can be checked. */
@KestraTest
class RestPostTest {
    @Inject
    private RunContextFactory runContextFactory;

    private HttpServer server;
    private final Map<String, String> received = new ConcurrentHashMap<>();
    private volatile int status = 200;
    private volatile String responseBody = "{\"class\":\"X\"}";
    private volatile long delayMillis = 0;

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", this::handle);
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private void handle(HttpExchange exchange) throws IOException {
        received.put("method", exchange.getRequestMethod());
        received.put("path", exchange.getRequestURI().getPath());
        received.put("body", new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        exchange.getRequestHeaders().forEach((name, values) -> received.put(name.toLowerCase(), String.join(",", values)));
        try {
            Thread.sleep(delayMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        var bytes = responseBody.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private SchemaCreate.SchemaCreateBuilder<?, ?> task() {
        return SchemaCreate.builder().url("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void sendsTheBodyWithAuthAndHeaders() throws Exception {
        var body = task()
            .apiKey(Property.ofValue("secret-key"))
            .headers(Property.ofValue(Map.of("X-Cohere-Api-Key", "cohere")))
            .build()
            .restPost(runContextFactory.of(), "/v1/schema", "{\"class\":\"X\"}");

        assertThat(body, is("{\"class\":\"X\"}"));
        assertThat(received.get("method"), is("POST"));
        assertThat(received.get("path"), is("/v1/schema"));
        assertThat(received.get("body"), is("{\"class\":\"X\"}"));
        assertThat(received.get("content-type"), startsWith("application/json"));
        assertThat(received.get("authorization"), is("Bearer secret-key"));
        assertThat(received.get("x-cohere-api-key"), is("cohere"));
        assertThat(received.get(WeaviateConnection.INTEGRATION_HEADER.toLowerCase()), is(WeaviateConnection.INTEGRATION_VALUE));
    }

    @Test
    void sendsNoAuthorizationWithoutApiKey() throws Exception {
        task().build().restPost(runContextFactory.of(), "/v1/schema", "{}");

        assertThat(received.containsKey("authorization"), is(false));
    }

    @Test
    void reportsErrorsWithATruncatedBody() {
        status = 422;
        responseBody = "e".repeat(10_000);

        var e = assertThrows(IllegalStateException.class, () -> task().build().restPost(runContextFactory.of(), "/v1/schema", "{}"));

        assertThat(e.getMessage(), startsWith("Weaviate answered HTTP 422 to POST /v1/schema: eee"));
        assertThat(e.getMessage(), endsWith("... (" + (10_000 - WeaviateConnection.MAX_ERROR_BODY_CHARS) + " more characters)"));
        assertThat(e.getMessage().length(), lessThan(WeaviateConnection.MAX_ERROR_BODY_CHARS + 200));
    }

    @Test
    void appliesTheRequestTimeout() {
        delayMillis = 1_500;

        var e = assertThrows(
            Exception.class,
            () -> task().requestTimeout(Property.ofValue(Duration.ofMillis(200))).build().restPost(runContextFactory.of(), "/v1/schema", "{}")
        );

        Throwable cause = e;
        while (cause != null && !(cause instanceof SocketTimeoutException)) {
            cause = cause.getCause();
        }
        assertThat("expected a socket timeout, got " + e, cause, notNullValue());
    }
}
