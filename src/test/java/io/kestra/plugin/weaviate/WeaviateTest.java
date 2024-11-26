package io.kestra.plugin.weaviate;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.serializers.FileSerde;
import io.weaviate.client.Config;
import io.weaviate.client.WeaviateClient;
import org.junit.jupiter.api.AfterEach;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

@KestraTest
public abstract class WeaviateTest {
    protected static final String HOST = "localhost:51525";
    protected static final String URL = "http://localhost:51525";
    protected static final String CLASS_NAME = "WeaviateTest";

    @AfterEach
    public void cleanAll() {
        client().schema().classDeleter().withClassName(CLASS_NAME).run();
    }

    protected WeaviateClient client() {
        return new WeaviateClient(new Config("http", HOST));
    }

    protected List<Map> readObjectsFromStream(InputStream inputStream) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            return FileSerde.readAll(reader, Map.class).collectList().block();
        }
    }
}
