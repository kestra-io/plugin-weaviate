package io.kestra.plugin.weaviate;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.junit.jupiter.api.AfterEach;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.common.FetchOutput;
import io.kestra.core.models.tasks.common.FetchType;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;

import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.CollectionConfig;
import io.weaviate.client6.v1.api.collections.VectorConfig;
import io.weaviate.client6.v1.internal.ObjectBuilder;

/**
 * Integration test base. Expects the services from docker-compose-ci.yml (HTTP on 51525, gRPC on 51526).
 */
@KestraTest
public abstract class WeaviateTest {
    protected static final String URL = "http://localhost:51525";
    protected static final int GRPC_PORT = 51526;
    protected static final String CLASS_NAME = "WeaviateTest";

    @AfterEach
    public void cleanAll() throws Exception {
        try (WeaviateClient client = client()) {
            client.collections.delete(CLASS_NAME);
        }
    }

    protected static WeaviateClient client() {
        return WeaviateClient.connectToLocal(l -> l.host("localhost").port(51525).grpcPort(GRPC_PORT));
    }

    /**
     * Creates {@link #CLASS_NAME} with self-provided vectors and the given property definitions.
     */
    protected static void createCollection(Function<CollectionConfig.Builder, ObjectBuilder<CollectionConfig>> fn) throws Exception {
        try (WeaviateClient client = client()) {
            client.collections.create(CLASS_NAME, fn);
        }
    }

    protected static void createSelfProvidedCollection(io.weaviate.client6.v1.api.collections.Property... properties) throws Exception {
        createCollection(c -> c.properties(properties).vectorConfig(VectorConfig.selfProvided()));
    }

    protected static Query.QueryBuilder<?, ?> query() {
        return Query.builder()
            .url(URL)
            .grpcPort(Property.ofValue(GRPC_PORT))
            .className(Property.ofValue(CLASS_NAME))
            .fetchType(Property.ofValue(FetchType.FETCH));
    }

    protected static BatchCreate.BatchCreateBuilder<?, ?> batchCreate() {
        return BatchCreate.builder()
            .url(URL)
            .grpcPort(Property.ofValue(GRPC_PORT))
            .className(Property.ofValue(CLASS_NAME));
    }

    protected static Delete.DeleteBuilder<?, ?> delete() {
        return Delete.builder()
            .url(URL)
            .grpcPort(Property.ofValue(GRPC_PORT))
            .className(Property.ofValue(CLASS_NAME));
    }

    protected static SchemaCreate.SchemaCreateBuilder<?, ?> schemaCreate() {
        return SchemaCreate.builder()
            .url(URL)
            .grpcPort(Property.ofValue(GRPC_PORT))
            .className(Property.ofValue(CLASS_NAME));
    }

    @SuppressWarnings("unchecked")
    protected static List<Map<String, Object>> fetchAll(RunContext runContext) throws Exception {
        FetchOutput output = query().build().run(runContext);
        return output.getRows().stream().map(r -> (Map<String, Object>) r).toList();
    }

    @SuppressWarnings("unchecked")
    protected static Map<String, Object> properties(Map<String, Object> row) {
        return (Map<String, Object>) row.get("properties");
    }

    protected List<Map> readObjectsFromStream(InputStream inputStream) throws Exception {
        try (inputStream) {
            return FileSerde.readAll(inputStream, Map.class).collectList().block();
        }
    }
}
