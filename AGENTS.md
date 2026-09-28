# Kestra Weaviate Plugin

## What

- Provides plugin components under `io.kestra.plugin.weaviate`.
- Includes classes such as `Delete`, `SchemaCreate`, `BatchCreate`, `Query`, `WeaviateConnection`.
- Built on the Weaviate Java client v6 (`io.weaviate:client6`), which uses REST and gRPC and requires Weaviate 1.32 or later. There is no GraphQL.

## Why

- What user problem does this solve? Teams need to manage and query vectors in Weaviate from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Weaviate steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Weaviate.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `weaviate`

Infrastructure dependencies (Docker Compose services in `docker-compose-ci.yml`):

- `weaviate`: HTTP on host port 51525, gRPC on 51526, `QUERY_MAXIMUM_RESULTS=100` to exercise Delete's batching
- `model2vec`: small text2vec inference container for NEAR_TEXT tests

Classpath notes:

- `build.gradle` forces protobuf-java 4.x: client6's gRPC stubs need it, but the Kestra platform pins 3.25.x.
- `shadowJar` uses `DuplicatesStrategy.INCLUDE` + `mergeServiceFiles()` so grpc `META-INF/services` providers survive; `verifyShadedGrpcProviders` (part of `check`) guards this.

### Key Plugin Classes

- `io.kestra.plugin.weaviate.BatchCreate`
- `io.kestra.plugin.weaviate.Delete`
- `io.kestra.plugin.weaviate.Query`
- `io.kestra.plugin.weaviate.SchemaCreate`

Shared helpers:

- `WeaviateConnection`: URL parsing (`Endpoint`), Weaviate Cloud detection, headers (incl. `X-Weaviate-Client-Integration`), version guard. Tasks must close the client (`try (WeaviateClient client = connect(runContext))`).
- `WeaviateFilters` / `FilterCondition`: `filter` map and `filters` list to a v6 `Filter`. Dates are sent as RFC3339 text with seconds, because the v6 client's `OffsetDateTime.toString()` drops zero seconds and the server rejects it.
- `WeaviateRows`: query results to `{uuid, properties, metadata, vector?, vectors?}` maps that serialize to JSON and ION.

### Project Structure

```
plugin-weaviate/
├── src/main/java/io/kestra/plugin/weaviate/
├── src/test/java/io/kestra/plugin/weaviate/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
