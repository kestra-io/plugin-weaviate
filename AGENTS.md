# Kestra Weaviate Plugin

## What

- Provides plugin components under `io.kestra.plugin.weaviate`.
- Includes classes such as `Delete`, `SchemaCreate`, `BatchCreate`, `WeaviateConnection`.

## Why

- What user problem does this solve? Teams need to manage and query vectors in Weaviate from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Weaviate steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Weaviate.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `weaviate`

Infrastructure dependencies (Docker Compose services):

- `weaviate`

### Key Plugin Classes

- `io.kestra.plugin.weaviate.BatchCreate`
- `io.kestra.plugin.weaviate.Delete`
- `io.kestra.plugin.weaviate.Query`
- `io.kestra.plugin.weaviate.SchemaCreate`

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
