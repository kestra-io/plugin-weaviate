# Kestra Weaviate Plugin

## What

- Provides plugin components under `io.kestra.plugin.weaviate`.
- Includes classes such as `Delete`, `SchemaCreate`, `BatchCreate`, `WeaviateConnection`.

## Why

- This plugin integrates Kestra with Weaviate.
- It provides tasks that manage and query vectors in Weaviate.

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
