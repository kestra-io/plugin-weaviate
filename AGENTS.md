# Kestra Weaviate Plugin

## What

Plugin Weaviate for Kestra Exposes 4 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with weaviate, allowing orchestration of weaviate-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
