# How to use the Weaviate plugin

Search, insert, and manage objects and collections in Weaviate from Kestra flows. The plugin uses the Weaviate Java client v6, which talks to Weaviate over REST and gRPC and requires **Weaviate 1.32 or later**.

## Connection

Set `url` to your Weaviate URL, e.g. `http://localhost:8080`. For authenticated clusters, set `apiKey`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and set connection properties on each task.

Queries and batch inserts use gRPC, so the gRPC port must be reachable from the Kestra worker:

- Self-hosted Weaviate: gRPC defaults to the host of `url` on port `50051`. Override it with `grpcHost` and `grpcPort`. gRPC uses TLS when `url` is https.
- Weaviate Cloud: URLs ending in `.weaviate.cloud` or `.weaviate.network` are detected automatically and connect to the matching gRPC endpoint on port 443. `apiKey` is required.

Pass extra headers with `headers`, for example vectorizer API keys such as `X-OpenAI-Api-Key`. They are sent on both REST and gRPC calls.

## Tasks

`SchemaCreate` creates a collection. Set `className`, `fields` as a map of property name to Weaviate data type (`text`, `int`, `number`, `boolean`, `date`, `uuid`, their `[]` array forms, `blob`, `geoCoordinates`, `phoneNumber`), and optionally a `vectorizer`. Set `skipIfExists: true` to make the task idempotent.

`BatchCreate` inserts objects in batches of `batchSize`. Set `objects` to a `kestra://` URI of an ION file or an inline list of maps. Use `uuidKey` and `vectorKey` to take object IDs and precomputed vectors from the input. The task fails if Weaviate rejects any object, unless `failOnError` is false; rejected objects are then listed in the `errors` output.

`Query` searches a collection. Choose the search with `searchType`:

| `searchType` | Uses |
|---|---|
| `FETCH_OBJECTS` (default) | lists objects, optionally filtered |
| `BM25` | keyword search on `query`, optionally limited to `queryProperties` |
| `HYBRID` | blends keyword and vector search on `query`; `alpha` sets the balance, `vector` is optional |
| `NEAR_TEXT` | vector search on `query`, vectorized by the collection's vectorizer |
| `NEAR_VECTOR` | vector search on `vector` |

Each result row is `{uuid, properties, metadata, vector?, vectors?}`. Request metadata such as `DISTANCE` or `SCORE` with `returnMetadata`, and vectors with `includeVector`. Control result handling with `fetchType` (default `STORE`).

`Delete` removes objects from a collection: set `objectId` to delete one object, or filters to delete every match. Deleting all objects requires `deleteAll: true`.

## Filters

`Query` and `Delete` accept two filter forms, which can be combined (both must match):

- `filter`: a map of property to value, combined with AND. Text values use `LIKE`, so `*` wildcards work; other values use `EQUAL`.
- `filters`: a list of `{property, operator, value, valueType}` conditions, combined with `filtersOperator` (`AND` or `OR`). Operators: `EQUAL`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_EQUAL`, `LESS_THAN`, `LESS_THAN_EQUAL`, `LIKE`, `CONTAINS_ANY`, `CONTAINS_ALL`, `CONTAINS_NONE`, `IS_NULL`. Set `valueType: DATE` or `UUID` for dates and UUIDs written as strings. Use `_id` to filter by object UUID.

## Upgrading from 1.x

Version 2 of this plugin replaces the GraphQL-based `Query` with the structured search above, and requires Weaviate 1.32 or later. The main changes:

- `Query.query` is now search text, not a GraphQL string. Use `searchType`, `limit`, `returnProperties` and `filters` instead of a `Get { ... }` query.
- Query rows changed from `{ClassName: {...}}` to `{uuid, properties, metadata}`: `row.ClassName.title` becomes `row.properties.title`, and `_additional.id` becomes `uuid`.
- `BatchCreate` now returns `insertedCount`, `failedCount` and `errors`, and fails on rejected objects by default.
- `Delete` with neither `objectId` nor filters now requires `deleteAll: true`.
- `SchemaCreate` takes one data type per field; the legacy `string` type is created as `text`.
- Aggregations, `groupBy`, generative search and `Explore` are not supported. Stay on plugin 1.x, or call the Weaviate REST API with `io.kestra.plugin.core.http.Request`.
