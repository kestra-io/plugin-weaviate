# How to use the Weaviate plugin

Search, insert, and manage objects and collections in Weaviate from Kestra flows. The plugin uses the Weaviate Java client v6, which talks to Weaviate over REST and gRPC and requires **Weaviate 1.32 or later**.

## Connection

Set `url` to your Weaviate URL, e.g. `http://localhost:8080`. For authenticated clusters, set `apiKey`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret). Set connection properties on each task, or once per flow with `pluginDefaults` as in the example below.

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

`Delete` removes objects from a collection: set `objectId` to delete one object, or filters to delete every match. Deleting all objects requires `deleteAll: true`. The task fails if Weaviate rejects any deletion, unless `failOnError` is false.

## Filters

`Query` and `Delete` accept two filter forms, which can be combined (both must match):

- `filter`: a map of property to value, combined with AND. Text values use `LIKE`, so `*` wildcards work; other values use `EQUAL`.
- `filters`: a list of `{property, operator, value, valueType}` conditions, combined with `filtersOperator` (`AND` or `OR`). Operators: `EQUAL`, `NOT_EQUAL`, `GREATER_THAN`, `GREATER_THAN_EQUAL`, `LESS_THAN`, `LESS_THAN_EQUAL`, `LIKE`, `CONTAINS_ANY`, `CONTAINS_ALL`, `CONTAINS_NONE`, `IS_NULL`. Set `valueType: DATE` or `UUID` for dates and UUIDs written as strings. Use `_id` to filter by object UUID.

## Example: load and search a collection

This flow uses every task and nearly every option. It works with a knowledge base of articles:

- **Collections:** `SchemaCreate` creates `Article`, whose `content` vector Weaviate computes from the text with Cohere (`TEXT2VEC_COHERE`, `vectorizerModel`), and `Snippet`, which stores vectors you provide.
- **Loading:** `BatchCreate` loads articles from an ION file and from an inline list, and snippets with their own vectors (`vectorKey`, `targetVector`). One article is rejected on purpose; with `failOnError: false` it is reported in `errors` and the task still succeeds.
- **Searching:** `Query` runs each search type:
  - `BM25` with boosted `queryProperties`;
  - `HYBRID` with `alpha` and a simple `filter`;
  - `NEAR_TEXT` with `certainty` and structured `filters` combined with `OR`;
  - `NEAR_VECTOR`, once with the vector of an article fetched with `includeVector`, once with a hand-written vector.

  It also uses each `fetchType`: `FETCH`, `FETCH_ONE`, `STORE` and `NONE`.
- **Deleting:** `Delete` removes one object by `objectId`, then every draft by filter. The `reset` task shows `deleteAll` and is disabled.
- **Connection:** `pluginDefaults` sets it once for all Weaviate tasks. It also passes the Cohere API key in `headers`, and comments show the Weaviate Cloud variant.

Running the flow again is safe. `skipIfExists` reuses the collections, and `uuidKey` replaces objects that have the same `id` instead of duplicating them.

Store your Cohere API key as the `COHERE_API_KEY` secret. Weaviate calls Cohere with it whenever it vectorizes objects or `NEAR_TEXT`/`HYBRID` queries. The `text2vec-cohere` module is enabled by default on recent Weaviate servers and on Weaviate Cloud. `tenant` is not shown, because it needs a multi-tenant collection, which `SchemaCreate` does not create.

```yaml
id: weaviate_load_and_search
namespace: company.team

inputs:
  - id: question
    type: STRING
    defaults: how do I keep my data safe

tasks:
  # Collection whose `content` vector Weaviate computes from the text properties with Cohere.
  - id: create_articles
    type: io.kestra.plugin.weaviate.SchemaCreate
    className: Article
    vectorizer: TEXT2VEC_COHERE
    vectorizerModel: embed-v4.0
    vectorName: content
    skipIfExists: true
    fields:
      title: text
      body: text
      category: text
      tags: text[]
      wordCount: int
      rating: number
      published: boolean
      publishedAt: date

  # Collection for vectors you compute yourself.
  - id: create_snippets
    type: io.kestra.plugin.weaviate.SchemaCreate
    className: Snippet
    vectorizer: NONE
    vectorName: embedding
    skipIfExists: true
    fields:
      text: text

  # An ION file, as produced by most Kestra tasks that read data.
  - id: articles_file
    type: io.kestra.plugin.core.storage.Write
    extension: .ion
    content: |
      {id:"2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0001",title:"Backups that actually restore",body:"Test your restore process regularly and keep copies in another region.",category:"operations",tags:["backup","disaster-recovery"],wordCount:900,rating:4.8,published:true,publishedAt:2024-02-01T00:00:00Z}
      {id:"2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0002",title:"Encrypting data at rest",body:"Turn on disk encryption and rotate keys to protect stored data.",category:"security",tags:["encryption","compliance"],wordCount:1200,rating:4.6,published:true,publishedAt:2024-05-15T09:30:00Z}

  - id: load_file
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Article
    objects: "{{ outputs.articles_file.uri }}"
    uuidKey: id

  - id: load_inline
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Article
    uuidKey: id
    batchSize: 2
    failOnError: false
    objects:
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0003
        title: Scaling reads with replicas
        body: Add read replicas to spread query load across nodes.
        category: operations
        tags: [scaling, replication]
        wordCount: 700
        rating: 4.1
        published: true
        publishedAt: "2023-11-20T00:00:00Z"
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0004
        title: Least-privilege API keys
        body: Give each service its own key with only the permissions it needs.
        category: security
        tags: [access-control, api-keys]
        wordCount: 650
        rating: 4.9
        published: true
        publishedAt: "2024-08-02T00:00:00Z"
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0005
        title: Draft notes on audit logging
        body: Record who changed what, and keep the logs where attackers cannot edit them.
        category: security
        tags: [audit, compliance]
        wordCount: 300
        published: false
      # Rejected on purpose: wordCount must be an int. With failOnError: false it is reported in `errors`.
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0006
        title: Broken object
        wordCount: a lot

  - id: load_snippets
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Snippet
    uuidKey: id
    vectorKey: vec
    targetVector: embedding
    objects:
      - {id: 7c0d3e1a-1111-4b6e-9a51-000000000001, text: north, vec: [1.0, 0.0, 0.0]}
      - {id: 7c0d3e1a-1111-4b6e-9a51-000000000002, text: north-east, vec: [0.7, 0.7, 0.0]}
      - {id: 7c0d3e1a-1111-4b6e-9a51-000000000003, text: up, vec: [0.0, 0.0, 1.0]}

  # Keyword search, with matches in the title counting twice.
  - id: bm25
    type: io.kestra.plugin.weaviate.Query
    className: Article
    searchType: BM25
    query: data
    queryProperties:
      - title^2
      - body
    returnProperties: [title]
    returnMetadata: [SCORE, EXPLAIN_SCORE]
    fetchType: FETCH

  # Keyword and vector search blended by alpha (0 = keyword only, 1 = vector only), limited to published articles.
  - id: hybrid
    type: io.kestra.plugin.weaviate.Query
    className: Article
    searchType: HYBRID
    query: "{{ inputs.question }}"
    alpha: 0.5
    targetVector: content
    filter:
      published: true
    limit: 3
    returnProperties: [title, category]
    returnMetadata: [SCORE]
    fetchType: FETCH

  # Vector search on the text, keeping close matches that are recent OR tagged for compliance.
  - id: near_text
    type: io.kestra.plugin.weaviate.Query
    className: Article
    searchType: NEAR_TEXT
    query: protect sensitive information
    targetVector: content
    certainty: 0.5
    filters:
      - property: publishedAt
        operator: GREATER_THAN_EQUAL
        value: "2024-01-01T00:00:00Z"
        valueType: DATE
      - property: tags
        operator: CONTAINS_ANY
        value: [compliance]
    filtersOperator: OR
    returnProperties: [title, publishedAt, tags]
    returnMetadata: [DISTANCE, CERTAINTY]
    fetchType: FETCH

  # One object by ID, with its vector.
  - id: pick
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: _id
        operator: EQUAL
        value: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0003
        valueType: UUID
    includeVector: true
    fetchType: FETCH_ONE

  # Articles similar to the one picked above. offset: 1 skips the article itself.
  - id: similar
    type: io.kestra.plugin.weaviate.Query
    className: Article
    searchType: NEAR_VECTOR
    vector: "{{ outputs.pick.row.vectors.content }}"
    targetVector: content
    distance: 0.9
    limit: 2
    offset: 1
    returnProperties: [title]
    returnMetadata: [DISTANCE]
    fetchType: FETCH

  # Vector search with your own query vector.
  - id: nearest_snippets
    type: io.kestra.plugin.weaviate.Query
    className: Snippet
    searchType: NEAR_VECTOR
    vector: [0.9, 0.1, 0.0]
    targetVector: embedding
    limit: 2
    returnMetadata: [DISTANCE]
    fetchType: FETCH

  # Every security article, stored as an ION file for downstream tasks.
  - id: export
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filter:
      category: secur*
    limit: 100
    returnMetadata: [CREATION_TIME, LAST_UPDATE_TIME]
    fetchType: STORE

  # Long, well-rated articles: both conditions must match (filtersOperator defaults to AND).
  - id: long_reads
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: wordCount
        operator: GREATER_THAN
        value: 800
      - property: rating
        operator: GREATER_THAN_EQUAL
        value: 4.5
    limit: 100
    returnProperties: [title]
    fetchType: FETCH

  # Runs the search without returning rows, e.g. to check the collection is reachable.
  # The row count is reported in the task's `records` metric.
  - id: check
    type: io.kestra.plugin.weaviate.Query
    className: Article
    limit: 1
    fetchType: NONE

  - id: report
    type: io.kestra.plugin.core.log.Log
    message: |
      Loaded {{ outputs.load_file.insertedCount + outputs.load_inline.insertedCount }} articles, rejected {{ outputs.load_inline.failedCount }}: {{ outputs.load_inline.errors[0].message }}
      BM25 "data":
      {%- for row in outputs.bm25.rows %}
        {{ row.metadata.score | numberFormat('0.00') }} {{ row.properties.title }}
      {%- endfor %}
      Hybrid "{{ inputs.question }}":
      {%- for row in outputs.hybrid.rows %}
        {{ row.metadata.score | numberFormat('0.00') }} {{ row.properties.title }} ({{ row.properties.category }})
      {%- endfor %}
      Near text, recent or compliance:
      {%- for row in outputs.near_text.rows %}
        {{ row.metadata.certainty | numberFormat('0.00') }} {{ row.properties.title }} {{ row.properties.tags }}
      {%- endfor %}
      Similar to "{{ outputs.pick.row.properties.title }}":
      {%- for row in outputs.similar.rows %}
        {{ row.metadata.distance | numberFormat('0.00') }} {{ row.properties.title }}
      {%- endfor %}
      Snippets nearest to [0.9, 0.1, 0]:
      {%- for row in outputs.nearest_snippets.rows %}
        {{ row.metadata.distance | numberFormat('0.00') }} {{ row.properties.text }}
      {%- endfor %}
      Exported {{ outputs.export.size }} security articles to {{ outputs.export.uri }}
      Long, well-rated reads ({{ outputs.long_reads.size }}):
      {%- for row in outputs.long_reads.rows %}
        {{ row.properties.title }}
      {%- endfor %}

  - id: delete_one
    type: io.kestra.plugin.weaviate.Delete
    className: Snippet
    objectId: 7c0d3e1a-1111-4b6e-9a51-000000000003

  - id: delete_drafts
    type: io.kestra.plugin.weaviate.Delete
    className: Article
    filter:
      published: false
    failOnError: true

  # Empties the collection. Enable it to reset the demo.
  - id: reset
    type: io.kestra.plugin.weaviate.Delete
    disabled: true
    className: Snippet
    deleteAll: true

pluginDefaults:
  - type: io.kestra.plugin.weaviate
    values:
      url: http://localhost:8080
      grpcHost: localhost
      grpcPort: 50051
      requestTimeout: PT30S
      # For Weaviate Cloud, use your cluster URL and an API key; gRPC is detected automatically.
      # url: https://my-cluster.weaviate.cloud
      # apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
      # Weaviate calls Cohere with this key when it vectorizes objects and NEAR_TEXT/HYBRID queries.
      headers:
        X-Cohere-Api-Key: "{{ secret('COHERE_API_KEY') }}"
```

The `report` task logs the result of each search. With the default question, the top hybrid match is "Encrypting data at rest". The `similar` search returns the two articles closest to "Scaling reads with replicas", excluding that article itself. Each row in `rows` is `{uuid, properties, metadata, vector?, vectors?}`. With `fetchType: STORE`, rows go to an ION file at `outputs.<task>.uri`, which suits large result sets.

## Upgrading from the GraphQL-based plugin

Plugin versions built on the Weaviate Java client v6 replace the GraphQL-based `Query` with the structured search above, and requires Weaviate 1.32 or later. The main changes:

- `Query.query` is now search text, not a GraphQL string. Use `searchType`, `limit`, `returnProperties` and `filters` instead of a `Get { ... }` query.
- Query rows changed from `{ClassName: {...}}` to `{uuid, properties, metadata}`: `row.ClassName.title` becomes `row.properties.title`, and `_additional.id` becomes `uuid`.
- `BatchCreate` now returns `insertedCount`, `failedCount` and `errors`, and fails on rejected objects by default.
- `Delete` with neither `objectId` nor filters now requires `deleteAll: true`, and fails on rejected deletions by default (`failOnError`).
- `SchemaCreate` takes one data type per field; the legacy `string` type is created as `text`. It defaults to `vectorizer: NONE` instead of the server's default vectorizer, so set `vectorizer` if you relied on it.
- Aggregations, `groupBy`, generative search and `Explore` are not supported. Stay on a plugin version built on the v5 client, or call the Weaviate REST API with `io.kestra.plugin.core.http.Request`.
