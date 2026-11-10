# How to use the Weaviate plugin

Search, insert, and manage objects and collections in Weaviate from Kestra flows. The plugin uses the Weaviate Java client v6, which talks to Weaviate over REST and gRPC and requires **Weaviate 1.32 or later**.

## Connection

Set `url` to your Weaviate URL, e.g. `http://localhost:8080`. For authenticated clusters, set `apiKey`. Store secrets in [secrets](https://kestra.io/docs/concepts/secret). Set connection properties on each task, or once per flow with `pluginDefaults` as in the example below.

Queries and batch inserts use gRPC, so the gRPC port must be reachable from the Kestra worker:

- Self-hosted Weaviate: gRPC defaults to the host of `url` on port `50051`. Override it with `grpcHost` and `grpcPort`. gRPC uses TLS when `url` is https.
- Weaviate Cloud: URLs ending in `.weaviate.cloud` or `.weaviate.network` are detected automatically and connect to the matching gRPC endpoint on port 443. `apiKey` is required.

Pass extra headers with `headers`, for example vectorizer API keys such as `X-OpenAI-Api-Key`. They are sent on both REST and gRPC calls.

## Tasks

`SchemaCreate` creates a collection. Set `className`, `fields` as a map of property name to Weaviate data type (`text`, `int`, `number`, `boolean`, `date`, `uuid`, their `[]` array forms, `blob`, `geoCoordinates`, `phoneNumber`), and optionally a `vectorizer`. To configure a property, give a map with `type` and any of the following, as in the example below:

- `description`;
- `indexFilterable`, for equality and most filters;
- `indexRangeFilters`, for faster `>`, `<`, `>=` and `<=` on `int`, `number` and `date`;
- `indexSearchable`, for BM25;
- `tokenization`: `WORD`, `LOWERCASE`, `WHITESPACE`, `FIELD`, `TRIGRAM`, `GSE`, `KAGOME_JA` or `KAGOME_KR`;
- `textAnalyzer`: `foldAscii`, `keepAscii` or `stopwordPreset`.

`vectorizeProperties` chooses which properties the vectorizer embeds. `invertedIndex` sets collection-wide options:

- `indexTimestamps`, `indexNullState` and `indexPropertyLength`, which some filters need;
- `bm25` (`b`, `k1`) and `stopwords` (`preset`, `additions`, `removals`), which tune keyword search;
- `cleanupIntervalSeconds`.

`quantization` compresses the vectors, and `multiTenancy` isolates data per tenant; both are described below. Instead of these options, `definition` can take the whole collection as JSON, which you can generate with the [Weaviate collection builder](https://weaviate.github.io/weaviate-add-collection/); see [Collections from JSON](#collections-from-json). Set `skipIfExists: true` to make the task idempotent; an existing collection keeps its schema.

`BatchCreate` inserts objects; `mode` sets how they are sent, as described below. Set `objects` to a `kestra://` URI of an ION file or an inline list of maps. Use `uuidKey` and `vectorKey` to take object IDs and precomputed vectors from the input. The task fails if Weaviate rejects any object, unless `failOnError` is false; rejected objects are then listed in the `errors` output.

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
- `filters`: a list of `{property, operator, value, valueType}` conditions, combined with `filtersOperator` (`AND` or `OR`). Set `valueType: DATE` or `UUID` for dates and UUIDs written as strings.

  An entry can also be a group: `and` or `or` with a list of conditions, or `not` with one condition. Groups nest, as in the `curated` task of the example below.

| Operator | Matches | Needs |
|---|---|---|
| `EQUAL`, `NOT_EQUAL` | equal, not equal | |
| `GREATER_THAN`, `GREATER_THAN_EQUAL`, `LESS_THAN`, `LESS_THAN_EQUAL` | numbers, dates and text compared | `indexRangeFilters` makes them faster on `int`, `number` and `date` |
| `LIKE` | text with `*` and `?` wildcards | |
| `CONTAINS_ANY`, `CONTAINS_ALL`, `CONTAINS_NONE` | arrays holding any, all or none of the listed values; on text, any, all or none of the tokens | |
| `IS_NULL` | `value: true` for missing values, `false` for present ones | `invertedIndex.indexNullState` |
| `WITHIN_GEO_RANGE` | `geoCoordinates` within `value.distance` meters of `value.latitude`/`value.longitude` | |

Special properties:

- `_id`: the object UUID.
- `_creationTimeUnix`, `_lastUpdateTimeUnix`: object timestamps. These need `invertedIndex.indexTimestamps`.
- `len(name)`: the length of a text or array property. This needs `invertedIndex.indexPropertyLength`.

## Batching

`BatchCreate.mode` sets how objects are sent to Weaviate:

> **Changed in 2.1:** `mode` defaults to `AUTO`. Up to 2.0, `BatchCreate` always sent sequential requests of `batchSize` objects; set `mode: FIXED_SIZE` to keep that behavior.

| `mode` | How it sends | Settings |
|---|---|---|
| `AUTO` (default) | `SERVER_SIDE` on Weaviate 1.36 or later, otherwise `DYNAMIC` | |
| `SERVER_SIDE` | streams objects; Weaviate sets the pace from its load. Generally available from Weaviate 1.36; fails fast on older servers | `batchSize` (starting size) |
| `DYNAMIC` | doubles the batch size after requests under 1 s and halves it after requests over 5 s | `batchSize` (starting size) |
| `FIXED_SIZE` | the same number of objects in every request | `batchSize`, `concurrentRequests` (1 to 32) |
| `RATE_LIMIT` | at most `objectsPerMinute`, e.g. to stay under a vectorizer provider's rate limit | `objectsPerMinute`, `batchSize` |

The outputs report what was used:

- `mode`: the mode `AUTO` resolved to;
- `requestCount`: the number of insert requests sent (not reported for `SERVER_SIDE`, where Weaviate sets the batches).

When the collection does not exist yet, auto-schema creates it on the first insert. Concurrent requests (`SERVER_SIDE`, or `FIXED_SIZE` with `concurrentRequests` above 1) race to create it, and Weaviate rejects the losers' objects with "class name ... already exists". To avoid this, `BatchCreate` sends the first object on its own before going concurrent.

To learn more, see the Weaviate docs on [batch import](https://docs.weaviate.io/weaviate/manage-objects/import).

## Collections from JSON

`SchemaCreate.definition` takes the whole collection in the JSON format of Weaviate's REST API, as a map or a JSON string. The task sends it to Weaviate unchanged, so every collection option the server supports is available, including nested object properties and settings this plugin has no property for. Build a definition visually with the [Weaviate collection builder](https://weaviate.github.io/weaviate-add-collection/), copy its JSON into the task, and the task creates the collection:

```yaml
- id: create_products
  type: io.kestra.plugin.weaviate.SchemaCreate
  skipIfExists: true
  definition: |
    {
      "class": "Product",
      "properties": [
        { "name": "name", "dataType": ["text"] },
        { "name": "sku", "dataType": ["text"], "tokenization": "field", "indexSearchable": false }
      ],
      "vectorConfig": {
        "default": { "vectorizer": { "none": {} }, "vectorIndexType": "hnsw" }
      }
    }
```

- `className`, if set, replaces `class`, so one definition can serve several collections.
- `skipIfExists` and `multiTenancy.tenants` still apply.
- The other collection options (`fields`, `vectorizer`, `quantization`, `invertedIndex`, `multiTenancy` settings) can't be combined with `definition`.
- If Weaviate rejects the definition, the task fails with the server's message.

## Vector quantization

Quantization compresses the vectors in memory, which cuts memory use and cost at a small loss in recall. Weaviate re-scores the top candidates with the original vectors to recover most of that loss. Set it when the collection is created, with `SchemaCreate.quantization`:

| `type` | Compression | Options |
|---|---|---|
| `RQ` (rotational, recommended) | about 4x with `bits: 8` (default), about 32x with `bits: 1`; no training needed | `bits` |
| `BQ` (binary) | about 32x; best with high-dimensional vectors | |
| `SQ` (scalar) | about 4x; trains on the first objects | |
| `PQ` (product) | configurable; trains on the first objects | `segments`, `centroids` |
| `NONE` | none, even if the server sets a default quantization | |

Without `quantization`, the server's default applies. That is no compression, unless the server sets one with `DEFAULT_QUANTIZATION`.

To learn more, see these Weaviate docs:

- [compression overview](https://docs.weaviate.io/weaviate/concepts/vector-quantization);
- [how to configure compression](https://docs.weaviate.io/weaviate/configuration/compression);
- one page per method: [RQ](https://docs.weaviate.io/weaviate/configuration/compression/rq-compression), [BQ](https://docs.weaviate.io/weaviate/configuration/compression/bq-compression), [SQ](https://docs.weaviate.io/weaviate/configuration/compression/sq-compression), [PQ](https://docs.weaviate.io/weaviate/configuration/compression/pq-compression);
- [choosing a method](https://docs.weaviate.io/weaviate/starter-guides/managing-resources/compression).

The plugin does not expose `rescoreLimit`, `trainingLimit` or the PQ encoder settings yet. The Weaviate Java client 6.3.2 sends them in a form the server ignores, so Weaviate's defaults apply.

## Multi-tenancy

A multi-tenant collection keeps each tenant's objects in a separate shard. Tenants never see each other's data, and each one can be activated, deactivated or deleted on its own. This suits one tenant per customer, team or user. Enable it with `SchemaCreate.multiTenancy`:

- `tenants`: tenants to create with the collection. With `skipIfExists: true`, a rerun adds the ones that are missing. The `tenantsCreated` output lists the tenants each run added.
- `autoTenantCreation`: `BatchCreate` creates a tenant the first time it writes to it.
- `autoTenantActivation`: an inactive tenant is activated when it is accessed.

`BatchCreate`, `Query` and `Delete` must then set `tenant`, and they only see that tenant's objects. To learn more, see the Weaviate docs on [multi-tenancy operations](https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy) and [tenant states](https://docs.weaviate.io/weaviate/starter-guides/managing-resources/tenant-states).

## Example: load and search a collection

This flow uses every task, every filter operator, and every index option. It works with a knowledge base of articles:

- **Collections:** `SchemaCreate` creates three collections:
  - `Article`: Weaviate computes its `content` vector from the text with Cohere (`TEXT2VEC_COHERE`, `vectorizerModel`), and compresses it with RQ quantization.
  - `Snippet`: stores vectors you provide. It is created from a JSON `definition`, as exported by the collection builder, with RQ settings that have no task property.
  - `Note`: multi-tenant, one tenant per team. `team-a` is created up front and `team-b` on first insert.

  In `Article`:
  - each field states how it is indexed: `indexRangeFilters`, `indexFilterable`, `indexSearchable`, `tokenization` and `textAnalyzer`;
  - `vectorizeProperties` keeps labels out of the vector;
  - `invertedIndex` turns on the indexes that the timestamp, null and length filters need, and tunes BM25 and stopwords.
- **Loading:** `BatchCreate` loads articles from an ION file and from an inline list, snippets with their own vectors (`vectorKey`, `targetVector`), and notes into each team's `tenant`. Each load uses a different batching mode: `AUTO`, which resolves to `SERVER_SIDE`, `FIXED_SIZE` with `concurrentRequests`, `RATE_LIMIT` and `DYNAMIC`. The report shows each mode. One article is rejected on purpose; with `failOnError: false` it is reported in `errors` and the task still succeeds.
- **Searching:** `Query` runs each search type:
  - `BM25` with boosted `queryProperties`;
  - `HYBRID` with `alpha` and a simple `filter`;
  - `NEAR_TEXT` with `certainty` and structured `filters` combined with `OR`;
  - `FETCH_OBJECTS` with every other filter:
    - a range on `wordCount`;
    - `CONTAINS_ALL` and `CONTAINS_NONE` on tags;
    - `WITHIN_GEO_RANGE` on the author's office;
    - `IS_NULL` on the publication date;
    - `len(title)`;
    - `_lastUpdateTimeUnix`;
    - a nested group with `EQUAL`, `NOT_EQUAL`, `or` and `not`.
  - `NEAR_VECTOR`, once with the vector of an article fetched with `includeVector`, once with a hand-written vector.

  A `tenant` query returns only team-a's notes. The flow also uses each `fetchType`: `FETCH`, `FETCH_ONE`, `STORE` and `NONE`.
- **Deleting:** `Delete` removes one object by `objectId`, then every draft by filter. The `reset` task shows `deleteAll` and is disabled.
- **Connection:** `pluginDefaults` sets it once for all Weaviate tasks. It points at `http://weaviate:8080`, the Weaviate of the README's Docker Compose stack, so the flow runs there unchanged. It also passes the Cohere API key in `headers`, and comments show the `localhost` and Weaviate Cloud variants.

Running the flow again is safe. `skipIfExists` reuses the collections, and `uuidKey` replaces objects that have the same `id` instead of duplicating them.

Store your Cohere API key as the `COHERE_API_KEY` secret. Weaviate calls Cohere with it whenever it vectorizes objects or `NEAR_TEXT`/`HYBRID` queries. The `text2vec-cohere` module is enabled by default on recent Weaviate servers and on Weaviate Cloud.

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
    vectorizeProperties: [title, body]  # labels such as category and tags stay out of the vector
    # Compresses the vectors about 4x (RQ, 8 bits per dimension) with little loss in recall.
    # See https://docs.weaviate.io/weaviate/configuration/compression/rq-compression
    quantization:
      type: RQ
      bits: 8
    skipIfExists: true
    # Collection-wide indexes that some filters need, and keyword-search tuning.
    invertedIndex:
      indexTimestamps: true       # filter on _creationTimeUnix / _lastUpdateTimeUnix (`updated_this_run`)
      indexNullState: true        # IS_NULL (`unpublished`)
      indexPropertyLength: true   # len(property) (`short_titles`)
      bm25:
        b: 0.75                   # length normalization (Weaviate's default)
        k1: 1.2                   # term frequency saturation (Weaviate's default)
      stopwords:
        preset: en
        additions: [kestra]       # ignored by keyword search
        removals: [not]           # kept, so "not" can be searched for
      cleanupIntervalSeconds: 60
    # A bare type keeps Weaviate's defaults: searchable, filterable and vectorized.
    # A map states how the property is used, so Weaviate only builds the indexes it needs.
    fields:
      title:
        type: text
        tokenization: word        # the default: lowercase words, punctuation removed
        textAnalyzer:
          foldAscii: true         # "café" also matches "cafe"
      body:
        type: text
        description: Full article text
        indexFilterable: false    # searched (BM25 and vectors), never filtered on
      category:
        type: text
        tokenization: field       # the whole value is one token: exact labels
        indexSearchable: false    # filtered on, never keyword-searched
      tags:
        type: text[]
        tokenization: field       # "disaster-recovery" stays one tag instead of two words
      wordCount:
        type: int
        indexRangeFilters: true   # speeds up the range filter in `mid_length`
      rating:
        type: number
        indexFilterable: false    # never compared for equality...
        indexRangeFilters: true   # ...only with >, <, >= and <= (`curated`)
      published: boolean
      publishedAt:
        type: date
        indexRangeFilters: true   # speeds up the date filter in `near_text`
      office: geoCoordinates      # where the author works (`near_berlin`)

  # Collection for vectors you compute yourself, written in Weaviate's own JSON format, as exported by
  # https://weaviate.github.io/weaviate-add-collection/. Every server option is available this way.
  - id: create_snippets
    type: io.kestra.plugin.weaviate.SchemaCreate
    skipIfExists: true
    definition: |
      {
        "class": "Snippet",
        "properties": [{ "name": "text", "dataType": ["text"] }],
        "vectorConfig": {
          "embedding": {
            "vectorizer": { "none": {} },
            "vectorIndexType": "hnsw",
            "vectorIndexConfig": { "rq": { "enabled": true, "rescoreLimit": 50 } }
          }
        }
      }

  # One tenant per team: each team's notes live in their own shard and never mix.
  # team-a is created here; team-b is created on first insert (autoTenantCreation).
  # See https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy
  - id: create_notes
    type: io.kestra.plugin.weaviate.SchemaCreate
    className: Note
    vectorizer: NONE
    skipIfExists: true             # on reruns, still adds tenants listed below that are missing
    multiTenancy:
      autoTenantCreation: true
      autoTenantActivation: true
      tenants: [team-a]
    fields:
      text: text

  # An ION file, as produced by most Kestra tasks that read data.
  - id: articles_file
    type: io.kestra.plugin.core.storage.Write
    extension: .ion
    content: |
      {id:"2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0001",title:"Backups that actually restore",body:"Test your restore process regularly and keep copies in another region.",category:"operations",tags:["backup","disaster-recovery"],wordCount:900,rating:4.8,published:true,publishedAt:2024-02-01T00:00:00Z,office:{latitude:52.52,longitude:13.40}}
      {id:"2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0002",title:"Encrypting data at rest",body:"Turn on disk encryption and rotate keys to protect stored data.",category:"security",tags:["encryption","compliance"],wordCount:1200,rating:4.6,published:true,publishedAt:2024-05-15T09:30:00Z,office:{latitude:48.86,longitude:2.35}}

  # The default mode, AUTO, uses server-side batching (Weaviate 1.36+): Weaviate sets the pace from its load.
  - id: load_file
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Article
    objects: "{{ outputs.articles_file.uri }}"
    uuidKey: id

  # FIXED_SIZE: 2 objects per request, 2 requests in parallel.
  - id: load_inline
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Article
    uuidKey: id
    mode: FIXED_SIZE
    batchSize: 2
    concurrentRequests: 2
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
        office: {latitude: 40.71, longitude: -74.01}
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0004
        title: Least-privilege API keys
        body: Give each service its own key with only the permissions it needs.
        category: security
        tags: [access-control, api-keys]
        wordCount: 650
        rating: 4.9
        published: true
        publishedAt: "2024-08-02T00:00:00Z"
        office: {latitude: -23.55, longitude: -46.63}
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0005
        title: Draft notes on audit logging
        body: Record who changed what, and keep the logs where attackers cannot edit them.
        category: security
        tags: [audit, compliance]
        wordCount: 300
        published: false          # no publishedAt: found by IS_NULL in `unpublished`
        office: {latitude: 35.68, longitude: 139.69}
      # Rejected on purpose: wordCount must be an int. With failOnError: false it is reported in `errors`.
      - id: 2f1c9a52-6a0e-4c43-9d63-1b6f1d0a0006
        title: Broken object
        wordCount: a lot

  # RATE_LIMIT: at most 120 objects per minute, e.g. to stay under a vectorizer provider's limit.
  - id: load_snippets
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Snippet
    mode: RATE_LIMIT
    objectsPerMinute: 120
    uuidKey: id
    vectorKey: vec
    targetVector: embedding
    objects:
      - {id: 7c0d3e1a-1111-4b6e-9a51-000000000001, text: north, vec: [1.0, 0.0, 0.0]}
      - {id: 7c0d3e1a-1111-4b6e-9a51-000000000002, text: north-east, vec: [0.7, 0.7, 0.0]}
      - {id: 7c0d3e1a-1111-4b6e-9a51-000000000003, text: up, vec: [0.0, 0.0, 1.0]}

  # DYNAMIC grows or shrinks the batch size with how fast Weaviate answers.
  - id: load_team_a_notes
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Note
    mode: DYNAMIC
    tenant: team-a
    uuidKey: id
    objects:
      - {id: 9b1d0000-0000-4000-8000-0000000000a1, text: Rotate the staging API keys on Friday}
      - {id: 9b1d0000-0000-4000-8000-0000000000a2, text: Restore test passed for the March backup}

  - id: load_team_b_notes
    type: io.kestra.plugin.weaviate.BatchCreate
    className: Note
    tenant: team-b
    uuidKey: id
    objects:
      - {id: 9b1d0000-0000-4000-8000-0000000000b1, text: Add a read replica in us-east}

  # Only team-a's notes, even without a filter.
  - id: team_a_notes
    type: io.kestra.plugin.weaviate.Query
    className: Note
    tenant: team-a
    returnProperties: [text]
    fetchType: FETCH

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

  # Range filter: articles of 600 to 1000 words. Both conditions must match (filtersOperator defaults to AND).
  # The data has one article above the range (1200 words) and one below (300).
  - id: mid_length
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: wordCount
        operator: GREATER_THAN_EQUAL
        value: 600
      - property: wordCount
        operator: LESS_THAN_EQUAL
        value: 1000
    limit: 100
    returnProperties: [title, wordCount, rating]
    fetchType: FETCH

  # Articles tagged with both values.
  - id: tagged_all
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: tags
        operator: CONTAINS_ALL
        value: [encryption, compliance]
    returnProperties: [title]
    fetchType: FETCH

  # Articles with none of the tags.
  - id: tagged_none
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: tags
        operator: CONTAINS_NONE
        value: [compliance]
    returnProperties: [title]
    fetchType: FETCH

  # Authors working within 1000 km of Berlin: Berlin and Paris, not New York, São Paulo or Tokyo.
  - id: near_berlin
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: office
        operator: WITHIN_GEO_RANGE
        value:
          latitude: 52.52
          longitude: 13.40
          distance: 1000000         # meters
    returnProperties: [title, office]
    fetchType: FETCH

  # Articles without a publication date (needs indexNullState).
  - id: unpublished
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: publishedAt
        operator: IS_NULL
        value: true
    returnProperties: [title]
    fetchType: FETCH

  # Titles shorter than 25 characters (needs indexPropertyLength).
  - id: short_titles
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: len(title)
        operator: LESS_THAN
        value: 25
    returnProperties: [title]
    fetchType: FETCH

  # Objects written by this execution (needs indexTimestamps).
  - id: updated_this_run
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: _lastUpdateTimeUnix
        operator: GREATER_THAN_EQUAL
        value: "{{ execution.startDate }}"
        valueType: DATE
    limit: 100
    returnMetadata: [LAST_UPDATE_TIME]
    fetchType: FETCH

  # Groups nest: published, not in operations, and either highly rated or not about encryption.
  - id: curated
    type: io.kestra.plugin.weaviate.Query
    className: Article
    filters:
      - property: published
        operator: EQUAL
        value: true
      - property: category
        operator: NOT_EQUAL
        value: operations
      - or:
          - property: rating
            operator: GREATER_THAN
            value: 4.7
          - not:
              property: tags
              operator: CONTAINS_ANY
              value: [encryption]
    returnProperties: [title, rating]
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
      Batching: load_file {{ outputs.load_file.mode }}; load_inline {{ outputs.load_inline.mode }}, {{ outputs.load_inline.requestCount }} requests; load_snippets {{ outputs.load_snippets.mode }}; load_team_a_notes {{ outputs.load_team_a_notes.mode }}
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
      Between 600 and 1000 words ({{ outputs.mid_length.size }}):
      {%- for row in outputs.mid_length.rows %}
        {{ row.properties.wordCount }} words, rated {{ row.properties.rating }}: {{ row.properties.title }}
      {%- endfor %}
      Tagged encryption AND compliance: {{ outputs.tagged_all.rows | jq('[.[].properties.title]') | first }}
      Not tagged compliance: {{ outputs.tagged_none.rows | jq('[.[].properties.title]') | first }}
      Authors within 1000 km of Berlin: {{ outputs.near_berlin.rows | jq('[.[].properties.title]') | first }}
      Unpublished: {{ outputs.unpublished.rows | jq('[.[].properties.title]') | first }}
      Titles under 25 characters: {{ outputs.short_titles.rows | jq('[.[].properties.title]') | first }}
      Written by this run: {{ outputs.updated_this_run.size }} articles
      Curated: {{ outputs.curated.rows | jq('[.[].properties.title]') | first }}
      Tenants created by create_notes: {{ outputs.create_notes.tenantsCreated }}
      team-a notes ({{ outputs.team_a_notes.size }}): {{ outputs.team_a_notes.rows | jq('[.[].properties.text]') | first }}

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
      # `weaviate` is the Weaviate service of docker-compose-dev.yml (see the README).
      # If Kestra runs outside Docker, use url http://localhost:8080 and grpcHost localhost instead.
      url: http://weaviate:8080
      grpcHost: weaviate
      grpcPort: 50051
      requestTimeout: PT60S # includes the time Weaviate waits for Cohere to vectorize the query
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
