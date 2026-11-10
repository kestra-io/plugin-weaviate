package io.kestra.plugin.weaviate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;

import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.InvertedIndex;
import io.weaviate.client6.v1.api.collections.MultiTenancy;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.Quantization;
import io.weaviate.client6.v1.api.collections.TextAnalyzer;
import io.weaviate.client6.v1.api.collections.Tokenization;
import io.weaviate.client6.v1.api.collections.VectorConfig;
import io.weaviate.client6.v1.api.collections.VectorIndex;
import io.weaviate.client6.v1.api.collections.tenants.Tenant;
import io.weaviate.client6.v1.internal.ObjectBuilder;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Create a Weaviate collection",
    description = """
        Creates a collection (class) with the given properties and vectorizer. Fails if the collection already exists,
        unless `skipIfExists` is true."""
)
@Plugin(
    examples = {
        @Example(
            title = "Create a collection for objects with precomputed vectors.",
            full = true,
            code = """
                id: create_weaviate_schema
                namespace: company.team

                tasks:
                  - id: schema
                    type: io.kestra.plugin.weaviate.SchemaCreate
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Movies
                    fields:
                      name: text
                      description: text
                      year: int
                      genres: text[]
                """
        ),
        @Example(
            title = "Create a collection vectorized by Weaviate Embeddings, if it does not exist yet.",
            full = true,
            code = """
                id: create_weaviate_collection_vectorizer
                namespace: company.team

                tasks:
                  - id: schema
                    type: io.kestra.plugin.weaviate.SchemaCreate
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: Question
                    vectorizer: TEXT2VEC_WEAVIATE
                    skipIfExists: true
                    fields:
                      question: text
                      answer: text
                      category: text
                """
        ),
        @Example(
            title = "Configure how each property is indexed, e.g. enable range filters on numbers and dates.",
            full = true,
            code = """
                id: create_weaviate_orders
                namespace: company.team

                tasks:
                  - id: schema
                    type: io.kestra.plugin.weaviate.SchemaCreate
                    url: http://localhost:8080
                    className: Order
                    skipIfExists: true
                    fields:
                      orderId: uuid
                      total:
                        type: number
                        indexRangeFilters: true
                      createdAt:
                        type: date
                        indexRangeFilters: true
                      note:
                        type: text
                        description: Free-text note from the customer
                        indexFilterable: false
                      status:
                        type: text
                        indexSearchable: false
                """
        ),
        @Example(
            title = "Multi-tenant collection with one tenant per customer and RQ-compressed vectors.",
            full = true,
            code = """
                id: create_weaviate_tenants
                namespace: company.team

                tasks:
                  - id: schema
                    type: io.kestra.plugin.weaviate.SchemaCreate
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: SupportTicket
                    vectorizer: TEXT2VEC_WEAVIATE
                    skipIfExists: true
                    quantization:
                      type: RQ
                      bits: 8
                    multiTenancy:
                      autoTenantCreation: true
                      tenants:
                        - acme
                        - globex
                    fields:
                      subject: text
                      body: text

                  - id: insert
                    type: io.kestra.plugin.weaviate.BatchCreate
                    url: https://demo-cluster-id.weaviate.cloud
                    apiKey: "{{ secret('WEAVIATE_API_KEY') }}"
                    className: SupportTicket
                    tenant: acme
                    objects:
                      - subject: Cannot log in
                        body: The password reset email never arrives.
                """
        ),
        @Example(
            title = "Create a collection from its JSON definition, e.g. as exported by https://weaviate.github.io/weaviate-add-collection/.",
            full = true,
            code = """
                id: create_weaviate_from_json
                namespace: company.team

                tasks:
                  - id: schema
                    type: io.kestra.plugin.weaviate.SchemaCreate
                    url: http://localhost:8080
                    skipIfExists: true
                    definition: |
                      {
                        "class": "Product",
                        "properties": [
                          { "name": "name", "dataType": ["text"] },
                          {
                            "name": "dimensions",
                            "dataType": ["object"],
                            "nestedProperties": [
                              { "name": "width", "dataType": ["number"] },
                              { "name": "unit", "dataType": ["text"] }
                            ]
                          }
                        ],
                        "vectorConfig": {
                          "default": {
                            "vectorizer": { "none": {} },
                            "vectorIndexType": "hnsw",
                            "vectorIndexConfig": { "rq": { "enabled": true, "rescoreLimit": 50 } }
                          }
                        }
                      }
                """
        )
    }
)
public class SchemaCreate extends WeaviateConnection implements RunnableTask<SchemaCreate.Output> {
    private static final Set<String> FIELD_OPTIONS = Set.of(
        "type", "description", "indexFilterable", "indexRangeFilters", "indexSearchable", "tokenization", "textAnalyzer"
    );
    private static final Set<String> TEXT_ANALYZER_OPTIONS = Set.of("foldAscii", "keepAscii", "stopwordPreset");

    @Schema(
        title = "Collection (class) name",
        description = "Required, unless `definition` names the collection in `class`. With `definition`, it overrides that name."
    )
    @PluginProperty(group = "main")
    private io.kestra.core.models.property.Property<String> className;

    @Schema(
        title = "Collection definition as Weaviate JSON",
        description = """
            The full collection, in the JSON format of Weaviate's REST API (`POST /v1/schema`), as a map or a JSON string.
            It is sent to Weaviate as is, so every option the server supports is available, including ones this task has
            no property for. Build one visually with https://weaviate.github.io/weaviate-add-collection/.
            Use it instead of `fields`, `vectorizer`, `quantization`, `invertedIndex` and the other collection options;
            `skipIfExists` and `multiTenancy.tenants` still apply."""
    )
    @PluginProperty(group = "main")
    private io.kestra.core.models.property.Property<Map<String, Object>> definition;

    @Schema(
        title = "Properties to add to the collection",
        description = """
            Map of property name to a Weaviate data type: text, int, number, boolean, date, uuid (each also as an array, e.g. text[]),
            blob, geoCoordinates or phoneNumber. A single-element list is also accepted; the legacy `string` type maps to text.
            To configure a property, give a map instead: `type` plus any of `description`, `indexFilterable` (equality filters),
            `indexRangeFilters` (range filters on int, number and date), `indexSearchable` (BM25 on text), `tokenization`
            (how text is split into tokens: WORD, LOWERCASE, WHITESPACE, FIELD, TRIGRAM, GSE, KAGOME_JA or KAGOME_KR),
            and `textAnalyzer` (`foldAscii`, `keepAscii`, `stopwordPreset`). Unset options keep Weaviate's defaults.
            To choose which properties are vectorized, use `vectorizeProperties`."""
    )
    @PluginProperty(group = "main")
    private io.kestra.core.models.property.Property<Map<String, Object>> fields;

    @Schema(
        title = "Vectorizer",
        description = """
            NONE (default) means vectors are provided on insert, e.g. with `BatchCreate.vectorKey`.
            Other values vectorize text properties server-side; the module must be enabled on the server.
            Unlike plugin versions built on the v5 client, the server's DEFAULT_VECTORIZER_MODULE is not used, so set this explicitly."""
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<Vectorizer> vectorizer = io.kestra.core.models.property.Property.ofValue(Vectorizer.NONE);

    @Schema(title = "Vectorizer model", description = "Model name for TEXT2VEC_COHERE, TEXT2VEC_OPENAI and TEXT2VEC_WEAVIATE. Defaults to the module default.")
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<String> vectorizerModel;

    @Schema(
        title = "Properties the vectorizer embeds",
        description = """
            Text properties the vectorizer turns into the vector (Weaviate's `sourceProperties`). Defaults to all text properties.
            Leave out labels and IDs so they don't affect similarity."""
    )
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<List<String>> vectorizeProperties;

    @Schema(
        title = "Vector quantization (compression)",
        description = """
            Compresses the vectors to cut memory use, at a small cost in recall. `type` is RQ (rotational, recommended), BQ
            (binary), SQ (scalar), PQ (product), or NONE to keep vectors uncompressed even if the server sets a default
            quantization. Unset uses the server default. See https://docs.weaviate.io/weaviate/configuration/compression."""
    )
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<QuantizationOptions> quantization;

    @Schema(title = "Vector name", description = "Creates a named vector instead of the default one.")
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<String> vectorName;

    @Schema(
        title = "Collection-wide inverted index settings",
        description = """
            Enables the extra indexes some filters need (`indexTimestamps`, `indexNullState`, `indexPropertyLength`) and tunes
            keyword search (`bm25`, `stopwords`). Unset values keep Weaviate's defaults."""
    )
    @PluginProperty(group = "advanced")
    private io.kestra.core.models.property.Property<InvertedIndexOptions> invertedIndex;

    @Schema(
        title = "Multi-tenancy",
        description = """
            Stores each tenant's objects in its own shard, so tenants are isolated and can be activated, deactivated or deleted
            on their own. `BatchCreate`, `Query` and `Delete` then need `tenant`. `tenants` are created with the collection;
            when the collection already exists and `skipIfExists` is true, missing ones are added.
            See https://docs.weaviate.io/weaviate/manage-collections/multi-tenancy."""
    )
    @PluginProperty(group = "advanced")
    private io.kestra.core.models.property.Property<MultiTenancyOptions> multiTenancy;

    @Schema(title = "Succeed without changes if the collection exists")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private io.kestra.core.models.property.Property<Boolean> skipIfExists = io.kestra.core.models.property.Property.ofValue(false);

    @Override
    public SchemaCreate.Output run(RunContext runContext) throws Exception {
        var rDefinition = runContext.render(definition).asMap(String.class, Object.class);
        if (!rDefinition.isEmpty()) {
            return createFromDefinition(runContext, rDefinition);
        }
        var rClassName = runContext.render(className).as(String.class).orElseThrow(() -> new IllegalArgumentException("`className` is required."));
        var properties = buildProperties(runContext.render(fields).asMap(String.class, Object.class), runContext.logger());
        var rVectorizeProperties = runContext.render(vectorizeProperties).asList(String.class);
        var vectorConfig = buildVectorConfig(
            runContext.render(vectorizer).as(Vectorizer.class).orElse(Vectorizer.NONE),
            runContext.render(vectorizerModel).as(String.class).orElse(null),
            runContext.render(vectorName).as(String.class).orElse(null),
            vectorizeProperties == null ? null : rVectorizeProperties,
            buildQuantization(runContext.render(quantization).as(QuantizationOptions.class).orElse(null))
        );
        boolean rSkipIfExists = runContext.render(skipIfExists).as(Boolean.class).orElse(false);
        var rInvertedIndex = runContext.render(invertedIndex).as(InvertedIndexOptions.class).orElse(null);
        var rMultiTenancy = runContext.render(multiTenancy).as(MultiTenancyOptions.class).orElse(null);
        var tenantNames = tenantNames(rMultiTenancy);

        try (WeaviateClient client = connect(runContext)) {
            if (rSkipIfExists && client.collections.exists(rClassName)) {
                var added = createMissingTenants(client, rClassName, tenantNames);
                runContext.logger().info("Collection '{}' already exists, skipping; added {} missing tenants", rClassName, added.size());
                return Output.builder().success(true).created(false).tenantsCreated(added).build();
            }

            client.collections.create(rClassName, c ->
            {
                c.properties(properties).vectorConfig(vectorConfig);
                if (rInvertedIndex != null) {
                    c.invertedIndex(buildInvertedIndex(rInvertedIndex));
                }
                if (rMultiTenancy != null) {
                    c.multiTenancy(buildMultiTenancy(rMultiTenancy));
                }
                return c;
            });
            var added = createMissingTenants(client, rClassName, tenantNames);
            return Output.builder().success(true).created(true).tenantsCreated(added).build();
        }
    }

    private SchemaCreate.Output createFromDefinition(RunContext runContext, Map<String, Object> rDefinition) throws Exception {
        var conflicting = new ArrayList<String>();
        if (fields != null) {
            conflicting.add("fields");
        }
        if (vectorizerModel != null) {
            conflicting.add("vectorizerModel");
        }
        if (vectorizeProperties != null) {
            conflicting.add("vectorizeProperties");
        }
        if (vectorName != null) {
            conflicting.add("vectorName");
        }
        if (quantization != null) {
            conflicting.add("quantization");
        }
        if (invertedIndex != null) {
            conflicting.add("invertedIndex");
        }
        if (runContext.render(vectorizer).as(Vectorizer.class).orElse(Vectorizer.NONE) != Vectorizer.NONE) {
            conflicting.add("vectorizer");
        }
        var rMultiTenancy = runContext.render(multiTenancy).as(MultiTenancyOptions.class).orElse(null);
        if (rMultiTenancy != null && (rMultiTenancy.getEnabled() != null || rMultiTenancy.getAutoTenantCreation() != null || rMultiTenancy.getAutoTenantActivation() != null)) {
            conflicting.add("multiTenancy (set multiTenancyConfig in the definition; only multiTenancy.tenants applies)");
        }
        if (!conflicting.isEmpty()) {
            throw new IllegalArgumentException("`definition` describes the whole collection; remove " + String.join(", ", conflicting) + ".");
        }

        var document = new LinkedHashMap<>(rDefinition);
        runContext.render(className).as(String.class).ifPresent(name -> document.put("class", name));
        if (!(document.get("class") instanceof String rClassName) || rClassName.isBlank()) {
            throw new IllegalArgumentException("`definition` must name the collection in `class`, or set `className`.");
        }
        var tenantNames = tenantNames(rMultiTenancy);
        if (!tenantNames.isEmpty() && !multiTenancyEnabled(document)) {
            throw new IllegalArgumentException("`multiTenancy.tenants` needs `multiTenancyConfig: {enabled: true}` in the definition.");
        }

        try (WeaviateClient client = connect(runContext)) {
            var existed = client.collections.exists(rClassName);
            if (existed && !runContext.render(skipIfExists).as(Boolean.class).orElse(false)) {
                throw new IllegalStateException("Collection '" + rClassName + "' already exists; set `skipIfExists: true` to reuse it.");
            }
            if (!existed) {
                restPost(runContext, "/v1/schema", JacksonMapper.ofJson().writeValueAsString(document));
                runContext.logger().info("Created collection '{}' from its JSON definition", rClassName);
            } else {
                runContext.logger().info("Collection '{}' already exists, skipping", rClassName);
            }
            var added = createMissingTenants(client, rClassName, tenantNames);
            return Output.builder().success(true).created(!existed).tenantsCreated(added).build();
        }
    }

    static boolean multiTenancyEnabled(Map<?, ?> definition) {
        return definition.get("multiTenancyConfig") instanceof Map<?, ?> config && Boolean.parseBoolean(String.valueOf(config.get("enabled")));
    }

    static List<String> tenantNames(MultiTenancyOptions options) {
        if (options == null || options.getTenants() == null || options.getTenants().isEmpty()) {
            return List.of();
        }
        if (Boolean.FALSE.equals(options.getEnabled())) {
            throw new IllegalArgumentException("`multiTenancy.tenants` needs multi-tenancy; remove `enabled: false`.");
        }
        var names = new LinkedHashSet<String>();
        for (var name : options.getTenants()) {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("`multiTenancy.tenants` must not contain empty names.");
            }
            names.add(name);
        }
        return List.copyOf(names);
    }

    static MultiTenancy buildMultiTenancy(MultiTenancyOptions options) {
        return MultiTenancy.of(m ->
        {
            m.enabled(options.getEnabled() == null || options.getEnabled());
            if (options.getAutoTenantCreation() != null) {
                m.autoTenantCreation(options.getAutoTenantCreation());
            }
            if (options.getAutoTenantActivation() != null) {
                m.autoTenantActivation(options.getAutoTenantActivation());
            }
            return m;
        });
    }

    private static List<String> createMissingTenants(WeaviateClient client, String className, List<String> names) throws IOException {
        if (names.isEmpty()) {
            return List.of();
        }
        var tenants = client.collections.use(className).tenants;
        var existing = tenants.get(names).stream().map(Tenant::name).collect(Collectors.toSet());
        var missing = names.stream().filter(n -> !existing.contains(n)).toList();
        if (!missing.isEmpty()) {
            tenants.create(missing.stream().map(Tenant::active).toList());
        }
        return missing;
    }

    static List<Property> buildProperties(Map<String, Object> fields, Logger logger) {
        List<Property> properties = new ArrayList<>();
        if (fields == null) {
            return properties;
        }
        fields.forEach((name, spec) -> properties.add(buildProperty(name, spec, logger)));
        return properties;
    }

    static Property buildProperty(String name, Object spec, Logger logger) {
        if (!(spec instanceof Map<?, ?> options)) {
            return buildProperty(name, dataType(name, spec), logger);
        }
        var unknown = new TreeSet<String>();
        options.keySet().forEach(key ->
        {
            if (!FIELD_OPTIONS.contains(String.valueOf(key))) {
                unknown.add(String.valueOf(key));
            }
        });
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Field '" + name + "' has unknown options " + unknown + "; supported: " + new TreeSet<>(FIELD_OPTIONS) + ".");
        }
        if (!(options.get("type") instanceof String type)) {
            throw new IllegalArgumentException("Field '" + name + "' must set `type` to a data type.");
        }
        return buildProperty(name, type, logger, b ->
        {
            if (options.get("description") != null) {
                b.description(String.valueOf(options.get("description")));
            }
            flag(name, options, "indexFilterable", b::indexFilterable);
            flag(name, options, "indexRangeFilters", b::indexRangeFilters);
            flag(name, options, "indexSearchable", b::indexSearchable);
            if (options.get("tokenization") != null) {
                b.tokenization(tokenization(name, options.get("tokenization")));
            }
            if (options.get("textAnalyzer") != null) {
                b.textAnalyzer(textAnalyzer(name, options.get("textAnalyzer")));
            }
            return b;
        });
    }

    private static void flag(String name, Map<?, ?> options, String key, Consumer<Boolean> setter) {
        var value = options.get(key);
        if (value == null) {
            return;
        }
        if (value instanceof Boolean bool) {
            setter.accept(bool);
        } else if ("true".equals(value) || "false".equals(value)) {
            setter.accept(Boolean.parseBoolean((String) value));
        } else {
            throw new IllegalArgumentException("Field '" + name + "': `" + key + "` must be true or false, got " + value + ".");
        }
    }

    private static Tokenization tokenization(String name, Object value) {
        try {
            return Tokenization.valueOf(String.valueOf(value).trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Field '" + name + "': unknown `tokenization` '" + value + "'; supported: " + Arrays.toString(Tokenization.values()) + "."
            );
        }
    }

    private static TextAnalyzer textAnalyzer(String name, Object value) {
        if (!(value instanceof Map<?, ?> options)) {
            throw new IllegalArgumentException("Field '" + name + "': `textAnalyzer` must be a map with " + new TreeSet<>(TEXT_ANALYZER_OPTIONS) + ".");
        }
        options.keySet().forEach(key ->
        {
            if (!TEXT_ANALYZER_OPTIONS.contains(String.valueOf(key))) {
                throw new IllegalArgumentException(
                    "Field '" + name + "': unknown `textAnalyzer` option '" + key + "'; supported: " + new TreeSet<>(TEXT_ANALYZER_OPTIONS) + "."
                );
            }
        });
        return TextAnalyzer.of(a ->
        {
            flag(name, options, "foldAscii", a::foldAscii);
            if (options.get("keepAscii") instanceof List<?> keep) {
                a.keepAscii(keep.stream().map(String::valueOf).toList());
            } else if (options.get("keepAscii") != null) {
                throw new IllegalArgumentException("Field '" + name + "': `textAnalyzer.keepAscii` must be a list of characters.");
            }
            if (options.get("stopwordPreset") != null) {
                a.stopwordPreset(String.valueOf(options.get("stopwordPreset")));
            }
            return a;
        });
    }

    static InvertedIndex buildInvertedIndex(InvertedIndexOptions options) {
        return InvertedIndex.of(i ->
        {
            if (options.getIndexTimestamps() != null) {
                i.indexTimestamps(options.getIndexTimestamps());
            }
            if (options.getIndexNullState() != null) {
                i.indexNulls(options.getIndexNullState());
            }
            if (options.getIndexPropertyLength() != null) {
                i.indexPropertyLength(options.getIndexPropertyLength());
            }
            if (options.getCleanupIntervalSeconds() != null) {
                i.cleanupIntervalSeconds(options.getCleanupIntervalSeconds());
            }
            var bm25 = options.getBm25();
            if (bm25 != null) {
                i.bm25(b ->
                {
                    if (bm25.getB() != null) {
                        b.b(bm25.getB().floatValue());
                    }
                    if (bm25.getK1() != null) {
                        b.k1(bm25.getK1().floatValue());
                    }
                    return b;
                });
            }
            var stopwords = options.getStopwords();
            if (stopwords != null) {
                i.stopwords(w ->
                {
                    if (stopwords.getPreset() != null) {
                        w.preset(stopwords.getPreset());
                    }
                    if (stopwords.getAdditions() != null) {
                        w.add(stopwords.getAdditions());
                    }
                    if (stopwords.getRemovals() != null) {
                        w.remove(stopwords.getRemovals());
                    }
                    return w;
                });
            }
            return i;
        });
    }

    private static String dataType(String name, Object rawType) {
        if (rawType instanceof String type) {
            return type;
        }
        if (rawType instanceof List<?> list && list.size() == 1 && list.getFirst() instanceof String type) {
            return type;
        }
        throw new IllegalArgumentException("Field '" + name + "' must have exactly one data type, got " + rawType + ".");
    }

    static Property buildProperty(String name, String type, Logger logger) {
        return buildProperty(name, type, logger, b -> b);
    }

    private static Property buildProperty(String name, String type, Logger logger, Function<Property.Builder, ObjectBuilder<Property>> o) {
        var normalized = type.trim().toLowerCase(Locale.ROOT);
        if (normalized.equals("string") || normalized.equals("string[]")) {
            if (logger != null) {
                logger.warn("Field '{}' uses the deprecated '{}' type; creating it as '{}'", name, type, normalized.replace("string", "text"));
            }
            normalized = normalized.replace("string", "text");
        }
        return switch (normalized) {
            case "text" -> Property.text(name, o);
            case "text[]" -> Property.textArray(name, o);
            case "int" -> Property.integer(name, o);
            case "int[]" -> Property.integerArray(name, o);
            case "number" -> Property.number(name, o);
            case "number[]" -> Property.numberArray(name, o);
            case "boolean" -> Property.bool(name, o);
            case "boolean[]" -> Property.boolArray(name, o);
            case "date" -> Property.date(name, o);
            case "date[]" -> Property.dateArray(name, o);
            case "uuid" -> Property.uuid(name, o);
            case "uuid[]" -> Property.uuidArray(name, o);
            case "blob" -> Property.blob(name, o);
            case "geocoordinates" -> Property.geoCoordinates(name, o);
            case "phonenumber" -> Property.phoneNumber(name, o);
            case "object", "object[]" -> throw new IllegalArgumentException(
                "Field '" + name + "': object properties need nested property definitions, which SchemaCreate does not support."
            );
            default -> throw new IllegalArgumentException(
                "Field '" + name + "' has unsupported data type '" + type + "'. Cross-references (capitalized class names) are not supported."
            );
        };
    }

    static Map.Entry<String, VectorConfig> buildVectorConfig(Vectorizer vectorizer, String model, String name) {
        return buildVectorConfig(vectorizer, model, name, null, null);
    }

    static Map.Entry<String, VectorConfig> buildVectorConfig(
        Vectorizer vectorizer, String model, String name, List<String> sourceProperties, Quantization quantization) {
        if (model != null && vectorizer != Vectorizer.TEXT2VEC_COHERE && vectorizer != Vectorizer.TEXT2VEC_OPENAI && vectorizer != Vectorizer.TEXT2VEC_WEAVIATE) {
            throw new IllegalArgumentException("`vectorizerModel` is only supported with TEXT2VEC_COHERE, TEXT2VEC_OPENAI and TEXT2VEC_WEAVIATE, not " + vectorizer + ".");
        }
        if (sourceProperties != null && sourceProperties.isEmpty()) {
            throw new IllegalArgumentException("`vectorizeProperties` must list at least one property.");
        }
        if (vectorizer == Vectorizer.NONE && sourceProperties != null) {
            throw new IllegalArgumentException("`vectorizeProperties` needs a vectorizer; with NONE, vectors are provided on insert.");
        }
        var vectorName = name != null ? name : VectorIndex.DEFAULT_VECTOR_NAME;
        return switch (vectorizer) {
            case NONE -> VectorConfig.selfProvided(vectorName, v -> quantization == null ? v : v.quantization(quantization));
            case TEXT2VEC_MODEL2VEC -> VectorConfig.text2vecModel2Vec(vectorName, v ->
            {
                if (quantization != null) {
                    v.quantization(quantization);
                }
                return sourceProperties == null ? v : v.sourceProperties(sourceProperties);
            });
            case TEXT2VEC_TRANSFORMERS -> VectorConfig.text2vecTransformers(vectorName, v ->
            {
                if (quantization != null) {
                    v.quantization(quantization);
                }
                return sourceProperties == null ? v : v.sourceProperties(sourceProperties);
            });
            case TEXT2VEC_COHERE -> VectorConfig.text2vecCohere(vectorName, v ->
            {
                if (model != null) {
                    v.model(model);
                }
                if (quantization != null) {
                    v.quantization(quantization);
                }
                return sourceProperties == null ? v : v.sourceProperties(sourceProperties);
            });
            case TEXT2VEC_OPENAI -> VectorConfig.text2vecOpenAi(vectorName, v ->
            {
                if (model != null) {
                    v.model(model);
                }
                if (quantization != null) {
                    v.quantization(quantization);
                }
                return sourceProperties == null ? v : v.sourceProperties(sourceProperties);
            });
            case TEXT2VEC_WEAVIATE -> VectorConfig.text2vecWeaviate(vectorName, v ->
            {
                if (model != null) {
                    v.model(model);
                }
                if (quantization != null) {
                    v.quantization(quantization);
                }
                return sourceProperties == null ? v : v.sourceProperties(sourceProperties);
            });
        };
    }

    static Quantization buildQuantization(QuantizationOptions options) {
        if (options == null) {
            return null;
        }
        if (options.getType() == null) {
            throw new IllegalArgumentException("`quantization.type` is required: NONE, RQ, BQ, SQ or PQ.");
        }
        var type = options.getType();
        allowed(type, "bits", options.getBits(), QuantizationType.RQ);
        allowed(type, "segments", options.getSegments(), QuantizationType.PQ);
        allowed(type, "centroids", options.getCentroids(), QuantizationType.PQ);
        if (options.getCentroids() != null && (options.getCentroids() < 1 || options.getCentroids() > 256)) {
            throw new IllegalArgumentException("`quantization.centroids` must be between 1 and 256, got " + options.getCentroids() + ".");
        }
        if (options.getSegments() != null && options.getSegments() < 0) {
            throw new IllegalArgumentException("`quantization.segments` must be 0 or more, got " + options.getSegments() + ".");
        }
        if (options.getBits() != null && options.getBits() != 1 && options.getBits() != 8) {
            throw new IllegalArgumentException("`quantization.bits` must be 8 or 1, got " + options.getBits() + ".");
        }
        return switch (type) {
            case NONE -> Quantization.uncompressed();
            case RQ -> options.getBits() == null ? Quantization.rq() : Quantization.rq(q -> q.bits(options.getBits()));
            case BQ -> Quantization.bq();
            case SQ -> Quantization.sq();
            case PQ -> Quantization.pq(q ->
            {
                if (options.getSegments() != null) {
                    q.segments(options.getSegments());
                }
                if (options.getCentroids() != null) {
                    q.centroids(options.getCentroids());
                }
                return q;
            });
        };
    }

    // Other quantizer settings are not exposed: client6 6.3.2 sends them in snake_case, which Weaviate ignores.
    private static void allowed(QuantizationType type, String option, Object value, QuantizationType... types) {
        if (value != null && !List.of(types).contains(type)) {
            throw new IllegalArgumentException("`quantization." + option + "` is only supported with " + List.of(types) + ", not " + type + ".");
        }
    }

    @lombok.Value
    @lombok.Builder
    @lombok.extern.jackson.Jacksonized
    @Schema(title = "Inverted index settings")
    public static class InvertedIndexOptions {
        @Schema(title = "Index creation and update times", description = "Needed to filter on `_creationTimeUnix` and `_lastUpdateTimeUnix`.")
        Boolean indexTimestamps;

        @Schema(title = "Index whether properties are null", description = "Needed for the IS_NULL filter operator.")
        Boolean indexNullState;

        @Schema(title = "Index the length of text and array properties", description = "Needed to filter on `len(property)`.")
        Boolean indexPropertyLength;

        @Schema(title = "BM25 ranking parameters")
        Bm25Options bm25;

        @Schema(title = "Stopwords ignored by keyword search")
        StopwordsOptions stopwords;

        @Schema(title = "Seconds between cleanups of deleted objects in the inverted index")
        Integer cleanupIntervalSeconds;
    }

    @lombok.Value
    @lombok.Builder
    @lombok.extern.jackson.Jacksonized
    @Schema(title = "BM25 ranking parameters")
    public static class Bm25Options {
        @Schema(title = "Length normalization, 0 to 1", description = "Weaviate's default is 0.75.")
        Double b;

        @Schema(title = "Term frequency saturation", description = "Weaviate's default is 1.2.")
        Double k1;
    }

    @lombok.Value
    @lombok.Builder
    @lombok.extern.jackson.Jacksonized
    @Schema(title = "Stopwords")
    public static class StopwordsOptions {
        @Schema(title = "Preset", description = "`en` (default) or `none`.")
        String preset;

        @Schema(title = "Words to add to the preset")
        List<String> additions;

        @Schema(title = "Words to remove from the preset")
        List<String> removals;
    }

    @lombok.Value
    @lombok.Builder
    @lombok.extern.jackson.Jacksonized
    @Schema(title = "Vector quantization", description = "See https://docs.weaviate.io/weaviate/concepts/vector-quantization.")
    public static class QuantizationOptions {
        @Schema(title = "Quantization method", description = "RQ, BQ, SQ, PQ, or NONE for uncompressed vectors.")
        QuantizationType type;

        @Schema(title = "Bits per dimension (RQ)", description = "8 (default, about 4x smaller) or 1 (about 32x smaller).")
        Integer bits;

        @Schema(title = "Number of segments (PQ)", description = "Must divide the vector dimensions. 0 lets Weaviate choose.")
        Integer segments;

        @Schema(title = "Centroids per segment (PQ)", description = "At most 256.")
        Integer centroids;
    }

    @lombok.Value
    @lombok.Builder
    @lombok.extern.jackson.Jacksonized
    @Schema(title = "Multi-tenancy settings")
    public static class MultiTenancyOptions {
        @Schema(title = "Enable multi-tenancy", description = "Defaults to true when `multiTenancy` is set.")
        Boolean enabled;

        @Schema(title = "Create unknown tenants on insert", description = "Lets `BatchCreate` write to a tenant that does not exist yet.")
        Boolean autoTenantCreation;

        @Schema(title = "Activate inactive tenants on access")
        Boolean autoTenantActivation;

        @Schema(title = "Tenants to create", description = "Created as active.")
        List<String> tenants;
    }

    public enum QuantizationType {
        NONE,
        RQ,
        BQ,
        SQ,
        PQ
    }

    public enum Vectorizer {
        NONE,
        TEXT2VEC_COHERE,
        TEXT2VEC_MODEL2VEC,
        TEXT2VEC_TRANSFORMERS,
        TEXT2VEC_OPENAI,
        TEXT2VEC_WEAVIATE
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(title = "Whether the task succeeded")
        private Boolean success;

        @Schema(title = "Whether the collection was created", description = "False when it already existed and `skipIfExists` is true.")
        private Boolean created;

        @Schema(title = "Tenants created by this run")
        private List<String> tenantsCreated;
    }
}
