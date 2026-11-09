package io.kestra.plugin.weaviate;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.swagger.v3.oas.annotations.media.Schema;
import io.weaviate.client6.v1.api.WeaviateClient;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.VectorConfig;
import io.weaviate.client6.v1.api.collections.VectorIndex;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@Getter
@EqualsAndHashCode
@NoArgsConstructor
@Schema(
    title = "Create a Weaviate collection",
    description = "Creates a collection (class) with the given properties and vectorizer. Fails if the collection already exists, " +
        "unless `skipIfExists` is true."
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
        )
    }
)
public class SchemaCreate extends WeaviateConnection implements RunnableTask<SchemaCreate.Output> {

    @Schema(title = "Collection (class) name")
    @NotNull
    @PluginProperty(group = "main")
    private io.kestra.core.models.property.Property<String> className;

    @Schema(
        title = "Properties to add to the collection",
        description = "Map of property name to a Weaviate data type: text, int, number, boolean, date, uuid (each also as an array, e.g. text[]), " +
            "blob, geoCoordinates or phoneNumber. A single-element list is also accepted; the legacy `string` type maps to text."
    )
    @PluginProperty(group = "main")
    private io.kestra.core.models.property.Property<Map<String, Object>> fields;

    @Schema(
        title = "Vectorizer",
        description = "NONE (default) means vectors are provided on insert, e.g. with `BatchCreate.vectorKey`. " +
            "Other values vectorize text properties server-side; the module must be enabled on the server. " +
            "Unlike plugin versions built on the v5 client, the server's DEFAULT_VECTORIZER_MODULE is not used, so set this explicitly."
    )
    @Builder.Default
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<Vectorizer> vectorizer = io.kestra.core.models.property.Property.ofValue(Vectorizer.NONE);

    @Schema(title = "Vectorizer model", description = "Model name for TEXT2VEC_OPENAI and TEXT2VEC_WEAVIATE. Defaults to the module default.")
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<String> vectorizerModel;

    @Schema(title = "Vector name", description = "Creates a named vector instead of the default one.")
    @PluginProperty(group = "processing")
    private io.kestra.core.models.property.Property<String> vectorName;

    @Schema(title = "Succeed without changes if the collection exists")
    @Builder.Default
    @PluginProperty(group = "advanced")
    private io.kestra.core.models.property.Property<Boolean> skipIfExists = io.kestra.core.models.property.Property.ofValue(false);

    @Override
    public SchemaCreate.Output run(RunContext runContext) throws Exception {
        String renderedClassName = runContext.render(className).as(String.class).orElseThrow();
        List<Property> properties = buildProperties(runContext.render(fields).asMap(String.class, Object.class), runContext.logger());
        Map.Entry<String, VectorConfig> vectorConfig = buildVectorConfig(
            runContext.render(vectorizer).as(Vectorizer.class).orElse(Vectorizer.NONE),
            runContext.render(vectorizerModel).as(String.class).orElse(null),
            runContext.render(vectorName).as(String.class).orElse(null)
        );
        boolean renderedSkipIfExists = runContext.render(skipIfExists).as(Boolean.class).orElse(false);

        try (WeaviateClient client = connect(runContext)) {
            if (renderedSkipIfExists && client.collections.exists(renderedClassName)) {
                runContext.logger().info("Collection '{}' already exists, skipping", renderedClassName);
                return Output.builder().success(true).created(false).build();
            }

            client.collections.create(renderedClassName, c -> c.properties(properties).vectorConfig(vectorConfig));
        }

        return Output.builder().success(true).created(true).build();
    }

    static List<Property> buildProperties(Map<String, Object> fields, Logger logger) {
        List<Property> properties = new ArrayList<>();
        if (fields == null) {
            return properties;
        }
        fields.forEach((name, rawType) -> properties.add(buildProperty(name, dataType(name, rawType), logger)));
        return properties;
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
        String normalized = type.trim().toLowerCase(Locale.ROOT);
        if (normalized.equals("string") || normalized.equals("string[]")) {
            if (logger != null) {
                logger.warn("Field '{}' uses the deprecated '{}' type; creating it as '{}'", name, type, normalized.replace("string", "text"));
            }
            normalized = normalized.replace("string", "text");
        }
        return switch (normalized) {
            case "text" -> Property.text(name);
            case "text[]" -> Property.textArray(name);
            case "int" -> Property.integer(name);
            case "int[]" -> Property.integerArray(name);
            case "number" -> Property.number(name);
            case "number[]" -> Property.numberArray(name);
            case "boolean" -> Property.bool(name);
            case "boolean[]" -> Property.boolArray(name);
            case "date" -> Property.date(name);
            case "date[]" -> Property.dateArray(name);
            case "uuid" -> Property.uuid(name);
            case "uuid[]" -> Property.uuidArray(name);
            case "blob" -> Property.blob(name);
            case "geocoordinates" -> Property.geoCoordinates(name);
            case "phonenumber" -> Property.phoneNumber(name);
            case "object", "object[]" -> throw new IllegalArgumentException(
                "Field '" + name + "': object properties need nested property definitions, which SchemaCreate does not support."
            );
            default -> throw new IllegalArgumentException(
                "Field '" + name + "' has unsupported data type '" + type + "'. Cross-references (capitalized class names) are not supported."
            );
        };
    }

    static Map.Entry<String, VectorConfig> buildVectorConfig(Vectorizer vectorizer, String model, String name) {
        if (model != null && vectorizer != Vectorizer.TEXT2VEC_OPENAI && vectorizer != Vectorizer.TEXT2VEC_WEAVIATE) {
            throw new IllegalArgumentException("`vectorizerModel` is only supported with TEXT2VEC_OPENAI and TEXT2VEC_WEAVIATE, not " + vectorizer + ".");
        }
        return switch (vectorizer) {
            case NONE -> name != null ? VectorConfig.selfProvided(name) : VectorConfig.selfProvided();
            case TEXT2VEC_MODEL2VEC -> name != null ? VectorConfig.text2vecModel2Vec(name) : VectorConfig.text2vecModel2Vec();
            case TEXT2VEC_TRANSFORMERS -> name != null ? VectorConfig.text2vecTransformers(name) : VectorConfig.text2vecTransformers();
            case TEXT2VEC_OPENAI -> model == null
                ? (name != null ? VectorConfig.text2vecOpenAi(name) : VectorConfig.text2vecOpenAi())
                : VectorConfig.text2vecOpenAi(name != null ? name : VectorIndex.DEFAULT_VECTOR_NAME, v -> v.model(model));
            case TEXT2VEC_WEAVIATE -> model == null
                ? (name != null ? VectorConfig.text2vecWeaviate(name) : VectorConfig.text2vecWeaviate())
                : VectorConfig.text2vecWeaviate(name != null ? name : VectorIndex.DEFAULT_VECTOR_NAME, v -> v.model(model));
        };
    }

    public enum Vectorizer {
        NONE,
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
    }
}
