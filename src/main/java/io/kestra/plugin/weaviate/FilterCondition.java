package io.kestra.plugin.weaviate;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@Schema(
    title = "Condition on one object property",
    description = """
        Compares `property` to `value` with `operator`. Use `_id` for the object UUID and `_creationTimeUnix` or
        `_lastUpdateTimeUnix` for object timestamps (these need `indexTimestamps` enabled on the collection)."""
)
public class FilterCondition {
    @Schema(title = "Property name")
    @NotBlank
    String property;

    @Schema(title = "Comparison operator")
    @NotNull
    Operator operator;

    @Schema(
        title = "Value to compare against",
        description = "A scalar, or a list for the CONTAINS_* operators. For IS_NULL, `true` (default) matches missing values and `false` matches present ones."
    )
    Object value;

    @Schema(
        title = "Force the value type",
        description = """
            Weaviate filters are typed. Without this, the type is inferred from the YAML value, so set it for dates (`DATE`),
            UUIDs (`UUID`), or numbers stored as `number` but written as integers (`NUMBER`)."""
    )
    ValueType valueType;

    public enum Operator {
        EQUAL,
        NOT_EQUAL,
        GREATER_THAN,
        GREATER_THAN_EQUAL,
        LESS_THAN,
        LESS_THAN_EQUAL,
        LIKE,
        CONTAINS_ANY,
        CONTAINS_ALL,
        CONTAINS_NONE,
        IS_NULL
    }

    public enum ValueType {
        TEXT,
        INT,
        NUMBER,
        BOOLEAN,
        DATE,
        UUID
    }

    public enum LogicalOperator {
        AND,
        OR
    }
}
