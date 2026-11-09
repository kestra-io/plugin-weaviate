package io.kestra.plugin.weaviate;

import java.util.List;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

@Value
@Builder
@Jacksonized
@Schema(
    title = "Filter condition or group",
    description = """
        Either a comparison (`property`, `operator`, `value`) or a group: `and` or `or` with a list of conditions, or `not`
        with one condition. Groups nest. In `property`, use `_id` for the object UUID, `_creationTimeUnix` or
        `_lastUpdateTimeUnix` for object timestamps (need `invertedIndex.indexTimestamps`), and `len(name)` for the length
        of a text or array property (needs `invertedIndex.indexPropertyLength`)."""
)
public class FilterCondition {
    @Schema(title = "Property name", description = "Also `_id`, `_creationTimeUnix`, `_lastUpdateTimeUnix` or `len(name)`.")
    String property;

    @Schema(title = "Comparison operator")
    Operator operator;

    @Schema(
        title = "Value to compare against",
        description = """
            A scalar, or a list for the CONTAINS_* operators. For IS_NULL, `true` (default) matches missing values and `false`
            matches present ones (needs `invertedIndex.indexNullState`). For WITHIN_GEO_RANGE, a map with `latitude`, `longitude`
            and `distance` in meters."""
    )
    Object value;

    @Schema(
        title = "Force the value type",
        description = """
            Weaviate filters are typed. Without this, the type is inferred from the YAML value, so set it for dates (`DATE`),
            UUIDs (`UUID`), or numbers stored as `number` but written as integers (`NUMBER`)."""
    )
    ValueType valueType;

    @Schema(title = "Conditions that must all match")
    List<FilterCondition> and;

    @Schema(title = "Conditions of which at least one must match")
    List<FilterCondition> or;

    @Schema(title = "Condition that must not match")
    FilterCondition not;

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
        IS_NULL,
        WITHIN_GEO_RANGE
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
