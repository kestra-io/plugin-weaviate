package io.kestra.plugin.weaviate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.weaviate.client6.v1.api.collections.query.Filter;
import io.weaviate.client6.v1.api.collections.query.FilterOperand;

/**
 * Translates the task-level `filter` map and `filters` list into a Weaviate v6 {@link Filter}.
 */
final class WeaviateFilters {
    static final String ID_PROPERTY = "_id";
    static final String CREATION_TIME_PROPERTY = "_creationTimeUnix";
    static final String LAST_UPDATE_TIME_PROPERTY = "_lastUpdateTimeUnix";

    private WeaviateFilters() {
    }

    /**
     * Builds the combined filter, or returns null when neither form yields a condition.
     *
     * @param simple legacy map: entries are ANDed; strings use LIKE (so `*` wildcards work), other values use EQUAL
     * @param conditions structured conditions, combined with {@code operator}
     * @param operator how to combine {@code conditions}; defaults to AND
     */
    static Filter build(Map<String, Object> simple, List<FilterCondition> conditions, FilterCondition.LogicalOperator operator) {
        List<Filter> parts = new ArrayList<>();

        if (simple != null && !simple.isEmpty()) {
            List<Filter> simpleFilters = simple.entrySet().stream()
                .map(e -> simpleCondition(e.getKey(), e.getValue()))
                .toList();
            parts.add(simpleFilters.size() == 1 ? simpleFilters.getFirst() : Filter.and(toOperands(simpleFilters)));
        }

        if (conditions != null && !conditions.isEmpty()) {
            List<Filter> structured = new ArrayList<>();
            for (int i = 0; i < conditions.size(); i++) {
                try {
                    structured.add(condition(conditions.get(i)));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Invalid `filters[" + i + "]`: " + e.getMessage(), e);
                }
            }
            if (structured.size() == 1) {
                parts.add(structured.getFirst());
            } else {
                parts.add(operator == FilterCondition.LogicalOperator.OR ? Filter.or(toOperands(structured)) : Filter.and(toOperands(structured)));
            }
        }

        if (parts.isEmpty()) {
            return null;
        }
        return parts.size() == 1 ? parts.getFirst() : Filter.and(toOperands(parts));
    }

    private static List<FilterOperand> toOperands(List<Filter> filters) {
        return new ArrayList<>(filters);
    }

    private static Filter simpleCondition(String property, Object value) {
        if (value == null) {
            throw new IllegalArgumentException("Invalid `filter` entry '" + property + "': value is null.");
        }
        try {
            Object normalized = normalize(value, null);
            FilterCondition.Operator operator = normalized instanceof String ? FilterCondition.Operator.LIKE : FilterCondition.Operator.EQUAL;
            return condition(property, operator, normalized);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid `filter` entry '" + property + "': " + e.getMessage(), e);
        }
    }

    static Filter condition(FilterCondition condition) {
        if (condition.getProperty() == null || condition.getProperty().isBlank()) {
            throw new IllegalArgumentException("`property` is required.");
        }
        if (condition.getOperator() == null) {
            throw new IllegalArgumentException("`operator` is required.");
        }
        Object value = condition.getOperator() == FilterCondition.Operator.IS_NULL
            ? condition.getValue()
            : normalize(condition.getValue(), condition.getValueType());
        if (value == null && condition.getOperator() != FilterCondition.Operator.IS_NULL) {
            throw new IllegalArgumentException("`value` is required for operator " + condition.getOperator() + ".");
        }
        return condition(condition.getProperty(), condition.getOperator(), value);
    }

    private static Filter condition(String property, FilterCondition.Operator operator, Object value) {
        return switch (property) {
            case ID_PROPERTY -> idCondition(operator, value);
            case CREATION_TIME_PROPERTY, LAST_UPDATE_TIME_PROPERTY -> timestampCondition(property, operator, value);
            default -> propertyCondition(Filter.property(property), property, operator, wire(value));
        };
    }

    private static Filter propertyCondition(Filter.FilterBuilder builder, String property, FilterCondition.Operator operator, Object value) {
        return switch (operator) {
            case EQUAL -> builder.eq(value);
            case NOT_EQUAL -> builder.ne(value);
            case GREATER_THAN -> builder.gt(comparable(property, operator, value));
            case GREATER_THAN_EQUAL -> builder.gte(comparable(property, operator, value));
            case LESS_THAN -> builder.lt(comparable(property, operator, value));
            case LESS_THAN_EQUAL -> builder.lte(comparable(property, operator, value));
            case LIKE -> {
                if (!(value instanceof String str)) {
                    throw new IllegalArgumentException("LIKE on '" + property + "' needs a text value, got " + describe(value) + ".");
                }
                yield builder.like(str);
            }
            case CONTAINS_ANY, CONTAINS_ALL, CONTAINS_NONE -> contains(builder, property, operator, asList(value));
            case IS_NULL -> builder.isNull(isNullValue(property, value));
        };
    }

    private static Object comparable(String property, FilterCondition.Operator operator, Object value) {
        if (value instanceof Boolean || value instanceof List) {
            throw new IllegalArgumentException(operator + " on '" + property + "' needs a number, text or date value, got " + describe(value) + ".");
        }
        return value;
    }

    private static Filter contains(Filter.FilterBuilder builder, String property, FilterCondition.Operator operator, List<?> values) {
        Object first = values.getFirst();
        if (first instanceof String) {
            List<String> strings = castAll(values, String.class, property);
            return switch (operator) {
                case CONTAINS_ANY -> builder.containsAny(strings);
                case CONTAINS_ALL -> builder.containsAll(strings);
                default -> builder.containsNone(strings);
            };
        }
        if (first instanceof Boolean) {
            Boolean[] array = castAll(values, Boolean.class, property).toArray(Boolean[]::new);
            return switch (operator) {
                case CONTAINS_ANY -> builder.containsAny(array);
                case CONTAINS_ALL -> builder.containsAll(array);
                default -> builder.containsNone(array);
            };
        }
        if (first instanceof Long) {
            Long[] array = castAll(values, Long.class, property).toArray(Long[]::new);
            return switch (operator) {
                case CONTAINS_ANY -> builder.containsAny(array);
                case CONTAINS_ALL -> builder.containsAll(array);
                default -> builder.containsNone(array);
            };
        }
        if (first instanceof Double) {
            Double[] array = castAll(values, Double.class, property).toArray(Double[]::new);
            return switch (operator) {
                case CONTAINS_ANY -> builder.containsAny(array);
                case CONTAINS_ALL -> builder.containsAll(array);
                default -> builder.containsNone(array);
            };
        }
        OffsetDateTime[] array = castAll(values, OffsetDateTime.class, property).toArray(OffsetDateTime[]::new);
        return switch (operator) {
            case CONTAINS_ANY -> builder.containsAny(array);
            case CONTAINS_ALL -> builder.containsAll(array);
            default -> builder.containsNone(array);
        };
    }

    private static Filter idCondition(FilterCondition.Operator operator, Object value) {
        var id = Filter.uuid();
        if (operator == FilterCondition.Operator.CONTAINS_ANY || operator == FilterCondition.Operator.CONTAINS_NONE) {
            String[] ids = castAll(asList(value), String.class, ID_PROPERTY).toArray(String[]::new);
            return operator == FilterCondition.Operator.CONTAINS_ANY ? id.containsAny(ids) : id.containsNone(ids);
        }
        if (!(value instanceof String str)) {
            throw new IllegalArgumentException("'" + ID_PROPERTY + "' needs a UUID string value, got " + describe(value) + ".");
        }
        return switch (operator) {
            case EQUAL -> id.eq(str);
            case NOT_EQUAL -> id.ne(str);
            case GREATER_THAN -> id.gt(str);
            case GREATER_THAN_EQUAL -> id.gte(str);
            case LESS_THAN -> id.lt(str);
            case LESS_THAN_EQUAL -> id.lte(str);
            default -> throw new IllegalArgumentException("Operator " + operator + " is not supported on '" + ID_PROPERTY + "'.");
        };
    }

    /**
     * Object timestamps only support comparisons; values are converted to dates first so non-date input fails early.
     */
    private static Filter timestampCondition(String property, FilterCondition.Operator operator, Object value) {
        return switch (operator) {
            case EQUAL, NOT_EQUAL, GREATER_THAN, GREATER_THAN_EQUAL, LESS_THAN, LESS_THAN_EQUAL ->
                propertyCondition(Filter.property(property), property, operator, wire(toDate(value, property)));
            case CONTAINS_ANY, CONTAINS_NONE -> propertyCondition(
                Filter.property(property), property, operator, wire(asList(value).stream().map(v -> toDate(v, property)).toList())
            );
            default -> throw new IllegalArgumentException("Operator " + operator + " is not supported on '" + property + "'.");
        };
    }

    /**
     * Formats dates as RFC3339 text before they reach the client. The v6 client serializes OffsetDateTime with
     * toString(), which drops zero seconds (2024-01-01T00:00Z) and is rejected by the server. Date filters are
     * sent as text values either way, so this changes only the formatting.
     */
    private static Object wire(Object value) {
        if (value instanceof OffsetDateTime date) {
            return date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        if (value instanceof List<?> list && !list.isEmpty() && list.getFirst() instanceof OffsetDateTime) {
            return list.stream().map(WeaviateFilters::wire).toList();
        }
        return value;
    }

    private static boolean isNullValue(String property, Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String str && (str.equalsIgnoreCase("true") || str.equalsIgnoreCase("false"))) {
            return Boolean.parseBoolean(str);
        }
        throw new IllegalArgumentException("IS_NULL on '" + property + "' needs a boolean value, got " + describe(value) + ".");
    }

    /**
     * Converts a rendered YAML value into one of the types the v6 filter builder accepts:
     * String, Boolean, Long, Double, OffsetDateTime, or a homogeneous List of those.
     */
    static Object normalize(Object value, FilterCondition.ValueType type) {
        if (value == null) {
            return null;
        }
        if (value instanceof List<?> list) {
            if (list.isEmpty()) {
                throw new IllegalArgumentException("list values must not be empty.");
            }
            List<Object> normalized = list.stream().map(v -> normalize(v, type)).toList();
            Class<?> first = normalized.getFirst().getClass();
            if (normalized.stream().anyMatch(v -> v.getClass() != first)) {
                throw new IllegalArgumentException("list values must all have the same type, got " + describe(value) + ".");
            }
            return normalized;
        }
        if (type == null) {
            return infer(value);
        }
        return switch (type) {
            case TEXT -> value.toString();
            case UUID -> {
                try {
                    yield UUID.fromString(value.toString()).toString();
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("'" + value + "' is not a valid UUID.");
                }
            }
            case BOOLEAN -> {
                if (value instanceof Boolean bool) {
                    yield bool;
                }
                String str = value.toString();
                if (!str.equalsIgnoreCase("true") && !str.equalsIgnoreCase("false")) {
                    throw new IllegalArgumentException("'" + value + "' is not a boolean.");
                }
                yield Boolean.parseBoolean(str);
            }
            case INT -> {
                try {
                    yield new BigDecimal(value.toString()).longValueExact();
                } catch (ArithmeticException | NumberFormatException e) {
                    throw new IllegalArgumentException("'" + value + "' is not an integer.");
                }
            }
            case NUMBER -> {
                try {
                    yield Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("'" + value + "' is not a number.");
                }
            }
            case DATE -> toDate(value, null);
        };
    }

    private static Object infer(Object value) {
        if (value instanceof String || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte || value instanceof BigInteger) {
            return ((Number) value).longValue();
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        if (
            value instanceof Date || value instanceof Instant || value instanceof ZonedDateTime || value instanceof OffsetDateTime
                || value instanceof LocalDateTime || value instanceof LocalDate
        ) {
            return toDate(value, null);
        }
        return value.toString();
    }

    private static OffsetDateTime toDate(Object value, String property) {
        String context = property == null ? "" : " for '" + property + "'";
        return switch (value) {
            case OffsetDateTime odt -> odt;
            case ZonedDateTime zdt -> zdt.toOffsetDateTime();
            case Instant instant -> instant.atOffset(ZoneOffset.UTC);
            case Date date -> date.toInstant().atOffset(ZoneOffset.UTC);
            case LocalDateTime ldt -> ldt.atOffset(ZoneOffset.UTC);
            case LocalDate ld -> ld.atStartOfDay().atOffset(ZoneOffset.UTC);
            case String str -> {
                try {
                    yield OffsetDateTime.parse(str);
                } catch (DateTimeException e) {
                    try {
                        yield LocalDate.parse(str).atStartOfDay().atOffset(ZoneOffset.UTC);
                    } catch (DateTimeException ignored) {
                        throw new IllegalArgumentException("'" + str + "' is not an ISO-8601 date" + context + ".");
                    }
                }
            }
            default -> throw new IllegalArgumentException(describe(value) + " is not a date" + context + ".");
        };
    }

    private static List<?> asList(Object value) {
        if (value instanceof List<?> list) {
            return list;
        }
        if (value == null) {
            throw new IllegalArgumentException("a list value is required.");
        }
        return List.of(value);
    }

    private static <T> List<T> castAll(List<?> values, Class<T> type, String property) {
        List<T> result = new ArrayList<>(values.size());
        for (Object v : values) {
            if (!type.isInstance(v)) {
                throw new IllegalArgumentException("values for '" + property + "' must all be " + type.getSimpleName() + ", got " + describe(v) + ".");
            }
            result.add(type.cast(v));
        }
        return result;
    }

    private static String describe(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + " '" + value + "'";
    }
}
