package io.kestra.plugin.weaviate;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;

final class WeaviateDates {
    private WeaviateDates() {
    }

    static Optional<OffsetDateTime> toOffsetDateTime(Object value) {
        return Optional.ofNullable(
            switch (value) {
                case OffsetDateTime odt -> odt;
                case ZonedDateTime zdt -> zdt.toOffsetDateTime();
                case Instant instant -> instant.atOffset(ZoneOffset.UTC);
                case Date date -> date.toInstant().atOffset(ZoneOffset.UTC);
                case LocalDateTime ldt -> ldt.atOffset(ZoneOffset.UTC);
                case LocalDate ld -> ld.atStartOfDay().atOffset(ZoneOffset.UTC);
                case null, default -> null;
            }
        );
    }

    // The v6 client sends OffsetDateTime.toString(), which drops zero seconds (2024-01-01T00:00Z) and the server rejects it.
    static String rfc3339(OffsetDateTime date) {
        return date.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
