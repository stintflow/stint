package io.stintflow.spi;

import java.net.URI;
import java.time.Duration;
import java.util.Locale;
import java.util.Optional;

/**
 * Transport-agnostic representation of a task failure, aligned with the DSL 1.0 error shape (SDD 1.3, RF7): a URI
 * {@code type}, a {@code status} (e.g. HTTP-style 429), a short {@code title}, a longer {@code detail}, an
 * {@code instance} (JSON Pointer of the component the error comes from) and an optional {@code retryAfter} the
 * caller should treat as a minimum delay (RNF2).
 * <p>
 * SDD 2.8: DSL 1.0 {@code dsl-reference.md}, Error — {@code type} "strongly recommended to use Standard Error Types",
 * {@code status} "Runtimes MUST ensure that the property has been set", {@code instance} "Runtimes MUST set the
 * property when raising or escalating the error". The errors Stint raises use the {@link StandardType standard
 * types}, with their status and the raising component's pointer ({@link #raisedAt}). The {@code stint://errors/*}
 * types of earlier versions are accepted as aliases of the standard ones ({@link #normalize}); this compatibility
 * is temporary and will be removed in a later version.
 */
public record ErrorInfo(URI type, Integer status, String title, String detail, String instance, Duration retryAfter) {

    /** DSL 1.0 Standard Error Types: {@code https://serverlessworkflow.io/spec/1.0.0/errors/<name>}. */
    public static final String STANDARD_TYPE_PREFIX = "https://serverlessworkflow.io/spec/1.0.0/errors/";
    /** The types Stint used before SDD 2.8 — aliases of the standard ones. */
    public static final String LEGACY_TYPE_PREFIX = "stint://errors/";
    /** Status of an error whose type has no standard default. */
    public static final int DEFAULT_STATUS = 500;

    /** DSL 1.0 Standard Error Types, with their default status. */
    public enum StandardType {
        CONFIGURATION(400), VALIDATION(400), EXPRESSION(400), AUTHENTICATION(401), AUTHORIZATION(403),
        TIMEOUT(408), COMMUNICATION(500), RUNTIME(500);

        private final int status;

        StandardType(int status) {
            this.status = status;
        }

        public int status() {
            return status;
        }

        public URI uri() {
            return URI.create(STANDARD_TYPE_PREFIX + name().toLowerCase(Locale.ROOT));
        }
    }

    public static final URI TYPE_TIMEOUT = StandardType.TIMEOUT.uri();
    public static final URI TYPE_COMMUNICATION = StandardType.COMMUNICATION.uri();
    public static final URI TYPE_RUNTIME = StandardType.RUNTIME.uri();
    public static final URI TYPE_VALIDATION = StandardType.VALIDATION.uri();

    public static ErrorInfo of(Throwable t) {
        return new ErrorInfo(TYPE_RUNTIME, StandardType.RUNTIME.status(), t.getClass().getSimpleName(),
                String.valueOf(t.getMessage()), null, null);
    }

    public static ErrorInfo timeout(String detail) {
        return new ErrorInfo(TYPE_TIMEOUT, StandardType.TIMEOUT.status(), "Timeout", detail, null, null);
    }

    /** The standard type {@code type} is, or is an alias of. */
    public static Optional<StandardType> standardType(URI type) {
        if (type == null) {
            return Optional.empty();
        }
        String text = type.toString();
        for (String prefix : new String[] {STANDARD_TYPE_PREFIX, LEGACY_TYPE_PREFIX}) {
            if (text.startsWith(prefix)) {
                String name = text.substring(prefix.length()).toUpperCase(Locale.ROOT);
                for (StandardType standard : StandardType.values()) {
                    if (standard.name().equals(name)) {
                        return Optional.of(standard);
                    }
                }
            }
        }
        return Optional.empty();
    }

    /** {@code type}, with a legacy {@code stint://errors/*} alias replaced by its standard type. */
    public static URI normalize(URI type) {
        return standardType(type).map(StandardType::uri).orElse(type);
    }

    /** The default status of {@code type}: the standard one, or {@value #DEFAULT_STATUS}. */
    public static int defaultStatus(URI type) {
        return standardType(type).map(StandardType::status).orElse(DEFAULT_STATUS);
    }

    /**
     * This error as the runtime raises it at {@code pointer}: the type normalized, the status defaulted when
     * absent, and the instance set to {@code pointer} unless the error already names a (more specific) one.
     */
    public ErrorInfo raisedAt(String pointer) {
        URI normalized = type == null ? TYPE_RUNTIME : normalize(type);
        return new ErrorInfo(normalized, status != null ? status : defaultStatus(normalized), title, detail,
                instance != null ? instance : pointer, retryAfter);
    }
}
