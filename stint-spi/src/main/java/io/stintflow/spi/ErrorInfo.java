package io.stintflow.spi;

import java.net.URI;
import java.time.Duration;

/**
 * Transport-agnostic representation of a task failure, aligned with the DSL 1.0 error shape
 * (SDD 1.3, RF7): a URI {@code type}, an optional {@code status} (e.g. HTTP-style 429), a short
 * {@code title}, a longer {@code detail}, an optional {@code instance} identifier, and an optional
 * {@code retryAfter} the caller should treat as a minimum delay (RNF2).
 */
public record ErrorInfo(URI type, Integer status, String title, String detail, String instance, Duration retryAfter) {

    public static final URI TYPE_TIMEOUT = URI.create("stint://errors/timeout");
    public static final URI TYPE_COMMUNICATION = URI.create("stint://errors/communication");
    public static final URI TYPE_RUNTIME = URI.create("stint://errors/runtime");
    public static final URI TYPE_VALIDATION = URI.create("stint://errors/validation");

    public static ErrorInfo of(Throwable t) {
        return new ErrorInfo(TYPE_RUNTIME, null, t.getClass().getSimpleName(), String.valueOf(t.getMessage()), null, null);
    }

    public static ErrorInfo timeout(String detail) {
        return new ErrorInfo(TYPE_TIMEOUT, null, "Timeout", detail, null, null);
    }
}
