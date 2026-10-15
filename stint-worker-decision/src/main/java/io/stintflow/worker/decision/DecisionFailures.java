package io.stintflow.worker.decision;

import java.time.Duration;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.worker.TaskFailure;

/**
 * The standard errors of a decision (SDD 3a.2, 4.5; DSL 1.0 types, SDD 2.8). Details are fixed sentences plus field
 * names and HTTP statuses — never a provider's message nor any part of an answer, which may quote the input.
 */
public final class DecisionFailures {

    public static final int UNAVAILABLE = 503;

    private DecisionFailures() {
    }

    /** The answer can't be read, or doesn't fit the request: a {@code validation} 400 a {@code catch} can handle. */
    public static TaskFailure validation(String detail) {
        return new TaskFailure(StandardType.VALIDATION.uri(), 400, detail, null);
    }

    /** The decision can't be made as configured: not retryable. */
    public static TaskFailure configuration(int status, String detail) {
        return new TaskFailure(StandardType.CONFIGURATION.uri(), status, detail, null);
    }

    /** The policy refused the adapter: not retryable. */
    public static TaskFailure refused(String reason) {
        return new TaskFailure(StandardType.AUTHORIZATION.uri(), 403, reason, null);
    }

    public static TaskFailure timeout(String detail) {
        return new TaskFailure(StandardType.TIMEOUT.uri(), 408, detail, null);
    }

    public static TaskFailure unavailable(int status, String detail) {
        return new TaskFailure(StandardType.COMMUNICATION.uri(), status, detail, null);
    }

    /**
     * An HTTP provider's error status, as the table of SDD 3a.3 (5.5) maps it: 429 is {@code communication} with
     * {@code retryAfter}; 401 {@code authentication}; 403 {@code authorization}; any other 4xx {@code configuration};
     * 5xx (and non-standard ones, like 529) {@code communication}.
     */
    public static TaskFailure http(int status, Duration retryAfter, String provider) {
        String detail = "The decision provider " + provider + " answered HTTP " + status;
        if (status == 429) {
            return new TaskFailure(StandardType.COMMUNICATION.uri(), 429, detail + " (rate limited)", retryAfter);
        }
        if (status == 401) {
            return new TaskFailure(StandardType.AUTHENTICATION.uri(), 401, detail + " (credentials rejected)", null);
        }
        if (status == 403) {
            return new TaskFailure(StandardType.AUTHORIZATION.uri(), 403, detail + " (call denied)", null);
        }
        if (status == 408) {
            return timeout(detail);
        }
        if (status >= 400 && status < 500) {
            return configuration(status, detail + " (request rejected)");
        }
        return unavailable(status, detail + " (unavailable)");
    }
}
