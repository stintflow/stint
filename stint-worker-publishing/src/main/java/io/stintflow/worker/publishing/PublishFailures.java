package io.stintflow.worker.publishing;

import java.time.Duration;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.worker.TaskFailure;

/**
 * The standard errors of a publication (SDD 3a.6, 4.5; DSL 1.0 types, SDD 2.8). Details are fixed sentences, HTTP
 * statuses and names — never a server's message nor any part of a page.
 */
public final class PublishFailures {

    private PublishFailures() {
    }

    /** A space other than the allowed one: refused before any call. */
    public static TaskFailure spaceNotAllowed(String space, String allowed) {
        return new TaskFailure(StandardType.AUTHORIZATION.uri(), 403, "Publishing to space '" + space
                + "' is not allowed: only '" + allowed + "' is", null);
    }

    public static TaskFailure validation(String detail) {
        return new TaskFailure(StandardType.VALIDATION.uri(), 400, detail, null);
    }

    public static TaskFailure unavailable(int status, String detail, Duration retryAfter) {
        return new TaskFailure(StandardType.COMMUNICATION.uri(), status, detail, retryAfter);
    }

    public static TaskFailure timeout(String detail) {
        return new TaskFailure(StandardType.TIMEOUT.uri(), 408, detail, null);
    }

    /**
     * An HTTP status from the server: 401 {@code authentication}, 403 {@code authorization}, 404 {@code configuration}
     * (the space or the API is not there), 400/409/413/422 {@code validation}, 429 and 5xx {@code communication}.
     */
    public static TaskFailure http(int status, Duration retryAfter, String what) {
        String detail = "The publishing server answered HTTP " + status + " to " + what;
        if (status == 429 || status >= 500) {
            return unavailable(status, detail, retryAfter);
        }
        if (status == 401) {
            return new TaskFailure(StandardType.AUTHENTICATION.uri(), 401, detail, null);
        }
        if (status == 403) {
            return new TaskFailure(StandardType.AUTHORIZATION.uri(), 403, detail, null);
        }
        if (status == 400 || status == 409 || status == 413 || status == 422) {
            return validation(detail);
        }
        return new TaskFailure(StandardType.CONFIGURATION.uri(), status, detail, null);
    }
}
