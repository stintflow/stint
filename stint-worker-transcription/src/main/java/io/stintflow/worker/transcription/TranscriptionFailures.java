package io.stintflow.worker.transcription;

import java.time.Duration;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.worker.TaskFailure;

/**
 * The standard errors of a transcription (SDD 3a.6, 4.2; DSL 1.0 types, SDD 2.8). Details are fixed sentences, HTTP
 * statuses and field names — never a server's message nor any part of a transcript.
 */
public final class TranscriptionFailures {

    private TranscriptionFailures() {
    }

    /** The server is down or overloaded: retryable ({@code communication}). */
    public static TaskFailure unavailable(int status, String detail, Duration retryAfter) {
        return new TaskFailure(StandardType.COMMUNICATION.uri(), status, detail, retryAfter);
    }

    public static TaskFailure timeout(String detail) {
        return new TaskFailure(StandardType.TIMEOUT.uri(), 408, detail, null);
    }

    /** The audio or the request can't be transcribed as sent: not retryable. */
    public static TaskFailure validation(String detail) {
        return new TaskFailure(StandardType.VALIDATION.uri(), 400, detail, null);
    }

    public static TaskFailure configuration(int status, String detail) {
        return new TaskFailure(StandardType.CONFIGURATION.uri(), status, detail, null);
    }

    /**
     * An HTTP status from the server: 429 and 5xx are {@code communication} (retryable), 401 {@code authentication},
     * 403 {@code authorization}, 400/413/415/422 {@code validation}, any other 4xx {@code configuration}.
     */
    public static TaskFailure http(int status, Duration retryAfter, String engine) {
        String detail = "The transcription server " + engine + " answered HTTP " + status;
        if (status == 429 || status >= 500) {
            return unavailable(status, detail, retryAfter);
        }
        if (status == 401) {
            return new TaskFailure(StandardType.AUTHENTICATION.uri(), 401, detail, null);
        }
        if (status == 403) {
            return new TaskFailure(StandardType.AUTHORIZATION.uri(), 403, detail, null);
        }
        if (status == 400 || status == 413 || status == 415 || status == 422) {
            return validation(detail);
        }
        return configuration(status, detail);
    }
}
