package io.stintflow.worker;

import java.net.URI;
import java.time.Duration;

import io.stintflow.spi.ErrorInfo;

/**
 * Exception a {@link TaskHandler} throws (or fails its returned stage with) to declare a rich,
 * typed failure — e.g. a 429 from an LLM provider with a {@code retryAfter} (SDD 1.3, RF8).
 */
public final class TaskFailure extends RuntimeException {

    private final URI type;
    private final Integer status;
    private final Duration retryAfter;

    public TaskFailure(URI type, Integer status, String detail, Duration retryAfter) {
        super(detail);
        this.type = type;
        this.status = status;
        this.retryAfter = retryAfter;
    }

    public ErrorInfo toErrorInfo() {
        return new ErrorInfo(type, status, type == null ? null : type.toString(), getMessage(), null, retryAfter);
    }
}
