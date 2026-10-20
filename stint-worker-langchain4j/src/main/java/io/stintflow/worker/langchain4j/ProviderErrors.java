package io.stintflow.worker.langchain4j;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

import dev.langchain4j.exception.AuthenticationException;
import dev.langchain4j.exception.ContentFilteredException;
import dev.langchain4j.exception.HttpException;
import dev.langchain4j.exception.InternalServerException;
import dev.langchain4j.exception.InvalidRequestException;
import dev.langchain4j.exception.ModelNotFoundException;
import dev.langchain4j.exception.RateLimitException;
import dev.langchain4j.exception.RetriableException;
import dev.langchain4j.exception.TimeoutException;
import dev.langchain4j.service.output.OutputParsingException;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.worker.TaskFailure;

/**
 * Maps what a model call throws to a standard error (SDD 3a.3, 5.5; DSL 1.0 types, SDD 2.8), so the workflow's
 * {@code catch} decides by {@code type} and {@code status}. The {@code detail} is a fixed sentence, the exception's
 * class and the HTTP status — never the exception's message, which a provider may fill with part of the prompt.
 */
final class ProviderErrors {

    static final int CONTENT_FILTERED_STATUS = 422;
    static final int UNAVAILABLE_STATUS = 503;

    private ProviderErrors() {
    }

    static TaskFailure toTaskFailure(Throwable thrown, Function<Throwable, Optional<Duration>> retryAfter,
            Duration defaultRateLimitRetryAfter) {
        for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
            if (t instanceof TaskFailure failure) {
                return failure;
            }
            Integer http = t instanceof HttpException h ? h.statusCode() : null;
            if (t instanceof OutputParsingException) {
                return failure(StandardType.VALIDATION, 400, "The model's answer does not fit the output type", t, null);
            }
            if (t instanceof RateLimitException || (http != null && http == 429)) {
                Duration after = retryAfter.apply(thrown).orElse(defaultRateLimitRetryAfter);
                return failure(StandardType.COMMUNICATION, 429, "The model provider rate-limited the call", t, after);
            }
            if (t instanceof TimeoutException || t instanceof HttpTimeoutException || t instanceof SocketTimeoutException
                    || t instanceof java.util.concurrent.TimeoutException || (http != null && http == 408)) {
                return failure(StandardType.TIMEOUT, 408, "The model call timed out", t, null);
            }
            if (t instanceof AuthenticationException || (http != null && http == 401)) {
                return failure(StandardType.AUTHENTICATION, 401, "The model provider rejected the credentials", t, null);
            }
            if (http != null && http == 403) {
                return failure(StandardType.AUTHORIZATION, 403, "The model provider denied the call", t, null);
            }
            if (t instanceof ModelNotFoundException || (http != null && http == 404)) {
                return failure(StandardType.CONFIGURATION, 404, "The model does not exist at the provider", t, null);
            }
            if (t instanceof ContentFilteredException) {
                return failure(StandardType.RUNTIME, CONTENT_FILTERED_STATUS, "The model provider filtered the content",
                        t, null);
            }
            if (t instanceof InvalidRequestException || (http != null && http >= 400 && http < 500)) {
                return failure(StandardType.CONFIGURATION, http != null ? http : 400,
                        "The model provider rejected the request", t, null);
            }
            if (t instanceof InternalServerException || t instanceof ConnectException
                    || (http != null && http >= 500)) {
                return failure(StandardType.COMMUNICATION, http != null ? http : UNAVAILABLE_STATUS,
                        "The model provider is unavailable", t, null);
            }
            if (t instanceof RetriableException) {
                return failure(StandardType.COMMUNICATION, UNAVAILABLE_STATUS, "The model call failed transiently", t,
                        null);
            }
        }
        return failure(StandardType.RUNTIME, StandardType.RUNTIME.status(), "The LLM task failed", thrown, null);
    }

    /** True for the catch-all of {@link #toTaskFailure}: a failure no rule above recognized. */
    static boolean unrecognized(TaskFailure failure) {
        return failure.getMessage() != null && failure.getMessage().startsWith("The LLM task failed");
    }

    /**
     * A failure's shape for a log line, without content: the classes of its cause chain and where the innermost one
     * was thrown — never a message.
     */
    static String describe(Throwable thrown) {
        StringBuilder chain = new StringBuilder();
        Throwable root = thrown;
        for (Throwable t = thrown; t != null; t = t.getCause() == t ? null : t.getCause()) {
            chain.append(chain.isEmpty() ? "" : " <- ").append(t.getClass().getName());
            root = t;
        }
        StackTraceElement[] frames = root.getStackTrace();
        return frames.length == 0 ? chain.toString() : chain + " at " + frames[0];
    }

    static TaskFailure validation(String detail) {
        return new TaskFailure(StandardType.VALIDATION.uri(), 400, detail, null);
    }

    private static TaskFailure failure(StandardType type, int status, String sentence, Throwable cause,
            Duration retryAfter) {
        String detail = sentence + " (" + cause.getClass().getSimpleName()
                + (cause instanceof HttpException h ? ", HTTP " + h.statusCode() : "") + ")";
        URI uri = type.uri();
        return new TaskFailure(uri, status, detail, retryAfter);
    }
}
