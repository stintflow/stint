package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.failure;
import static io.stintflow.worker.langchain4j.Fakes.task;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.api.Test;

import dev.langchain4j.exception.AuthenticationException;
import dev.langchain4j.exception.ContentFilteredException;
import dev.langchain4j.exception.HttpException;
import dev.langchain4j.exception.InternalServerException;
import dev.langchain4j.exception.InvalidRequestException;
import dev.langchain4j.exception.ModelNotFoundException;
import dev.langchain4j.exception.RateLimitException;
import dev.langchain4j.exception.TimeoutException;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;

/**
 * SDD 3a.3, CA7 (RF7, table 5.5): what a provider throws becomes a standard error — and the provider's message, which
 * may quote the prompt, never reaches it.
 */
class ProviderErrorsTest {

    /** Synthetic text standing for a prompt a provider quoted in its exception. */
    static final String QUOTED_PROMPT = "QUOTED-PROMPT-7f3a";

    static Stream<Arguments> table() {
        return Stream.of(
                Arguments.of(new RateLimitException(QUOTED_PROMPT), StandardType.COMMUNICATION, 429),
                Arguments.of(new HttpException(429, QUOTED_PROMPT), StandardType.COMMUNICATION, 429),
                Arguments.of(new TimeoutException(QUOTED_PROMPT), StandardType.TIMEOUT, 408),
                Arguments.of(new RuntimeException(new java.net.http.HttpTimeoutException(QUOTED_PROMPT)),
                        StandardType.TIMEOUT, 408),
                Arguments.of(new InternalServerException(QUOTED_PROMPT), StandardType.COMMUNICATION, 503),
                Arguments.of(new HttpException(502, QUOTED_PROMPT), StandardType.COMMUNICATION, 502),
                Arguments.of(new RuntimeException(new ConnectException(QUOTED_PROMPT)), StandardType.COMMUNICATION, 503),
                Arguments.of(new AuthenticationException(QUOTED_PROMPT), StandardType.AUTHENTICATION, 401),
                Arguments.of(new HttpException(403, QUOTED_PROMPT), StandardType.AUTHORIZATION, 403),
                Arguments.of(new ModelNotFoundException(QUOTED_PROMPT), StandardType.CONFIGURATION, 404),
                Arguments.of(new InvalidRequestException(QUOTED_PROMPT), StandardType.CONFIGURATION, 400),
                Arguments.of(new HttpException(413, QUOTED_PROMPT), StandardType.CONFIGURATION, 413),
                Arguments.of(new ContentFilteredException(QUOTED_PROMPT), StandardType.RUNTIME, 422),
                Arguments.of(new IllegalStateException(QUOTED_PROMPT), StandardType.RUNTIME, 500));
    }

    @ParameterizedTest
    @MethodSource("table")
    void each_provider_failure_is_a_standard_error_without_the_provider_s_message(RuntimeException thrown,
            StandardType type, int status) {
        ScriptedModel model = new ScriptedModel(request -> {
            throw thrown;
        });
        var handler = LangChain4jWorker.builder().chatModel(model).build().handler(Chat.class, (ai, t) -> ai.reply("hi"));

        ErrorInfo error = failure(handler, task("i-A", "/do/0/a", "c-1")).toErrorInfo();

        assertThat(error.type()).isEqualTo(type.uri());
        assertThat(error.status()).isEqualTo(status);
        Throwable recognized = thrown.getCause() != null ? thrown.getCause() : thrown;   // the detail names it
        assertThat(error.detail()).doesNotContain(QUOTED_PROMPT).contains(recognized.getClass().getSimpleName());
        assertThat(error.retryAfter()).isNull();
    }

    @Test
    void a_rate_limit_carries_the_retry_after_the_extractor_finds_or_the_default() {
        ScriptedModel model = new ScriptedModel(request -> {
            throw new RateLimitException("retry in 30s");
        });
        var extracted = LangChain4jWorker.builder().chatModel(model)
                .retryAfter(t -> Optional.of(Duration.ofSeconds(30))).build()
                .handler(Chat.class, (ai, t) -> ai.reply("hi"));
        var defaulted = LangChain4jWorker.builder().chatModel(model).rateLimitRetryAfter(Duration.ofSeconds(10)).build()
                .handler(Chat.class, (ai, t) -> ai.reply("hi"));

        assertThat(failure(extracted, task("i-A", "/do/0/a", "c-1")).toErrorInfo().retryAfter())
                .isEqualTo(Duration.ofSeconds(30));
        assertThat(failure(defaulted, task("i-A", "/do/0/a", "c-2")).toErrorInfo().retryAfter())
                .isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    void a_task_failure_thrown_by_the_author_passes_through() {
        var handler = LangChain4jWorker.builder().chatModel(ScriptedModel.replying("x")).build()
                .handler(Chat.class, (ai, t) -> {
                    throw new io.stintflow.worker.TaskFailure(URI.create("urn:example:quota"), 409, "over quota", null);
                });

        ErrorInfo error = failure(handler, task("i-A", "/do/0/a", "c-1")).toErrorInfo();

        assertThat(error.type()).isEqualTo(URI.create("urn:example:quota"));
        assertThat(error.status()).isEqualTo(409);
    }
}
