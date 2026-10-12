package io.stintflow.worker.decision.jev;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionOption;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.decision.AdapterDescriptor;
import io.stintflow.worker.decision.Decision;
import io.stintflow.worker.decision.DecisionAdapter;
import io.stintflow.worker.decision.DecisionFailures;
import io.stintflow.worker.decision.DecisionTask;
import io.stintflow.worker.decision.DecisionUsage;

/**
 * A decision adapter over TypeSafe AI's Jev (SDD 3a.2, 4.4), through its documented HTTP API — {@code POST
 * /v1/systemone} — with the JDK's {@link HttpClient} and Jackson: no third-party client, so nothing extra for Java 25
 * or a native image. One question per decision:
 * <table>
 *   <caption>Mapping</caption>
 *   <tr><th>Stint</th><th>Jev question</th><th>Decision</th></tr>
 *   <tr><td>choice</td><td>{@code choice}, criteria = option → description</td>
 *       <td>value = {@code choice}; confidence; distribution = {@code probabilities}</td></tr>
 *   <tr><td>boolean</td><td>{@code noul}, criteria from the options {@code true}/{@code false} if any</td>
 *       <td>value = p ≥ 0.5; confidence = max(p, 1 − p); distribution = {true: p, false: 1 − p}</td></tr>
 *   <tr><td>score</td><td>{@code score}, criteria = the configured levels</td>
 *       <td>value = {@code score}; confidence; distribution = {@code probabilities}</td></tr>
 * </table>
 * HTTP errors map as {@link DecisionFailures#http}; a 429's {@code Retry-After} (seconds or an HTTP date) becomes the
 * error's {@code retryAfter}. The API key comes from a supplier the worker owns — never from Stint's configuration.
 * <p>
 * <b>Validated against a fake server only</b>: no account or key was available (SDD 3a.2, 8d).
 */
public final class JevDecisionAdapter implements DecisionAdapter {

    public static final String VERSION = "jev-adapter/1";
    public static final URI DEFAULT_BASE_URI = URI.create("https://api.typesafe.ai");
    static final String QUESTION = "decision";

    private final AdapterDescriptor descriptor;
    private final URI endpoint;
    private final Supplier<String> apiKey;
    private final String model;
    private final List<String> scoreLevels;
    private final Duration timeout;
    private final HttpClient http;
    private final InstantSource clock;
    private final Duration rateLimitRetryAfter;

    private JevDecisionAdapter(Builder b) {
        this.descriptor = new AdapterDescriptor(Objects.requireNonNull(b.id, "id is required"), "typesafe", b.model,
                VERSION, true);
        this.endpoint = b.baseUri.resolve("/v1/systemone");
        this.apiKey = Objects.requireNonNull(b.apiKey, "apiKey is required");
        this.model = b.model;
        this.scoreLevels = b.scoreLevels;
        this.timeout = b.timeout;
        this.http = b.http != null ? b.http : HttpClient.newBuilder().connectTimeout(b.timeout).build();
        this.clock = b.clock;
        this.rateLimitRetryAfter = b.rateLimitRetryAfter;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public AdapterDescriptor descriptor() {
        return descriptor;
    }

    @Override
    public CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task) {
        ObjectNode body;
        try {
            body = body(request, task.instructions());
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
        HttpRequest call = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Authorization", "Bearer " + apiKey.get())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.bytes(body)))
                .build();
        Instant start = clock.instant();
        return http.sendAsync(call, HttpResponse.BodyHandlers.ofByteArray())
                .handle((response, failure) -> {
                    Duration latency = Duration.between(start, clock.instant());
                    if (failure != null) {
                        throw failure(failure);
                    }
                    if (response.statusCode() != 200) {
                        throw DecisionFailures.http(response.statusCode(), retryAfter(response), "typesafe");
                    }
                    return answer(request, response.body(), latency);
                });
    }

    ObjectNode body(DecisionRequest request, String instructions) {
        ObjectNode question = Json.obj();
        question.put("instructions", instructions);
        switch (request.kind()) {
            case CHOICE -> {
                question.put("type", "choice");
                ObjectNode criteria = question.putObject("criteria");
                for (DecisionOption option : request.options()) {
                    criteria.put(option.name(), describe(option));
                }
            }
            case BOOLEAN -> {
                question.put("type", "noul");
                if (!request.options().isEmpty()) {
                    ObjectNode criteria = question.putObject("criteria");
                    request.options().forEach(option -> criteria.put(option.name(), describe(option)));
                }
            }
            case SCORE -> {
                if (scoreLevels.isEmpty()) {
                    throw DecisionFailures.configuration(400, "The Jev adapter '" + descriptor.id()
                            + "' has no score levels configured, so it can't make a score decision");
                }
                question.put("type", "score");
                scoreLevels.forEach(question.putArray("criteria")::add);
            }
        }
        ObjectNode body = Json.obj();
        body.set("state", request.input() == null ? Json.MAPPER.nullNode() : request.input());
        body.put("model", model);
        body.putObject("questions").set(QUESTION, question);
        return body;
    }

    private static String describe(DecisionOption option) {
        return option.description() == null || option.description().isBlank() ? option.name() : option.description();
    }

    private Decision answer(DecisionRequest request, byte[] bytes, Duration latency) {
        JsonNode response;
        try {
            response = Json.read(bytes);
        } catch (RuntimeException e) {
            throw DecisionFailures.validation("The Jev answer is not JSON");
        }
        JsonNode answer = response.path("answers").path(QUESTION);
        if (!answer.isObject()) {
            throw DecisionFailures.validation("The Jev answer has no answers." + QUESTION);
        }
        JsonNode usage = response.path("usage");
        DecisionUsage used = new DecisionUsage(1, usage.path("input_tokens").asLong(0),
                usage.path("output_tokens").asLong(0), latency, null);
        String answeredBy = response.path("model").isTextual() ? response.path("model").asText() : null;
        return switch (request.kind()) {
            case CHOICE -> new Decision(answer.get("choice"), number(answer, "confidence"),
                    probabilities(answer), answeredBy, used);
            case SCORE -> new Decision(answer.get("score"), number(answer, "confidence"),
                    probabilities(answer), answeredBy, used);
            case BOOLEAN -> {
                Double p = number(answer, "noul");
                if (p == null || p < 0 || p > 1) {
                    throw DecisionFailures.validation("The Jev answer's noul is not a probability");
                }
                yield new Decision(Json.MAPPER.getNodeFactory().booleanNode(p >= 0.5), Math.max(p, 1 - p),
                        Map.of("true", p, "false", 1 - p), answeredBy, used);
            }
        };
    }

    private static Double number(JsonNode answer, String field) {
        JsonNode node = answer.get(field);
        if (node == null || node.isNull()) {
            return null;
        }
        if (!node.isNumber()) {
            throw DecisionFailures.validation("The Jev answer's " + field + " is not a number");
        }
        return node.doubleValue();
    }

    private static Map<String, Double> probabilities(JsonNode answer) {
        JsonNode node = answer.get("probabilities");
        if (node == null || !node.isObject()) {
            return null;
        }
        Map<String, Double> probabilities = new HashMap<>();
        node.properties().forEach(e -> {
            if (e.getValue().isNumber()) {
                probabilities.put(e.getKey(), e.getValue().doubleValue());
            }
        });
        return probabilities;
    }

    private Duration retryAfter(HttpResponse<?> response) {
        return response.headers().firstValue("Retry-After").map(this::parseRetryAfter).orElse(rateLimitRetryAfter);
    }

    /** {@code Retry-After} is delay-seconds or an HTTP date (RFC 9110, 10.2.3); anything else is ignored. */
    Duration parseRetryAfter(String value) {
        try {
            long seconds = Long.parseLong(value.trim());
            return seconds >= 0 ? Duration.ofSeconds(seconds) : rateLimitRetryAfter;
        } catch (NumberFormatException e) {
            try {
                Instant at = ZonedDateTime.parse(value.trim(), DateTimeFormatter.RFC_1123_DATE_TIME).toInstant();
                Duration until = Duration.between(clock.instant(), at);
                return until.isNegative() ? Duration.ZERO : until;
            } catch (DateTimeParseException ignored) {
                return rateLimitRetryAfter;
            }
        }
    }

    private static TaskFailure failure(Throwable thrown) {
        Throwable cause = thrown instanceof CompletionException && thrown.getCause() != null ? thrown.getCause() : thrown;
        if (cause instanceof TaskFailure failure) {
            return failure;
        }
        if (cause instanceof HttpTimeoutException) {
            return DecisionFailures.timeout("The decision provider typesafe timed out (" + cause.getClass().getSimpleName()
                    + ")");
        }
        if (cause instanceof IOException) {
            return DecisionFailures.unavailable(DecisionFailures.UNAVAILABLE,
                    "The decision provider typesafe is unreachable (" + cause.getClass().getSimpleName() + ")");
        }
        return new TaskFailure(StandardType.RUNTIME.uri(), 500,
                "The Jev call failed (" + cause.getClass().getSimpleName() + ")", null);
    }

    public static final class Builder {
        private String id = "jev";
        private URI baseUri = DEFAULT_BASE_URI;
        private Supplier<String> apiKey;
        private String model = "jev-latest";
        private List<String> scoreLevels = List.of();
        private Duration timeout = Duration.ofSeconds(30);
        private HttpClient http;
        private InstantSource clock = InstantSource.system();
        private Duration rateLimitRetryAfter;

        private Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /** Where the API is; tests point it at a fake server. Default {@code https://api.typesafe.ai}. */
        public Builder baseUri(URI baseUri) {
            this.baseUri = Objects.requireNonNull(baseUri, "baseUri");
            return this;
        }

        /** The API key, read on every call from wherever the worker keeps its secrets. */
        public Builder apiKey(Supplier<String> apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        /** Default {@code jev-latest}. */
        public Builder model(String model) {
            this.model = Objects.requireNonNull(model, "model");
            return this;
        }

        /** The levels of a score decision, lowest first (2 to 10); without them, a score decision is not supported. */
        public Builder scoreLevels(List<String> levels) {
            if (levels.size() < 2 || levels.size() > 10) {
                throw new IllegalArgumentException("Jev score levels are 2 to 10, got " + levels.size());
            }
            this.scoreLevels = List.copyOf(levels);
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder httpClient(HttpClient http) {
            this.http = http;
            return this;
        }

        public Builder clock(InstantSource clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /** The {@code retryAfter} of a 429 without a {@code Retry-After} header. None by default. */
        public Builder rateLimitRetryAfter(Duration retryAfter) {
            this.rateLimitRetryAfter = retryAfter;
            return this;
        }

        public JevDecisionAdapter build() {
            return new JevDecisionAdapter(this);
        }
    }
}
