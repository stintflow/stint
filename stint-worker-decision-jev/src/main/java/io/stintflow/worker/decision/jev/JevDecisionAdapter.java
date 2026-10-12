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
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionKind;
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
 * A decision adapter over the Jev API (SDD 3a.2, 4.4) — {@code POST /v1/systemone} — with the JDK's
 * {@link HttpClient} and Jackson: no third-party client, so nothing extra for Java 25 or a native image. One question
 * per decision:
 * <table>
 *   <caption>Mapping</caption>
 *   <tr><th>Stint</th><th>Jev question</th><th>Decision</th></tr>
 *   <tr><td>choice</td><td>{@code choice}, criteria = option → description</td>
 *       <td>value = {@code choice}; confidence by the {@link ConfidenceSource}; distribution = {@code probabilities}</td></tr>
 *   <tr><td>boolean</td><td>{@code noul}, criteria from the options {@code true}/{@code false} if any; without
 *       options, none for TypeSafe and {@code {true: "Yes", false: "No"}} for LitJev</td>
 *       <td>value = p ≥ 0.5; confidence = max(p, 1 − p); distribution = {true: p, false: 1 − p}</td></tr>
 *   <tr><td>score</td><td>{@code score}, criteria = the configured levels</td>
 *       <td>value = {@code score}; confidence by the {@link ConfidenceSource}; distribution = {@code probabilities}</td></tr>
 * </table>
 * HTTP errors map as {@link DecisionFailures#http}; a 429's {@code Retry-After} (seconds or an HTTP date) becomes the
 * error's {@code retryAfter}. The API key comes from a supplier the worker owns — never from Stint's configuration.
 * A decision without input fails with a {@code validation} error before any call (SDD 3a.2b, 4.3).
 * <p>
 * Two servers speak this API (SDD 3a.2b, 4.1), chosen with {@link Builder#server}:
 * <ul>
 *   <li>{@link Server#TYPESAFE}, the default: TypeSafe AI's hosted Jev, with an API key. <b>Validated against a fake
 *       server only</b>: no account or key was available (SDD 3a.2, 8d).</li>
 *   <li>{@link Server#LITJEV}: a LitJev server on this machine — an independent reproduction over Qwen checkpoints —
 *       without a key. On a loopback address the adapter is local ({@code remote = false}), for the sensitivity
 *       policy of SDD 3a.5.</li>
 * </ul>
 */
public final class JevDecisionAdapter implements DecisionAdapter {

    public static final String VERSION = "jev-adapter/1";
    public static final URI DEFAULT_BASE_URI = URI.create("https://api.typesafe.ai");
    public static final URI DEFAULT_LOCAL_BASE_URI = URI.create("http://127.0.0.1:8000");
    static final String QUESTION = "decision";
    private static final Pattern VERSION_PART = Pattern.compile("[A-Za-z0-9._-]+");
    private static final Logger LOG = LoggerFactory.getLogger(JevDecisionAdapter.class);

    /** The server behind the adapter, and its defaults (SDD 3a.2b, 4.1). */
    public enum Server {
        /** TypeSafe AI's hosted Jev: a key is required. */
        TYPESAFE("typesafe", "jev", DEFAULT_BASE_URI, "jev-latest", ConfidenceSource.SERVER, Duration.ofSeconds(30)),
        /** A local LitJev server: no key; its first request loads the model, hence the longer timeout. */
        LITJEV("litjev", "litjev", DEFAULT_LOCAL_BASE_URI, "litjev", ConfidenceSource.MAX_PROBABILITY,
                Duration.ofSeconds(120));

        final String provider;
        final String id;
        final URI baseUri;
        final String model;
        final ConfidenceSource confidenceSource;
        final Duration timeout;

        Server(String provider, String id, URI baseUri, String model, ConfidenceSource confidenceSource,
                Duration timeout) {
            this.provider = provider;
            this.id = id;
            this.baseUri = baseUri;
            this.model = model;
            this.confidenceSource = confidenceSource;
            this.timeout = timeout;
        }
    }

    /**
     * Where the confidence of a choice or a score comes from (SDD 3a.2b, 4.2). A {@code noul}'s is always
     * {@code max(p, 1 − p)}.
     */
    public enum ConfidenceSource {
        /** The answer's {@code confidence}. LitJev's is a normalized Gini concentration, not a probability. */
        SERVER,
        /** The highest of the answer's {@code probabilities}: the chance of the chosen option. */
        MAX_PROBABILITY
    }

    private final Server server;
    private final AdapterDescriptor descriptor;
    private final URI endpoint;
    private final Supplier<String> apiKey;
    private final String model;
    private final List<String> scoreLevels;
    private final Duration timeout;
    private final ConfidenceSource confidenceSource;
    private final HttpClient http;
    private final InstantSource clock;
    private final Duration rateLimitRetryAfter;

    private JevDecisionAdapter(Builder b) {
        Server server = b.server;
        this.server = server;
        String id = b.id != null ? b.id : server.id;
        URI baseUri = b.baseUri != null ? b.baseUri : server.baseUri;
        this.model = b.model != null ? b.model : server.model;
        this.timeout = b.timeout != null ? b.timeout : server.timeout;
        this.confidenceSource = b.confidenceSource != null ? b.confidenceSource : server.confidenceSource;
        boolean remote = true;
        String version = VERSION;
        if (server == Server.TYPESAFE) {
            if (b.apiKey == null) {
                throw new IllegalArgumentException("The Jev adapter '" + id
                        + "' needs an API key (use server(LITJEV) for a local server without one)");
            }
        } else {
            boolean loopback = isLoopback(baseUri);
            if (!loopback && !b.allowRemote) {
                throw new IllegalArgumentException("The local Jev profile '" + id + "' points at a non-loopback address ("
                        + baseUri.getHost() + "); call allowRemote(true) if that is intended");
            }
            if (!loopback) {
                LOG.warn("The local Jev profile '{}' sends decision inputs to {}: it is remote", id, baseUri.getHost());
            }
            remote = !loopback;
            version = VERSION + ";server=" + server.provider + (b.serverVersion != null ? "@" + b.serverVersion : "")
                    + (b.readout != null ? ";readout=" + b.readout : "");
        }
        this.descriptor = new AdapterDescriptor(id, server.provider, model, version, remote);
        this.endpoint = baseUri.resolve("/v1/systemone");
        this.apiKey = b.apiKey;
        this.scoreLevels = b.scoreLevels;
        this.http = b.http != null ? b.http : HttpClient.newBuilder().connectTimeout(timeout).build();
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

    /**
     * Whether {@code uri} names this machine — by its text, without resolving a name: {@code localhost}, an IPv4
     * literal in 127.0.0.0/8, or {@code ::1}.
     */
    static boolean isLoopback(URI uri) {
        String host = uri.getHost();
        if (host == null) {
            return false;
        }
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        if (host.equalsIgnoreCase("localhost") || host.equals("::1") || host.equals("0:0:0:0:0:0:0:1")) {
            return true;
        }
        String[] parts = host.split("\\.", -1);
        if (parts.length != 4 || !parts[0].equals("127")) {
            return false;
        }
        for (String part : parts) {
            if (part.isEmpty() || part.length() > 3 || !part.chars().allMatch(c -> c >= '0' && c <= '9')
                    || Integer.parseInt(part) > 255) {
                return false;
            }
        }
        return true;
    }

    /** No input to decide on (SDD 3a.2b, 4.3): absent or null, blank text, an empty object or an empty list. */
    static boolean hasNoInput(JsonNode input) {
        return input == null || input.isNull() || input.isMissingNode()
                || (input.isTextual() && input.textValue().isBlank())
                || (input.isContainerNode() && input.isEmpty());
    }

    @Override
    public CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task) {
        ObjectNode body;
        try {
            if (hasNoInput(request.input())) {
                throw DecisionFailures.validation("The decision has no input to decide on");
            }
            body = body(request, task.instructions());
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
        HttpRequest.Builder call = HttpRequest.newBuilder(endpoint)
                .timeout(timeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.bytes(body)));
        String key = apiKey == null ? null : apiKey.get();
        if (key != null && !key.isBlank()) {
            call.header("Authorization", "Bearer " + key);
        }
        Instant start = clock.instant();
        return http.sendAsync(call.build(), HttpResponse.BodyHandlers.ofByteArray())
                .handle((response, failure) -> {
                    Duration latency = Duration.between(start, clock.instant());
                    if (failure != null) {
                        throw failure(failure);
                    }
                    if (response.statusCode() != 200) {
                        throw DecisionFailures.http(response.statusCode(), retryAfter(response), descriptor.provider());
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
                } else if (server == Server.LITJEV) {
                    // LitJev 2cd915a's content readout refuses a noul without both descriptions (SDD 3a.2b, 9.6)
                    question.putObject("criteria").put("true", "Yes").put("false", "No");
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
        body.set("state", request.input());
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
            case CHOICE, SCORE -> {
                Map<String, Double> probabilities = probabilities(answer);
                yield new Decision(answer.get(request.kind() == DecisionKind.CHOICE ? "choice" : "score"),
                        confidence(answer, probabilities), probabilities, answeredBy, used);
            }
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

    /** SDD 3a.2b, 4.2: the server's confidence or the highest probability; {@code null} when the answer has none. */
    private Double confidence(JsonNode answer, Map<String, Double> probabilities) {
        if (confidenceSource == ConfidenceSource.SERVER) {
            return number(answer, "confidence");
        }
        return probabilities == null || probabilities.isEmpty() ? null
                : probabilities.values().stream().mapToDouble(Double::doubleValue).max().orElseThrow();
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
                double p = e.getValue().doubleValue();
                if (!(p >= 0 && p <= 1)) {
                    throw DecisionFailures.validation("The Jev answer's probabilities are not in [0,1]");
                }
                probabilities.put(e.getKey(), p);
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

    private TaskFailure failure(Throwable thrown) {
        Throwable cause = thrown instanceof CompletionException && thrown.getCause() != null ? thrown.getCause() : thrown;
        if (cause instanceof TaskFailure failure) {
            return failure;
        }
        String provider = descriptor.provider();
        if (cause instanceof HttpTimeoutException) {
            return DecisionFailures.timeout("The decision provider " + provider + " timed out ("
                    + cause.getClass().getSimpleName() + ")");
        }
        if (cause instanceof IOException) {
            return DecisionFailures.unavailable(DecisionFailures.UNAVAILABLE,
                    "The decision provider " + provider + " is unreachable (" + cause.getClass().getSimpleName() + ")");
        }
        return new TaskFailure(StandardType.RUNTIME.uri(), 500,
                "The Jev call failed (" + cause.getClass().getSimpleName() + ")", null);
    }

    /** Values left unset take the {@link Server}'s defaults, whatever the order of the calls. */
    public static final class Builder {
        private Server server = Server.TYPESAFE;
        private String id;
        private URI baseUri;
        private Supplier<String> apiKey;
        private String model;
        private List<String> scoreLevels = List.of();
        private Duration timeout;
        private ConfidenceSource confidenceSource;
        private boolean allowRemote;
        private String serverVersion;
        private String readout;
        private HttpClient http;
        private InstantSource clock = InstantSource.system();
        private Duration rateLimitRetryAfter;

        private Builder() {
        }

        /** The server behind the adapter, and with it the defaults (SDD 3a.2b, 4.1). Default {@link Server#TYPESAFE}. */
        public Builder server(Server server) {
            this.server = Objects.requireNonNull(server, "server");
            return this;
        }

        /** Default {@code jev}, or {@code litjev} for {@link Server#LITJEV}. */
        public Builder id(String id) {
            this.id = id;
            return this;
        }

        /**
         * Where the API is; tests point it at a fake server. Default {@code https://api.typesafe.ai}, or
         * {@code http://127.0.0.1:8000} for {@link Server#LITJEV}.
         */
        public Builder baseUri(URI baseUri) {
            this.baseUri = Objects.requireNonNull(baseUri, "baseUri");
            return this;
        }

        /**
         * The API key, read on every call from wherever the worker keeps its secrets. Required for
         * {@link Server#TYPESAFE}. Optional for {@link Server#LITJEV} (e.g. behind an authenticating proxy); a null
         * or blank key sends no {@code Authorization}.
         */
        public Builder apiKey(Supplier<String> apiKey) {
            this.apiKey = apiKey;
            return this;
        }

        /** Default {@code jev-latest}, or {@code litjev} — the loaded checkpoint — for {@link Server#LITJEV}. */
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

        /** Default 30 s, or 120 s for {@link Server#LITJEV}, whose first request loads the model. */
        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /** Default {@link ConfidenceSource#SERVER}, or {@link ConfidenceSource#MAX_PROBABILITY} for LitJev. */
        public Builder confidenceSource(ConfidenceSource source) {
            this.confidenceSource = Objects.requireNonNull(source, "source");
            return this;
        }

        /**
         * Lets the {@link Server#LITJEV} profile point at a non-loopback address: the adapter is then remote and
         * warns at startup. Without it, such an address fails {@link #build()}.
         */
        public Builder allowRemote(boolean allowRemote) {
            this.allowRemote = allowRemote;
            return this;
        }

        /** The LitJev version (e.g. its commit), for the provenance's version: the server's answer doesn't say. */
        public Builder serverVersion(String serverVersion) {
            this.serverVersion = versionPart("serverVersion", serverVersion);
            return this;
        }

        /** LitJev's readout ({@code content} or {@code coded}), for the provenance's version. */
        public Builder readout(String readout) {
            this.readout = versionPart("readout", readout);
            return this;
        }

        private static String versionPart(String name, String value) {
            if (value != null && !VERSION_PART.matcher(value).matches()) {
                throw new IllegalArgumentException(name + " is letters, digits, '.', '_' or '-', got '" + value + "'");
            }
            return value;
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
