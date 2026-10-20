package io.stintflow.worker.transcription;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.wire.Json;

/**
 * A {@link Transcriber} over a Whisper server with the OpenAI transcription API (SDD 3a.6, 8b): {@code POST
 * /v1/audio/transcriptions}, multipart ({@code file}, {@code model}, {@code language}, {@code response_format=
 * verbose_json}, {@code timestamp_granularities[]=segment}). Served by speaches (ex faster-whisper-server) and LocalAI;
 * whisper.cpp's server takes the same form on {@code /inference} ({@link Builder#path}).
 * <p>
 * Like the Jev adapter's local profile (SDD 3a.2b), it refuses a non-loopback address unless told otherwise
 * ({@link Builder#allowRemote}); in loopback it is local ({@code remote = false}) for the sensitivity policy. The JDK's
 * HttpClient only, no third-party client. Errors are {@link TranscriptionFailures}; a server's message never reaches
 * one.
 */
public final class OpenAiCompatibleTranscriber implements Transcriber {

    private final HttpClient http;
    private final URI endpoint;
    private final String model;
    private final String engine;
    private final Duration timeout;
    private final boolean remote;

    private OpenAiCompatibleTranscriber(Builder b) {
        URI base = Objects.requireNonNull(b.baseUri, "baseUri is required");
        this.model = Objects.requireNonNull(b.model, "model is required");
        this.engine = b.engine;
        this.timeout = b.timeout;
        boolean loopback = isLoopback(base);
        if (!loopback && !b.allowRemote) {
            throw new IllegalArgumentException("The transcriber points at a non-loopback address (" + base.getHost()
                    + "): a local Whisper server runs on loopback; set allowRemote for a remote one");
        }
        this.remote = !loopback;
        this.endpoint = base.resolve(b.path);
        this.http = b.http != null ? b.http : HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Provenance provenance() {
        return new Provenance(engine, model, remote);
    }

    @Override
    public Transcript transcribe(Audio audio, Options options) {
        String boundary = "stint-" + UUID.randomUUID();
        byte[] body = multipart(boundary, audio, options);
        HttpRequest request = HttpRequest.newBuilder(endpoint).timeout(timeout)
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body)).build();
        HttpResponse<byte[]> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (HttpTimeoutException e) {
            throw TranscriptionFailures.timeout("The transcription server " + engine + " did not answer within "
                    + timeout.toSeconds() + " s");
        } catch (ConnectException e) {
            throw TranscriptionFailures.unavailable(503, "The transcription server " + engine
                    + " refused the connection", null);
        } catch (IOException e) {
            throw TranscriptionFailures.unavailable(503, "The transcription server " + engine + " could not be "
                    + "reached (" + e.getClass().getSimpleName() + ")", null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw TranscriptionFailures.unavailable(503, "The transcription was interrupted", null);
        }
        if (response.statusCode() / 100 != 2) {
            throw TranscriptionFailures.http(response.statusCode(), retryAfter(response), engine);
        }
        return parse(response.body(), options);
    }

    private Transcript parse(byte[] body, Options options) {
        JsonNode json;
        try {
            json = Json.read(body);
        } catch (RuntimeException e) {
            throw TranscriptionFailures.validation("The transcription server " + engine + " answered something that "
                    + "is not JSON");
        }
        List<Segment> segments = new ArrayList<>();
        for (JsonNode segment : json.path("segments")) {
            if (!segment.path("text").isTextual()) {
                throw TranscriptionFailures.validation("A segment of the answer has no text");
            }
            segments.add(new Segment(seconds(segment.path("start")), seconds(segment.path("end")),
                    segment.get("text").asText()));
        }
        if (segments.isEmpty() && json.path("text").isTextual()) {
            // a server without segments: one segment for the whole audio
            segments.add(new Segment(Duration.ZERO, seconds(json.path("duration")), json.get("text").asText()));
        }
        if (segments.isEmpty()) {
            throw TranscriptionFailures.validation("The answer has neither segments nor text");
        }
        String language = json.path("language").asText(options.language());
        return new Transcript(normalizeLanguage(language), seconds(json.path("duration")), segments);
    }

    /** OpenAI-compatible servers answer {@code english}; the contract keeps ISO 639-1. */
    private static String normalizeLanguage(String language) {
        if (language == null) {
            return null;
        }
        return switch (language.toLowerCase(java.util.Locale.ROOT)) {
            case "english" -> "en";
            case "portuguese" -> "pt";
            case "spanish" -> "es";
            default -> language;
        };
    }

    private static Duration seconds(JsonNode node) {
        return node.isNumber() ? Duration.ofMillis(Math.round(node.asDouble() * 1000)) : Duration.ZERO;
    }

    private byte[] multipart(String boundary, Audio audio, Options options) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        field(out, boundary, "model", model);
        field(out, boundary, "response_format", "verbose_json");
        field(out, boundary, "timestamp_granularities[]", "segment");
        if (options.language() != null) {
            field(out, boundary, "language", options.language());
        }
        write(out, "--" + boundary + "\r\nContent-Disposition: form-data; name=\"file\"; filename=\""
                + audio.fileName().replace("\"", "") + "\"\r\nContent-Type: " + audio.mediaType() + "\r\n\r\n");
        out.writeBytes(audio.bytes());
        write(out, "\r\n--" + boundary + "--\r\n");
        return out.toByteArray();
    }

    private static void field(ByteArrayOutputStream out, String boundary, String name, String value) {
        write(out, "--" + boundary + "\r\nContent-Disposition: form-data; name=\"" + name + "\"\r\n\r\n" + value
                + "\r\n");
    }

    private static void write(ByteArrayOutputStream out, String text) {
        out.writeBytes(text.getBytes(StandardCharsets.UTF_8));
    }

    private static Duration retryAfter(HttpResponse<?> response) {
        Optional<String> header = response.headers().firstValue("Retry-After");
        try {
            return header.map(h -> Duration.ofSeconds(Long.parseLong(h.trim()))).orElse(null);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static boolean isLoopback(URI uri) {
        try {
            return uri.getHost() != null && InetAddress.getByName(uri.getHost()).isLoopbackAddress();
        } catch (IOException e) {
            return false;
        }
    }

    public static final class Builder {
        private URI baseUri;
        private String model;
        private String path = "/v1/audio/transcriptions";
        private String engine = "speaches";
        private Duration timeout = Duration.ofMinutes(10);
        private boolean allowRemote;
        private HttpClient http;

        private Builder() {
        }

        /** The server, e.g. {@code http://127.0.0.1:8000}. */
        public Builder baseUri(URI baseUri) {
            this.baseUri = baseUri;
            return this;
        }

        /** The model the server loads, e.g. {@code Systran/faster-distil-whisper-small.en}. */
        public Builder model(String model) {
            this.model = model;
            return this;
        }

        /** The transcription path: {@code /v1/audio/transcriptions} (default), or whisper.cpp's {@code /inference}. */
        public Builder path(String path) {
            this.path = Objects.requireNonNull(path, "path");
            return this;
        }

        /** The engine's name, for the provenance and the errors. Default {@code speaches}. */
        public Builder engine(String engine) {
            this.engine = Objects.requireNonNull(engine, "engine");
            return this;
        }

        /** How long one transcription may take. Default 10 minutes. */
        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        /** Allows a non-loopback server; it then counts as remote for the sensitivity policy. */
        public Builder allowRemote(boolean allowRemote) {
            this.allowRemote = allowRemote;
            return this;
        }

        public Builder httpClient(HttpClient http) {
            this.http = http;
            return this;
        }

        public OpenAiCompatibleTranscriber build() {
            return new OpenAiCompatibleTranscriber(this);
        }
    }
}
