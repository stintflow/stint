package io.stintflow.worker.transcription;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskReport;

/**
 * SDD 3a.6, 4.2: the {@code transcribe} handler reads the audio by pointer, writes the transcript to a blob of the
 * {@code raw-transcript} class and answers only a pointer and numbers; the OpenAI-compatible adapter speaks multipart
 * and {@code verbose_json} to a loopback server and maps its errors, never quoting it. Fake server, synthetic audio.
 */
class TranscriptionTest {

    static final String MARKER = "SYNTH-PII-0901";
    static final byte[] AUDIO = "RIFF-synthetic-silence".getBytes(StandardCharsets.UTF_8);
    static final Transcriber.Transcript SCRIPT = new Transcriber.Transcript("en", Duration.ofSeconds(9), List.of(
            new Transcriber.Segment(Duration.ZERO, Duration.ofSeconds(4), "Let's review the recap."),
            new Transcriber.Segment(Duration.ofSeconds(4), Duration.ofSeconds(9), "Mail " + MARKER + "@example.com.")));

    /** Blobs in a map, by key; a {@code put} with a class goes under its prefix, as the SPI's default does. */
    static final class MapBlobs implements BlobStore {
        final Map<URI, byte[]> data = new ConcurrentHashMap<>();

        @Override
        public CompletionStage<URI> put(byte[] bytes, String key) {
            URI uri = URI.create("mem://" + key);
            data.put(uri, bytes);
            return CompletableFuture.completedFuture(uri);
        }

        @Override
        public CompletionStage<byte[]> get(URI ref) {
            byte[] bytes = data.get(ref);
            return bytes == null ? CompletableFuture.failedFuture(new BlobNotFoundException(ref, "absent"))
                    : CompletableFuture.completedFuture(bytes);
        }

        @Override
        public CompletionStage<Void> delete(URI ref) {
            data.remove(ref);
            return CompletableFuture.completedFuture(null);
        }
    }

    private HttpServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private static TaskContext task(JsonNode input, TaskReport report) {
        return new TaskContext("/do/1/transcribe", "i-1", "c-1", 1, input, new WorkflowRef("examples", "pm", "1.0.0"),
                "chn-1", Lineage.NONE, null, Sensitivity.RESTRICTED, report);
    }

    @Test
    void the_handler_writes_the_transcript_to_a_restricted_blob_and_answers_only_a_pointer() {
        MapBlobs blobs = new MapBlobs();
        URI audioRef = blobs.put(AUDIO, "retention/raw-audio/m-1.wav").toCompletableFuture().join();
        FakeTranscriber fake = FakeTranscriber.scripted(Map.of(FakeTranscriber.sha256(AUDIO), SCRIPT));
        TaskReport report = new TaskReport();

        JsonNode out = new TranscriptionHandler(fake, blobs).execute(task(Json.obj().put("audioRef",
                audioRef.toString()).put("language", "en"), report)).toCompletableFuture().join();

        assertThat(ClaimCheck.isPointer(out.get("transcriptRef"))).isTrue();
        assertThat(out.toString()).doesNotContain(MARKER, "recap");
        assertThat(out.path("segmentCount").asInt()).isEqualTo(2);
        assertThat(out.path("durationSeconds").asDouble()).isEqualTo(9.0);
        URI stored = URI.create(out.get("transcriptRef").get(ClaimCheck.POINTER_FIELD).asText());
        assertThat(stored.toString()).startsWith("mem://retention/raw-transcript/transcripts/i-1/");
        JsonNode transcript = Json.read(blobs.data.get(stored));
        assertThat(transcript.path("segments").get(1).path("text").asText()).contains(MARKER);
        assertThat(transcript.path("segments").get(1).path("start").asDouble()).isEqualTo(4.0);
        assertThat(report.provider()).isEqualTo(new ModelProvider("fake-transcriber", "corpus-script", false));
        assertThat(blobs.data).containsKey(audioRef);   // the audio is only read
    }

    @Test
    void a_missing_audio_is_a_validation_error_and_an_unavailable_server_a_retryable_one() {
        MapBlobs blobs = new MapBlobs();
        FakeTranscriber fake = FakeTranscriber.scripted(Map.of(FakeTranscriber.sha256(AUDIO), SCRIPT));
        TranscriptionHandler handler = new TranscriptionHandler(fake, blobs);

        assertThat(failure(handler, Json.obj().put("audioRef", "mem://retention/raw-audio/gone.wav")))
                .satisfies(e -> {
                    assertThat(e.type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
                    assertThat(e.detail()).contains("expired");
                });
        assertThat(failure(handler, Json.obj().put("audioRef", "not a uri"))).satisfies(e ->
                assertThat(e.detail()).isEqualTo("audioRef must be a pointer to the audio (a URI)"));

        URI audioRef = blobs.put(AUDIO, "a.wav").toCompletableFuture().join();
        TranscriptionHandler flaky = new TranscriptionHandler(fake.failingFirst(1), blobs);
        assertThat(failure(flaky, Json.obj().put("audioRef", audioRef.toString()))).satisfies(e -> {
            assertThat(e.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
            assertThat(e.status()).isEqualTo(503);
        });
        assertThat(flaky.execute(task(Json.obj().put("audioRef", audioRef.toString()), new TaskReport()))
                .toCompletableFuture().join().path("segmentCount").asInt()).isEqualTo(2);   // the retry, same audio
    }

    private static ErrorInfo failure(TranscriptionHandler handler, JsonNode input) {
        try {
            handler.execute(task(input, new TaskReport())).toCompletableFuture().join();
        } catch (CompletionException e) {
            return ((TaskFailure) e.getCause()).toErrorInfo();
        }
        throw new AssertionError("the transcription did not fail");
    }

    private URI serve(int status, String body, AtomicReference<String> received) throws IOException {
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/v1/audio/transcriptions", exchange -> {
            received.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.ISO_8859_1)
                    + "\nContent-Type: " + exchange.getRequestHeaders().getFirst("Content-Type"));
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        });
        server.start();
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void the_adapter_speaks_multipart_and_verbose_json_to_a_loopback_server() throws Exception {
        AtomicReference<String> received = new AtomicReference<>();
        URI base = serve(200, """
                {"text": "Let's review the recap. Next steps.", "language": "english", "duration": 7.5,
                 "segments": [ {"id": 0, "start": 0.0, "end": 3.2, "text": " Let's review the recap."},
                               {"id": 1, "start": 3.2, "end": 7.5, "text": " Next steps."} ]}""", received);
        OpenAiCompatibleTranscriber whisper = OpenAiCompatibleTranscriber.builder().baseUri(base)
                .model("Systran/faster-distil-whisper-small.en").build();

        Transcriber.Transcript transcript = whisper.transcribe(new Transcriber.Audio(AUDIO, "audio/wav", "m-1.wav"),
                new Transcriber.Options("en"));

        assertThat(transcript.language()).isEqualTo("en");
        assertThat(transcript.duration()).isEqualTo(Duration.ofMillis(7500));
        assertThat(transcript.segments()).hasSize(2);
        assertThat(transcript.segments().get(1).start()).isEqualTo(Duration.ofMillis(3200));
        assertThat(transcript.text()).isEqualTo("Let's review the recap. Next steps.");
        assertThat(received.get()).contains("name=\"model\"\r\n\r\nSystran/faster-distil-whisper-small.en",
                "name=\"response_format\"\r\n\r\nverbose_json", "name=\"language\"\r\n\r\nen",
                "filename=\"m-1.wav\"", "RIFF-synthetic-silence", "Content-Type: multipart/form-data; boundary=");
        assertThat(whisper.provenance()).isEqualTo(new Transcriber.Provenance("speaches",
                "Systran/faster-distil-whisper-small.en", false));
    }

    @Test
    void the_adapter_maps_errors_without_quoting_the_server() throws Exception {
        AtomicReference<String> received = new AtomicReference<>();
        URI busy = serve(503, "{\"error\": \"overloaded while reading " + MARKER + "\"}", received);
        OpenAiCompatibleTranscriber whisper = OpenAiCompatibleTranscriber.builder().baseUri(busy).model("m").build();
        Transcriber.Audio audio = new Transcriber.Audio(AUDIO, "audio/wav", "a.wav");

        assertThatThrownBy(() -> whisper.transcribe(audio, new Transcriber.Options("en")))
                .isInstanceOf(TaskFailure.class)
                .satisfies(e -> {
                    ErrorInfo error = ((TaskFailure) e).toErrorInfo();
                    assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
                    assertThat(error.status()).isEqualTo(503);
                    assertThat(error.detail()).isEqualTo("The transcription server speaches answered HTTP 503")
                            .doesNotContain(MARKER);
                });
        stop();
        URI badRequest = serve(415, "{}", received);
        assertThatThrownBy(() -> OpenAiCompatibleTranscriber.builder().baseUri(badRequest).model("m").build()
                .transcribe(audio, new Transcriber.Options(null)))
                .satisfies(e -> assertThat(((TaskFailure) e).toErrorInfo().type()).isEqualTo(ErrorInfo.TYPE_VALIDATION));
        stop();
        server = null;
        assertThatThrownBy(() -> OpenAiCompatibleTranscriber.builder().baseUri(badRequest).model("m").build()
                .transcribe(audio, new Transcriber.Options("en")))   // nobody listens anymore
                .satisfies(e -> {
                    ErrorInfo error = ((TaskFailure) e).toErrorInfo();
                    assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
                    assertThat(error.detail()).contains("refused the connection");
                });
    }

    @Test
    void a_non_loopback_server_is_refused_unless_allowed_and_then_counts_as_remote() {
        assertThatThrownBy(() -> OpenAiCompatibleTranscriber.builder().baseUri(URI.create("http://192.0.2.10:8000"))
                .model("m").build()).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("non-loopback");
        assertThat(OpenAiCompatibleTranscriber.builder().baseUri(URI.create("http://192.0.2.10:8000")).model("m")
                .allowRemote(true).build().provenance().remote()).isTrue();
        assertThat(new RetentionClass("raw-transcript", Duration.ofDays(7))).isEqualTo(TranscriptionHandler.RAW_TRANSCRIPT);
    }
}
