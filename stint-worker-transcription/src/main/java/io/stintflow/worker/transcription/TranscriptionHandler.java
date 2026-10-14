package io.stintflow.worker.transcription;

import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.RetentionClass;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task behind {@code transcribe} (SDD 3a.6, 4.2). Its input is {@code { audioRef, language? }}: the audio by
 * pointer (a URI, or a claim-check pointer). It reads the audio, transcribes it, and writes the transcript —
 * {@code { language, durationSeconds, segments: [{start, end, text}], engine, model }} — to a blob of the
 * {@code raw-transcript} class (restricted, short retention). Its output never holds the text:
 * <pre>{ transcriptRef: {__stint_claim_check__: uri}, language, durationSeconds, segmentCount, engine, model }</pre>
 * The audio is only read: a failure leaves it where it was, for the workflow's retry. Logs carry ids and numbers.
 */
public final class TranscriptionHandler implements TaskHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TranscriptionHandler.class);
    public static final RetentionClass RAW_TRANSCRIPT = new RetentionClass("raw-transcript",
            java.time.Duration.ofDays(7));

    private final Transcriber transcriber;
    private final BlobStore blobs;
    private final RetentionClass retention;

    public TranscriptionHandler(Transcriber transcriber, BlobStore blobs) {
        this(transcriber, blobs, RAW_TRANSCRIPT);
    }

    public TranscriptionHandler(Transcriber transcriber, BlobStore blobs, RetentionClass retention) {
        this.transcriber = Objects.requireNonNull(transcriber, "transcriber");
        this.blobs = Objects.requireNonNull(blobs, "blobs");
        this.retention = Objects.requireNonNull(retention, "retention");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        URI audioRef;
        try {
            audioRef = audioRef(ctx.input().get("audioRef"));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        }
        String language = ctx.input().path("language").asText(null);
        return blobs.get(audioRef).handle((bytes, failure) -> {
            if (failure != null) {
                throw new CompletionException(BlobNotFoundException.in(failure)
                        ? TranscriptionFailures.validation("The audio is not in the blob store (deleted, or expired "
                                + "by its retention class)")
                        : TranscriptionFailures.unavailable(503, "The audio could not be read from the blob store",
                                null));
            }
            return bytes;
        }).thenCompose(bytes -> {
            Transcriber.Provenance provenance = transcriber.provenance();
            ctx.report().provider(new ModelProvider(provenance.engine(), provenance.model(), provenance.remote()));
            long started = System.nanoTime();
            Transcriber.Transcript transcript = transcriber.transcribe(
                    new Transcriber.Audio(bytes, mediaType(audioRef), fileName(audioRef)),
                    new Transcriber.Options(language));
            LOG.info("Transcribed task {} of instance {}: {} segments, {} s of audio, {} ms, engine {}", ctx.taskId(),
                    ctx.instanceId(), transcript.segments().size(), transcript.duration().toSeconds(),
                    (System.nanoTime() - started) / 1_000_000, provenance.engine());
            ObjectNode stored = toJson(transcript, provenance);
            String key = "transcripts/" + ctx.instanceId() + "/" + ctx.correlationId() + ".json";
            return blobs.put(Json.bytes(stored), key, retention).thenApply(uri -> {
                ObjectNode out = Json.obj();
                out.set("transcriptRef", ClaimCheck.pointer(uri));
                out.put("language", transcript.language());
                out.put("durationSeconds", seconds(transcript.duration()));
                out.put("segmentCount", transcript.segments().size());
                out.put("engine", provenance.engine());
                out.put("model", provenance.model());
                return (JsonNode) out;
            });
        });
    }

    /** The transcript as it rests in its blob (the {@code transcript.v1} contract). */
    public static ObjectNode toJson(Transcriber.Transcript transcript, Transcriber.Provenance provenance) {
        ObjectNode json = Json.obj();
        json.put("version", 1);
        json.put("language", transcript.language());
        json.put("durationSeconds", seconds(transcript.duration()));
        ArrayNode segments = json.putArray("segments");
        for (Transcriber.Segment segment : transcript.segments()) {
            segments.addObject().put("start", seconds(segment.start())).put("end", seconds(segment.end()))
                    .put("text", segment.text());
        }
        json.put("engine", provenance.engine());
        json.put("model", provenance.model());
        return json;
    }

    private static double seconds(java.time.Duration duration) {
        return duration.toMillis() / 1000.0;
    }

    private static URI audioRef(JsonNode node) {
        String text = node == null ? null : ClaimCheck.isPointer(node) ? node.get(ClaimCheck.POINTER_FIELD).asText()
                : node.isTextual() ? node.asText() : null;
        try {
            URI uri = text == null ? null : URI.create(text);
            if (uri == null || uri.getScheme() == null) {
                throw TranscriptionFailures.validation("audioRef must be a pointer to the audio (a URI)");
            }
            return uri;
        } catch (IllegalArgumentException e) {
            throw TranscriptionFailures.validation("audioRef must be a pointer to the audio (a URI)");
        }
    }

    private static String fileName(URI ref) {
        String path = ref.getPath() == null ? "" : ref.getPath();
        String name = path.substring(path.lastIndexOf('/') + 1);
        return name.isBlank() ? "audio.wav" : name;
    }

    private static String mediaType(URI ref) {
        String name = fileName(ref).toLowerCase(java.util.Locale.ROOT);
        if (name.endsWith(".mp3")) {
            return "audio/mpeg";
        }
        if (name.endsWith(".ogg") || name.endsWith(".opus")) {
            return "audio/ogg";
        }
        if (name.endsWith(".flac")) {
            return "audio/flac";
        }
        return "audio/wav";
    }
}
