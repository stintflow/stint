package io.stintflow.worker.redaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.wire.redaction.RedactionContract;
import io.stintflow.worker.TaskContext;

/**
 * SDD 3a.6, 4.6: with a blob store, the redactor reads a raw transcript by its pointer — the transcript stays in its
 * restricted blob — and answers its segments' text redacted, inline. Synthetic data.
 */
class RedactionPointerTest {

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
            return CompletableFuture.completedFuture(data.get(ref));
        }

        @Override
        public CompletionStage<Void> delete(URI ref) {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Test
    void a_transcript_by_pointer_is_read_and_redacted_as_its_segments_text() {
        MapBlobs blobs = new MapBlobs();
        ObjectNode transcript = Json.obj().put("version", 1).put("language", "en");
        transcript.putArray("segments")
                .add(Json.obj().put("start", 0.0).put("end", 3.0).put("text", " Recap of the checkout project."))
                .add(Json.obj().put("start", 3.0).put("end", 6.0).put("text", " Mail SYNTH-PII-0102@example.com."));
        URI ref = blobs.put(Json.bytes(transcript), "retention/raw-transcript/t.json").toCompletableFuture().join();
        RedactionHandler handler = new RedactionHandler(LayeredRedactor.reference(),
                new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8)), blobs);
        ObjectNode request = Json.obj();
        request.set(RedactionContract.INPUT, ClaimCheck.pointer(ref));
        request.putArray(RedactionContract.TYPES);

        JsonNode result = handler.execute(new TaskContext("/do/2/redact", "i-1", "c-1", 1, request,
                new WorkflowRef("examples", "pm", "1.0.0"), "chn-1", Lineage.NONE)).toCompletableFuture().join();

        assertThat(RedactionContract.violations(result)).isEmpty();
        assertThat(result.path("output").asText()).startsWith("Recap of the checkout project.\nMail EMAIL_")
                .doesNotContain("SYNTH-PII");
        assertThat(result.path("counts").path("email").asInt()).isEqualTo(1);
        assertThat(result.path("doubt").asBoolean()).isFalse();
    }
}
