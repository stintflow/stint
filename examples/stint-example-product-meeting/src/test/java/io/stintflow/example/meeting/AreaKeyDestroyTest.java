package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAreaKeys;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AreaKeyDestroyedException;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.EnvelopeBlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.wire.Json;

/**
 * SDD 3b.5b, CA8: the key of an area is destroyed through the definition {@code area-key-destroy}, started by a fact. What the
 * blobs of the area held can no longer be read — the bytes kept do not change — and another area is not touched. Synthetic data
 * only.
 */
class AreaKeyDestroyTest {

    private static final String DESTROYED = "io.stintflow.knowledge.area-key-destroyed.v1";
    private static final String TEXT = "SYNTH-PII-0901 owns the rollback plan";

    @TempDir
    Path work;

    private static CloudEvent request(String id, String area) {
        ObjectNode data = Json.obj().put("requestId", id).put("area", area).put("label", "confidential");
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("https://dpo-channel.example"))
                .withType(KnowledgeIngestPipeline.AREA_ERASURE_REQUESTED).withSubject(id)
                .withDataContentType("application/json").withData(Json.bytes(data)).build();
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private KnowledgeIngestPipeline pipeline(FilesystemBlobStore raw, InMemoryAreaKeys keys) throws Exception {
        return new KnowledgeIngestPipeline(EnvelopeBlobStore.forArea(raw, keys, "examples"),
                new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()), InstantSource.system(),
                new InMemoryTimerService(), true, null, keys);
    }

    @Test
    void the_fact_destroys_the_key_of_the_area_and_what_it_encrypted_is_gone_for_good() throws Exception {
        FilesystemBlobStore raw = new FilesystemBlobStore(work);
        InMemoryAreaKeys keys = new InMemoryAreaKeys();
        EnvelopeBlobStore examples = EnvelopeBlobStore.forArea(raw, keys, "examples");
        EnvelopeBlobStore other = EnvelopeBlobStore.forArea(raw, keys, "other");
        URI minutes = examples.put(bytes(TEXT), "minutes/m-9.md", new RetentionClass("minutes", Duration.ofDays(90)))
                .toCompletableFuture().join();
        URI kept = other.put(bytes("another area"), "minutes/o-1.md").toCompletableFuture().join();
        byte[] stored = raw.get(minutes).toCompletableFuture().join();
        try (KnowledgeIngestPipeline ingest = pipeline(raw, keys)) {
            InstanceSnapshot done = ingest.destroyAreaKey(request("ar-1", "examples"), Duration.ofSeconds(60));

            assertThat(done.status()).as("at %s", done.position()).isEqualTo(InstanceStatus.COMPLETED);
            CloudEvent fact = ingest.emitted().stream().filter(e -> e.getType().equals(DESTROYED)).findFirst().orElseThrow();
            JsonNode data = Json.read(fact.getData().toBytes());
            assertThat(data.path("area").asText()).isEqualTo("examples");
            assertThat(data.path("state").asText()).isEqualTo("DESTROYED");
            assertThat(data.toString()).doesNotContain("SYNTH-PII");

            assertThatThrownBy(() -> examples.get(minutes).toCompletableFuture().join())
                    .satisfies(e -> assertThat(e instanceof CompletionException ? e.getCause() : e)
                            .isInstanceOf(AreaKeyDestroyedException.class));
            assertThat(raw.get(minutes).toCompletableFuture().join()).isEqualTo(stored);   // the key went, not the bytes
            assertThat(other.get(kept).toCompletableFuture().join()).isEqualTo(bytes("another area"));
            assertThat(keys.state("other").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ACTIVE);
            assertThat(ingest.audit.all()).anySatisfy(r -> {
                assertThat(r.kind()).isEqualTo("area-key-destroy");
                assertThat(r.detail()).containsEntry("area", "examples").containsEntry("state", "DESTROYED");
            });

            // asked again, as another fact: the same end, and nothing else changes
            InstanceSnapshot again = ingest.destroyAreaKey(request("ar-2", "examples"), Duration.ofSeconds(60));
            assertThat(again.status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(keys.state("examples").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
        }
    }

    @Test
    void an_area_the_worker_does_not_serve_is_refused_and_its_key_stays() throws Exception {
        FilesystemBlobStore raw = new FilesystemBlobStore(work);
        InMemoryAreaKeys keys = new InMemoryAreaKeys();
        EnvelopeBlobStore other = EnvelopeBlobStore.forArea(raw, keys, "other");
        URI kept = other.put(bytes("another area"), "minutes/o-1.md").toCompletableFuture().join();
        try (KnowledgeIngestPipeline ingest = pipeline(raw, keys)) {
            InstanceSnapshot refused = ingest.destroyAreaKey(request("ar-3", "other"), Duration.ofSeconds(30));

            assertThat(refused.status()).isEqualTo(InstanceStatus.FAILED);
            assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(DESTROYED))).isEmpty();
            assertThat(keys.state("other").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ACTIVE);
            assertThat(other.get(kept).toCompletableFuture().join()).isEqualTo(bytes("another area"));
        }
    }
}
