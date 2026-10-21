package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.ChangeEntry.Op;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.1: the in-memory composite (vector index, graph, change log) against the conformance suite, and what only it can show:
 * a failure before the state is published leaves no trace of the write, and no log entry is ever missing from a change.
 */
class InMemoryKnowledgeStoreTest extends KnowledgeStoreContract {

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-19T12:00:00Z"));
    private Path blobDir;
    private CompositeKnowledgeStore last;

    @BeforeEach
    void blobs() throws IOException {
        blobDir = Files.createTempDirectory("stint-3b1-blobs");
    }

    @Override
    protected KnowledgeStore newStore(boolean withRestrictedClass) {
        last = new CompositeKnowledgeStore(new DeterministicEmbedder(), new FilesystemBlobStore(blobDir, now::get),
                withRestrictedClass ? new RetentionClass("restricted", Duration.ofDays(30)) : null, now::get);
        return last;
    }

    @Override
    protected void advanceClock(Duration by) {
        now.set(now.get().plus(by));
    }

    @Override
    protected String heldOutsideBlobs(KnowledgeScope scope) {
        return last.dump(scope);
    }

    @Override
    protected String heldInBlobs() {
        try (Stream<Path> files = Files.walk(blobDir)) {
            return files.filter(Files::isRegularFile).map(p -> {
                try {
                    return new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }).reduce("", (a, b) -> a + "\n" + b);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    @Override
    protected int reapExpired(KnowledgeScope scope) {
        return last.reapExpired(scope);
    }

    @Override
    protected java.net.URI putBlob(String content) {
        return new FilesystemBlobStore(blobDir, now::get).put(content.getBytes(StandardCharsets.UTF_8), "origin-"
                + Math.abs(content.hashCode()), new RetentionClass("origin", Duration.ofDays(90))).toCompletableFuture().join();
    }

    @Override
    protected boolean blobExists(java.net.URI uri) {
        try {
            new FilesystemBlobStore(blobDir, now::get).get(uri).toCompletableFuture().join();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    // --- CA6: the change and its log entries happen together, or neither ------------------------------------------

    @Test
    void ca6_a_failure_before_the_state_is_published_leaves_no_change_no_log_entry_and_no_gap() throws Exception {
        KnowledgeStore store = newStore(true);
        B first = new B(SCOPE, "meeting:a1").passage("Kickoff notes for the rollout.", Sensitivity.INTERNAL)
                .entity("e-rollout", "Term", "Rollout", Sensitivity.INTERNAL).link(0, "e-rollout");
        write(store, SCOPE, first.build());
        List<ChangeEntry> logged = log(store, SCOPE);
        String heldBefore = heldOutsideBlobs(SCOPE);
        String blobsBefore = heldInBlobs();

        B second = new B(SCOPE, "meeting:a2")
                .passage("Budget talk with SYNTH-PII-0401 attending.", Sensitivity.RESTRICTED)
                .passage("Rollout moves to May.", Sensitivity.INTERNAL)
                .entity("e-rollout", "Term", "Rollout", Sensitivity.INTERNAL).link(1, "e-rollout");
        last.faultBeforeSwap(() -> {
            throw new IllegalStateException("simulated failure just before the swap");
        });
        assertThatThrownBy(() -> write(store, SCOPE, second.build())).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);

        assertThat(log(store, SCOPE)).isEqualTo(logged);
        assertThat(heldOutsideBlobs(SCOPE)).isEqualTo(heldBefore);
        assertThat(heldInBlobs()).isEqualTo(blobsBefore);   // the blob the failed write had created is gone again
        assertThat(retrieve(store, SCOPE, "rollout May budget", Sensitivity.RESTRICTED)).extracting(ContextItem::content)
                .noneMatch(c -> c.contains("May") || c.contains("SYNTH-PII-0401"));

        last.faultBeforeSwap(null);   // the same write, now that nothing fails
        assertThat(write(store, SCOPE, second.build()).status()).isEqualTo(WriteReceipt.Status.APPLIED);
        List<ChangeEntry> after = log(store, SCOPE);
        for (int i = 0; i < after.size(); i++) {
            assertThat(after.get(i).sequence()).isEqualTo(i + 1);   // no gap
        }
        assertThat(after.subList(0, logged.size())).isEqualTo(logged);   // the past was not touched
    }

    @Test
    void ca6_a_failure_before_an_erasure_is_published_erases_nothing_and_logs_nothing() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, new B(SCOPE, "meeting:a3").passage("Hiring plan for Q3.", Sensitivity.INTERNAL).build());
        List<ChangeEntry> logged = log(store, SCOPE);
        last.faultBeforeSwap(() -> {
            throw new IllegalStateException("simulated failure");
        });

        assertThatThrownBy(() -> erase(store, SCOPE, new DeleteRequest.ByDocument("req-x", "erasure:document", "meeting:a3")))
                .hasCauseInstanceOf(IllegalStateException.class);

        assertThat(log(store, SCOPE)).isEqualTo(logged);
        assertThat(retrieve(store, SCOPE, "hiring plan", Sensitivity.INTERNAL)).hasSize(1);
        last.faultBeforeSwap(null);
        assertThat(erase(store, SCOPE, new DeleteRequest.ByDocument("req-x", "erasure:document", "meeting:a3")).counts())
                .containsKey("ERASE.PASSAGE");   // the same request, retried, now takes effect
    }

    @Test
    void ca4_a_failure_during_a_reindexation_leaves_the_model_the_vectors_and_the_log_as_they_were() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:a4").passage("Capacity review for the data cluster.", Sensitivity.INTERNAL)
                .passage("SYNTH-PII-0701 raised the storage budget.", Sensitivity.RESTRICTED).build());
        List<ChangeEntry> logged = log(store, SCOPE);
        String held = heldOutsideBlobs(SCOPE);
        last.faultBeforeSwap(() -> {
            throw new IllegalStateException("simulated failure");
        });

        assertThatThrownBy(() -> reindex(store, SCOPE, "req-f", new OtherEmbedder(java.util.Set.of("en"))))
                .hasCauseInstanceOf(IllegalStateException.class);

        assertThat(store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS).model())
                .isEqualTo(new DeterministicEmbedder().model());
        assertThat(log(store, SCOPE)).isEqualTo(logged);
        assertThat(heldOutsideBlobs(SCOPE)).isEqualTo(held);
        assertThat(retrieve(store, SCOPE, "capacity cluster storage", Sensitivity.RESTRICTED)).hasSize(2);   // still the old model
        last.faultBeforeSwap(null);
        assertThat(reindex(store, SCOPE, "req-f", new OtherEmbedder(java.util.Set.of("en"))).status())
                .isEqualTo(ReindexReceipt.Status.APPLIED);   // the same request, retried, takes effect
    }

    @Test
    void ca4_a_reader_during_a_reindexation_sees_the_old_base_whole() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, new B(SCOPE, "meeting:a5").passage("Alpha topic one.", Sensitivity.INTERNAL).build());
        var started = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        last.faultBeforeSwap(() -> {
            started.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        var moving = java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                reindex(store, SCOPE, "req-r", new OtherEmbedder(java.util.Set.of("en")));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        assertThat(store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS).model())
                .isEqualTo(new DeterministicEmbedder().model());
        assertThat(retrieve(store, SCOPE, "alpha topic", Sensitivity.INTERNAL)).hasSize(1);
        release.countDown();
        moving.get(10, TimeUnit.SECONDS);
        assertThat(store.describe(SCOPE).toCompletableFuture().get(10, TimeUnit.SECONDS).model()).isEqualTo("other-bag-16");
        assertThat(retrieve(store, SCOPE, "alpha topic", Sensitivity.INTERNAL)).hasSize(1);
    }

    @Test
    void the_rest_of_the_store_is_readable_while_a_write_is_in_flight_and_sees_the_old_or_the_new_state() throws Exception {
        KnowledgeStore store = newStore(false);
        write(store, SCOPE, new B(SCOPE, "meeting:c1").passage("Alpha topic one.", Sensitivity.INTERNAL).build());
        var started = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        last.faultBeforeSwap(() -> {
            started.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        var writing = java.util.concurrent.CompletableFuture.runAsync(() -> {
            try {
                write(store, SCOPE, new B(SCOPE, "meeting:c2").passage("Alpha topic two.", Sensitivity.INTERNAL).build());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        // the write has changed its copy but not published it: a reader sees the old state whole, log included
        assertThat(retrieve(store, SCOPE, "alpha topic", Sensitivity.INTERNAL)).hasSize(1);
        assertThat(log(store, SCOPE)).hasSize(1);
        release.countDown();
        writing.get(10, TimeUnit.SECONDS);
        assertThat(retrieve(store, SCOPE, "alpha topic", Sensitivity.INTERNAL)).hasSize(2);
        assertThat(log(store, SCOPE)).hasSize(2);
    }

    @Test
    void an_embedder_of_another_model_is_refused_for_a_base_that_already_has_vectors() throws Exception {
        String[] model = {"model-one"};
        DeterministicEmbedder delegate = new DeterministicEmbedder();
        Embedder switching = new Embedder() {
            @Override
            public float[] embed(String text) {
                return delegate.embed(text);
            }

            @Override
            public int dimension() {
                return delegate.dimension();
            }

            @Override
            public String model() {
                return model[0];
            }

            @Override
            public java.util.Set<String> languages() {
                return delegate.languages();
            }
        };
        KnowledgeStore store = new CompositeKnowledgeStore(switching, new FilesystemBlobStore(blobDir, now::get), null,
                now::get);
        write(store, SCOPE, new B(SCOPE, "meeting:m1").passage("Some text.", Sensitivity.INTERNAL).build());

        model[0] = "model-two";

        assertThatThrownBy(() -> write(store, SCOPE, new B(SCOPE, "meeting:m2").passage("More.", Sensitivity.INTERNAL).build()))
                .hasCauseInstanceOf(KnowledgeValidationException.class).hasMessageContaining("reindex");
        assertThatThrownBy(() -> retrieve(store, SCOPE, "text", Sensitivity.INTERNAL))
                .hasCauseInstanceOf(KnowledgeValidationException.class);
    }

    @Test
    void reaping_logs_one_expire_entry_for_an_expired_entity_and_the_edges_and_links_that_went_with_it() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:x1").passage("Open item.", Sensitivity.INTERNAL)
                .entity("e-secret", "Person", "PERSON_zz", Sensitivity.RESTRICTED)
                .entity("e-pub", "Term", "Item", Sensitivity.RESTRICTED)
                .relation("mentions", "e-secret", "e-pub", Sensitivity.RESTRICTED).link(0, "e-secret").build());
        advanceClock(Duration.ofDays(40));

        assertThat(reapExpired(SCOPE)).isEqualTo(2);

        assertThat(log(store, SCOPE)).filteredOn(e -> e.op() == Op.EXPIRE).extracting(ChangeEntry::kind)
                .contains(ChangeEntry.Kind.ENTITY, ChangeEntry.Kind.RELATION, ChangeEntry.Kind.LINK);
        assertThat(heldOutsideBlobs(SCOPE)).doesNotContain("PERSON_zz");
    }
}
