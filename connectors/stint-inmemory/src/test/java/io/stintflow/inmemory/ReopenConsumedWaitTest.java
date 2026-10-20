package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;

/**
 * SDD 3a.10: a save that consumes a wait key and adds a wait under the same key (a listen that, in the same
 * activation, waits again for the event it just consumed) must succeed and leave the new wait in place.
 */
class ReopenConsumedWaitTest {

    private static final WorkflowRef REF = new WorkflowRef("test", "reopen", "1.0.0");

    @Test
    void a_save_that_consumes_and_reopens_the_same_key_is_ok() throws Exception {
        InMemoryStateStore store = new InMemoryStateStore();
        String key = "event:sdd.done:id:abc";
        InstanceSnapshot first = snapshot(1);
        store.save(first, 0, List.of(new Wait(key, "inst-1", "/do/0/listen", Instant.now())), List.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        InstanceSnapshot second = snapshot(2);
        SaveOutcome outcome = store.save(second, 1,
                List.of(new Wait(key, "inst-1", "/do/1/listen", Instant.now())), List.of(key))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(outcome).isEqualTo(SaveOutcome.OK);
        assertThat(store.findWait(key).toCompletableFuture().get().orElseThrow().position())
                .isEqualTo("/do/1/listen");
    }

    @Test
    void reopening_a_key_that_is_gone_is_a_conflict() throws Exception {
        InMemoryStateStore store = new InMemoryStateStore();
        String key = "event:sdd.done:id:abc";
        store.save(snapshot(1), 0, List.of(), List.of()).toCompletableFuture().get(5, TimeUnit.SECONDS);

        SaveOutcome outcome = store.save(snapshot(2), 1,
                List.of(new Wait(key, "inst-1", "/do/1/listen", Instant.now())), List.of(key))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(outcome).isEqualTo(SaveOutcome.CONFLICT);
        assertThat(store.findWait(key).toCompletableFuture().get()).isEmpty();
    }

    @Test
    void reopening_a_key_held_by_another_instance_is_a_conflict() throws Exception {
        InMemoryStateStore store = new InMemoryStateStore();
        String key = "event:sdd.done:id:abc";
        store.save(new InstanceSnapshot("inst-other", REF, "/do/0/listen", null, Json.obj(), InstanceStatus.WAITING,
                1, null, Instant.now(), null, null), 0,
                List.of(new Wait(key, "inst-other", "/do/0/listen", Instant.now())), List.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
        store.save(snapshot(1), 0, List.of(), List.of()).toCompletableFuture().get(5, TimeUnit.SECONDS);

        SaveOutcome outcome = store.save(snapshot(2), 1,
                List.of(new Wait(key, "inst-1", "/do/1/listen", Instant.now())), List.of(key))
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(outcome).isEqualTo(SaveOutcome.CONFLICT);
        assertThat(store.findWait(key).toCompletableFuture().get().orElseThrow().instanceId()).isEqualTo("inst-other");
    }

    /** CA3: two activations racing to consume-and-reopen the same key from the same version — exactly one wins. */
    @Test
    void two_threads_racing_to_reopen_the_same_key_only_one_wins() throws Exception {
        InMemoryStateStore store = new InMemoryStateStore();
        String key = "event:sdd.done:id:abc";
        store.save(snapshot(1), 0, List.of(new Wait(key, "inst-1", "/do/0/listen", Instant.now())), List.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger conflict = new AtomicInteger();
        List<CompletableFuture<Void>> threads = List.of("A", "B").stream().map(name -> CompletableFuture.runAsync(() -> {
            try {
                barrier.await(5, TimeUnit.SECONDS);
                SaveOutcome outcome = store.save(snapshot(2), 1,
                        List.of(new Wait(key, "inst-1", "/do/1/listen-" + name, Instant.now())), List.of(key))
                        .toCompletableFuture().get(5, TimeUnit.SECONDS);
                (outcome == SaveOutcome.OK ? ok : conflict).incrementAndGet();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        })).toList();
        for (CompletableFuture<Void> t : threads) {
            t.get(10, TimeUnit.SECONDS);
        }

        assertThat(ok.get()).isEqualTo(1);
        assertThat(conflict.get()).isEqualTo(1);
        assertThat(store.findWait(key).toCompletableFuture().get()).isPresent();
    }

    private static InstanceSnapshot snapshot(long version) {
        return new InstanceSnapshot("inst-1", REF, "/do/0/listen", null, Json.obj(), InstanceStatus.WAITING,
                version, null, Instant.now(), null, null);
    }
}
