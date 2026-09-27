package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;

/**
 * CA2 (SDD 1.2): two threads racing to claim-and-save the same wait — only one wins, the other gets
 * {@link SaveOutcome#CONFLICT}. Synchronized with a {@link CyclicBarrier}, never a sleep — both
 * threads are released at the exact same instant, so this proves the race is resolved atomically by
 * {@link InMemoryStateStore#save}, not by lucky scheduling.
 */
class InMemoryStateStoreTest {

    private static final WorkflowRef REF = new WorkflowRef("test", "concurrency", "1.0.0");

    @Test
    void two_threads_racing_to_consume_the_same_wait_only_one_wins() throws Exception {
        InMemoryStateStore store = new InMemoryStateStore();
        String instanceId = "inst-1";
        String waitKey = "task:corr-1";
        ObjectNode context = Json.obj();

        InstanceSnapshot initial = new InstanceSnapshot(instanceId, REF, "/do/0/echo", waitKey, context,
                InstanceStatus.WAITING, 1, null, Instant.now());
        store.save(initial, 0, List.of(new Wait(waitKey, instanceId, "/do/0/echo", Instant.now())), List.of())
                .toCompletableFuture().get(5, TimeUnit.SECONDS);

        int threadCount = 2;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicReference<SaveOutcome> outcomeA = new AtomicReference<>();
        AtomicReference<SaveOutcome> outcomeB = new AtomicReference<>();

        CompletableFuture<Void> threadA = CompletableFuture.runAsync(() -> raceAttempt(store, instanceId, waitKey, barrier, outcomeA));
        CompletableFuture<Void> threadB = CompletableFuture.runAsync(() -> raceAttempt(store, instanceId, waitKey, barrier, outcomeB));

        CompletableFuture.allOf(threadA, threadB).get(5, TimeUnit.SECONDS);

        List<SaveOutcome> outcomes = List.of(outcomeA.get(), outcomeB.get());
        assertThat(outcomes).containsExactlyInAnyOrder(SaveOutcome.OK, SaveOutcome.CONFLICT);

        assertThat(store.findWait(waitKey).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        InstanceSnapshot finalSnap = store.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(finalSnap.version()).isEqualTo(2); // exactly one winner advanced the version once
    }

    private static void raceAttempt(InMemoryStateStore store, String instanceId, String waitKey,
            CyclicBarrier barrier, AtomicReference<SaveOutcome> result) {
        try {
            barrier.await(5, TimeUnit.SECONDS); // both threads start the save at the same instant
            InstanceSnapshot next = new InstanceSnapshot(instanceId, REF, "/do/1/after", null, Json.obj(),
                    InstanceStatus.COMPLETED, 2, null, Instant.now());
            SaveOutcome outcome = store.save(next, 1, List.of(), List.of(waitKey))
                    .toCompletableFuture().get(5, TimeUnit.SECONDS);
            result.set(outcome);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
