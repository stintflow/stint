package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    private static InstanceSnapshot snapshot(long version) {
        return new InstanceSnapshot("inst-1", REF, "/do/0/listen", null, Json.obj(), InstanceStatus.WAITING,
                version, null, Instant.now(), null, null);
    }
}
