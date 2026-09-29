package io.stintflow.spi;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Port: durable checkpoint storage for suspended workflow instances (SDD 1.2).
 * <p>
 * {@link #save} is the only mutation: it conditionally persists {@code snapshot} (by
 * {@code expectedVersion} — {@code 0} means "must not exist yet") and, in the same atomic act,
 * adds each {@link Wait} in {@code addWaits} and removes each key in {@code consumeWaitKeys}. A
 * {@code consumeWaitKeys} entry that no longer exists (already claimed by someone else) fails the
 * whole call with {@link SaveOutcome#CONFLICT} — this is how "reivindicar" a wait and persisting the
 * result of having done so are the same write, closing the crash-between-claim-and-save gap a
 * separate atomic-claim-then-separate-save choreography would have.
 * <p>
 * SDD 2.2: the same atomic act also records the facts the activation emitted ({@code addOutbox}), so
 * a fact exists if and only if the state that produced it was saved. An {@code addOutbox} entry whose
 * {@code eventId} already exists fails the whole call with {@link SaveOutcome#CONFLICT}, like
 * {@code addWaits}.
 */
public interface StateStore {

    CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                       List<Wait> addWaits, List<String> consumeWaitKeys,
                                       List<OutboxEntry> addOutbox);

    /** A save that emits no fact. */
    default CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                               List<Wait> addWaits, List<String> consumeWaitKeys) {
        return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, List.of());
    }

    CompletionStage<Optional<InstanceSnapshot>> load(String instanceId);

    /**
     * Strongly-consistent, read-only lookup (RNF1) — does NOT claim the wait. Tells the caller which
     * instance (and at which position) is waiting on {@code waitKey}, so it can {@link #load} and
     * decide what to save.
     */
    CompletionStage<Optional<Wait>> findWait(String waitKey);

    CompletionStage<Void> delete(String instanceId);

    /**
     * SDD 2.2, sec. 8c: facts still waiting to be published, recorded at or before
     * {@code createdAtOrBefore}, oldest first (then by {@link OutboxEntry#seq()}), at most {@code limit}.
     * Must not scan everything ever emitted — only what is pending.
     */
    CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit);

    /** SDD 2.2: removes a published fact from the outbox. Idempotent — removing a missing entry is a no-op. */
    CompletionStage<Void> removeOutbox(String eventId);
}
