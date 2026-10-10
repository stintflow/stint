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
 * <p>
 * SDD 2.3: a short-lived inbox of domain events that arrived before the {@code listen} waiting for them
 * ({@link #putInbox}, {@link #findInbox}); the save that consumes one removes it in the same act
 * ({@code removeInbox}). The inbox methods are defaults that throw, so an existing implementation keeps
 * compiling and simply can't hold early events until it implements them.
 */
public interface StateStore {

    CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                       List<Wait> addWaits, List<String> consumeWaitKeys,
                                       List<OutboxEntry> addOutbox);

    /**
     * SDD 2.3: as {@link #save(InstanceSnapshot, long, List, List, List)}, also removing the inbox entries
     * the activation consumed. Removing a missing entry is not a conflict. The default removes them right
     * after an OK save — not atomic, which the engine tolerates (it remembers the events it consumed);
     * implementations should override it to remove them in the same atomic act.
     */
    default CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                               List<Wait> addWaits, List<String> consumeWaitKeys,
                                               List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
        CompletionStage<SaveOutcome> saved = save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox);
        if (removeInbox.isEmpty()) {
            return saved;
        }
        return saved.thenCompose(outcome -> {
            CompletionStage<Void> removed = java.util.concurrent.CompletableFuture.completedFuture(null);
            if (outcome == SaveOutcome.OK) {
                for (InboxEntry.Key key : removeInbox) {
                    removed = removed.thenCompose(v -> removeInbox(key));
                }
            }
            return removed.thenApply(v -> outcome);
        });
    }

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

    /** SDD 2.3: records an early event. Idempotent — the same (waitKey, eventId) again overwrites it. */
    default CompletionStage<Void> putInbox(InboxEntry entry) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " has no early-event inbox");
    }

    /**
     * SDD 2.3: every inbox entry under {@code waitKey}, expired ones included (the caller filters by
     * {@link InboxEntry#expiresAt()}). Strongly consistent, like {@link #findWait}: the early-event
     * protocol relies on a write being visible to the next read.
     */
    default CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " has no early-event inbox");
    }

    /** SDD 2.3: removes one inbox entry. Idempotent. */
    default CompletionStage<Void> removeInbox(InboxEntry.Key key) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " has no early-event inbox");
    }
}
