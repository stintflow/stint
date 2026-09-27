package io.stintflow.spi;

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
 */
public interface StateStore {

    CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                       List<Wait> addWaits, List<String> consumeWaitKeys);

    CompletionStage<Optional<InstanceSnapshot>> load(String instanceId);

    /**
     * Strongly-consistent, read-only lookup (RNF1) — does NOT claim the wait. Tells the caller which
     * instance (and at which position) is waiting on {@code waitKey}, so it can {@link #load} and
     * decide what to save.
     */
    CompletionStage<Optional<Wait>> findWait(String waitKey);

    CompletionStage<Void> delete(String instanceId);
}
