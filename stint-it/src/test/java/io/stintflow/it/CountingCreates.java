package io.stintflow.it;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;

/**
 * Delegates to a store and counts instance creations (an OK save at version 0): the proof of "one instance".
 * An optional listener sees each created snapshot, so a test can wait for a given creation instead of polling.
 */
final class CountingCreates implements StateStore {

    private final StateStore delegate;
    private final AtomicInteger creates;
    private final Consumer<InstanceSnapshot> onCreate;

    CountingCreates(StateStore delegate, AtomicInteger creates) {
        this(delegate, creates, snapshot -> { });
    }

    CountingCreates(StateStore delegate, AtomicInteger creates, Consumer<InstanceSnapshot> onCreate) {
        this.delegate = delegate;
        this.creates = creates;
        this.onCreate = onCreate;
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
        return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
        return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox)
                .thenApply(outcome -> {
                    if (expectedVersion == 0 && outcome == SaveOutcome.OK) {
                        creates.incrementAndGet();
                        onCreate.accept(snapshot);
                    }
                    return outcome;
                });
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        return delegate.load(instanceId);
    }

    @Override
    public CompletionStage<Optional<Wait>> findWait(String waitKey) {
        return delegate.findWait(waitKey);
    }

    @Override
    public CompletionStage<Void> delete(String instanceId) {
        return delegate.delete(instanceId);
    }

    @Override
    public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
        return delegate.pendingOutbox(createdAtOrBefore, limit);
    }

    @Override
    public CompletionStage<Void> removeOutbox(String eventId) {
        return delegate.removeOutbox(eventId);
    }

    @Override
    public CompletionStage<Void> putInbox(InboxEntry entry) {
        return delegate.putInbox(entry);
    }

    @Override
    public CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
        return delegate.findInbox(waitKey);
    }

    @Override
    public CompletionStage<Void> removeInbox(InboxEntry.Key key) {
        return delegate.removeInbox(key);
    }
}
