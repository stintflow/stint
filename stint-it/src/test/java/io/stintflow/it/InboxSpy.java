package io.stintflow.it;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;

/** A {@link StateStore} that keeps every early event put in the inbox, for scanners (SDD 3a.4, 4.6). */
final class InboxSpy implements StateStore {

    final List<InboxEntry> kept = new CopyOnWriteArrayList<>();
    private final StateStore delegate;

    InboxSpy(StateStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
        return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox);
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
        return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox);
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
        kept.add(entry);
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
