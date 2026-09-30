package io.stintflow.inmemory;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;

/**
 * An {@link InMemoryStateStore} that can make two concurrent activations meet at a {@link CyclicBarrier}
 * (SDD 2.3 concurrency tests — barriers, never sleeps):
 * <ul>
 *   <li>{@link #holdLoads}: the next two {@code load}s wait for each other, so both activations read the
 *       same snapshot version before either saves;</li>
 *   <li>{@link #holdAfter}: the next two writes matching the predicates (e.g. an inbox put and a listen's
 *       save) wait for each other <em>after</em> being applied, so both sides write before either checks.</li>
 * </ul>
 */
final class BarrierStateStore implements StateStore {

    private final InMemoryStateStore delegate = new InMemoryStateStore();
    private final AtomicInteger loadsToHold = new AtomicInteger();
    private volatile CyclicBarrier barrier;
    private volatile Predicate<List<Wait>> holdSave = w -> false;
    private final AtomicInteger writesToHold = new AtomicInteger();
    private final java.util.concurrent.CompletableFuture<Void> inboxWritten = new java.util.concurrent.CompletableFuture<>();

    /** Completes once an inbox entry was written (before that writer waits at the barrier). */
    java.util.concurrent.CompletableFuture<Void> inboxWritten() {
        return inboxWritten;
    }

    void holdLoads() {
        barrier = new CyclicBarrier(2);
        loadsToHold.set(2);
    }

    /** Holds the next save whose added waits match {@code saveFilter}, and the next inbox put. */
    void holdAfter(Predicate<List<Wait>> saveFilter) {
        barrier = new CyclicBarrier(2);
        holdSave = saveFilter;
        writesToHold.set(2);
    }

    private void meet() {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("The other side never reached the barrier", e);
        }
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
        return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
        CompletionStage<SaveOutcome> saved = delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys,
                addOutbox, removeInbox);
        if (holdSave.test(addWaits) && writesToHold.getAndDecrement() > 0) {
            saved.toCompletableFuture().join();
            meet();
        }
        return saved;
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        CompletionStage<Optional<InstanceSnapshot>> loaded = delegate.load(instanceId);
        if (loadsToHold.getAndDecrement() > 0) {
            loaded.toCompletableFuture().join();
            meet();
        }
        return loaded;
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
        CompletionStage<Void> put = delegate.putInbox(entry);
        inboxWritten.complete(null);
        if (writesToHold.getAndDecrement() > 0) {
            meet();
        }
        return put;
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
