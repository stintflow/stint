package io.stintflow.inmemory;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;

/**
 * {@link StateStore} backed by two {@link ConcurrentHashMap}s, with a per-instance {@link ReentrantLock}
 * around {@link #save} so the version check and the add/consume of waits are one atomic step —
 * mirroring what {@code TransactWriteItems} gives the Dynamo connector (SDD 1.2, RF4). The outbox
 * (SDD 2.2) is a third map, written under the same lock as the snapshot it belongs to. The early-event
 * inbox (SDD 2.3) is a fourth; a save removes the entries it consumed under that lock too.
 */
public final class InMemoryStateStore implements StateStore {

    private final Map<String, InstanceSnapshot> instances = new ConcurrentHashMap<>();
    private final Map<String, Wait> waits = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final Map<String, OutboxEntry> outbox = new ConcurrentHashMap<>();
    private final Map<InboxEntry.Key, InboxEntry> inbox = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                              List<Wait> addWaits, List<String> consumeWaitKeys,
                                              List<OutboxEntry> addOutbox) {
        return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                              List<Wait> addWaits, List<String> consumeWaitKeys,
                                              List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
        ReentrantLock lock = locks.computeIfAbsent(snapshot.instanceId(), id -> new ReentrantLock());
        lock.lock();
        try {
            InstanceSnapshot current = instances.get(snapshot.instanceId());
            long currentVersion = current == null ? 0 : current.version();
            if (currentVersion != expectedVersion) {
                return CompletableFuture.completedFuture(SaveOutcome.CONFLICT);
            }
            for (Wait toAdd : addWaits) {
                if (waits.containsKey(toAdd.waitKey())) {
                    return CompletableFuture.completedFuture(SaveOutcome.CONFLICT);
                }
            }
            for (String key : consumeWaitKeys) {
                if (!waits.containsKey(key)) {
                    return CompletableFuture.completedFuture(SaveOutcome.CONFLICT);
                }
            }
            for (OutboxEntry entry : addOutbox) {
                if (outbox.containsKey(entry.eventId())) {
                    return CompletableFuture.completedFuture(SaveOutcome.CONFLICT);
                }
            }

            for (String key : consumeWaitKeys) {
                waits.remove(key);
            }
            for (Wait toAdd : addWaits) {
                waits.put(toAdd.waitKey(), toAdd);
            }
            for (OutboxEntry entry : addOutbox) {
                outbox.put(entry.eventId(), entry);
            }
            for (InboxEntry.Key key : removeInbox) {
                inbox.remove(key);
            }
            instances.put(snapshot.instanceId(), snapshot);
            return CompletableFuture.completedFuture(SaveOutcome.OK);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        return CompletableFuture.completedFuture(Optional.ofNullable(instances.get(instanceId)));
    }

    @Override
    public CompletionStage<Optional<Wait>> findWait(String waitKey) {
        return CompletableFuture.completedFuture(Optional.ofNullable(waits.get(waitKey)));
    }

    @Override
    public CompletionStage<Void> delete(String instanceId) {
        instances.remove(instanceId);
        locks.remove(instanceId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
        List<OutboxEntry> pending = outbox.values().stream()
                .filter(e -> !e.createdAt().isAfter(createdAtOrBefore))
                .sorted(Comparator.comparing(OutboxEntry::createdAt).thenComparingInt(OutboxEntry::seq)
                        .thenComparing(OutboxEntry::eventId))
                .limit(limit)
                .toList();
        return CompletableFuture.completedFuture(pending);
    }

    @Override
    public CompletionStage<Void> removeOutbox(String eventId) {
        outbox.remove(eventId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Void> putInbox(InboxEntry entry) {
        inbox.put(entry.key(), entry);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
        return CompletableFuture.completedFuture(inbox.values().stream()
                .filter(e -> e.waitKey().equals(waitKey))
                .sorted(Comparator.comparing(InboxEntry::eventId))
                .toList());
    }

    @Override
    public CompletionStage<Void> removeInbox(InboxEntry.Key key) {
        inbox.remove(key);
        return CompletableFuture.completedFuture(null);
    }
}
