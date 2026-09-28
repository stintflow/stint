package io.stintflow.inmemory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;

/**
 * {@link StateStore} backed by two {@link ConcurrentHashMap}s, with a per-instance {@link ReentrantLock}
 * around {@link #save} so the version check and the add/consume of waits are one atomic step —
 * mirroring what {@code TransactWriteItems} gives the Dynamo connector (SDD 1.2, RF4).
 */
public final class InMemoryStateStore implements StateStore {

    private final Map<String, InstanceSnapshot> instances = new ConcurrentHashMap<>();
    private final Map<String, Wait> waits = new ConcurrentHashMap<>();
    private final Map<String, ReentrantLock> locks = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                              List<Wait> addWaits, List<String> consumeWaitKeys) {
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

            for (String key : consumeWaitKeys) {
                waits.remove(key);
            }
            for (Wait toAdd : addWaits) {
                waits.put(toAdd.waitKey(), toAdd);
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
}
