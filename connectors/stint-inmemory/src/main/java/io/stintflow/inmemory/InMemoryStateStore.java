package io.stintflow.inmemory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.StateStore;

/** ConcurrentHashMap-backed state store with a correlation-id index, mirroring a DynamoDB GSI. */
public final class InMemoryStateStore implements StateStore {

    private final Map<String, InstanceSnapshot> instances = new ConcurrentHashMap<>();
    private final Map<String, String> correlationIndex = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<Void> save(InstanceSnapshot snapshot) {
        instances.put(snapshot.instanceId(), snapshot);
        if (snapshot.waitingForCorrelationId() != null) {
            correlationIndex.put(snapshot.waitingForCorrelationId(), snapshot.instanceId());
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        return CompletableFuture.completedFuture(Optional.ofNullable(instances.get(instanceId)));
    }

    @Override
    public CompletionStage<Void> delete(String instanceId) {
        instances.remove(instanceId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<Optional<String>> instanceWaitingFor(String correlationId) {
        return CompletableFuture.completedFuture(Optional.ofNullable(correlationIndex.get(correlationId)));
    }
}
