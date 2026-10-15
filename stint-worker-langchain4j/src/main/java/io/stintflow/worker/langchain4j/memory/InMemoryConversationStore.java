package io.stintflow.worker.langchain4j.memory;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/** A {@link ConversationStore} in a map, for tests and local runs. Expiry is checked on read, against the clock. */
public final class InMemoryConversationStore implements ConversationStore {

    static final Comparator<Exchange> ORDER =
            Comparator.comparing(Exchange::committedAt).thenComparing(Exchange::correlationId);

    private final Map<MemoryKey, List<Exchange>> byMemory = new HashMap<>();
    private final InstantSource clock;

    public InMemoryConversationStore() {
        this(InstantSource.system());
    }

    public InMemoryConversationStore(InstantSource clock) {
        this.clock = clock;
    }

    @Override
    public synchronized CompletionStage<List<Exchange>> committed(MemoryKey memory) {
        List<Exchange> live = new ArrayList<>();
        for (Exchange exchange : byMemory.getOrDefault(memory, List.of())) {
            if (exchange.expiresAt() == null || exchange.expiresAt().isAfter(clock.instant())) {
                live.add(exchange);
            }
        }
        live.sort(ORDER);
        return CompletableFuture.completedFuture(live);
    }

    @Override
    public synchronized CompletionStage<CommitOutcome> commit(Exchange exchange) {
        boolean known = byMemory.values().stream().flatMap(List::stream)
                .anyMatch(e -> e.memory().instanceId().equals(exchange.memory().instanceId())
                        && e.correlationId().equals(exchange.correlationId()));
        if (known) {
            return CompletableFuture.completedFuture(CommitOutcome.ALREADY_COMMITTED);
        }
        byMemory.computeIfAbsent(exchange.memory(), k -> new ArrayList<>()).add(exchange);
        return CompletableFuture.completedFuture(CommitOutcome.COMMITTED);
    }

    @Override
    public synchronized CompletionStage<List<Exchange>> exchangesOf(String instanceId) {
        List<Exchange> all = new ArrayList<>();
        byMemory.forEach((memory, exchanges) -> {
            if (memory.instanceId().equals(instanceId)) {
                all.addAll(exchanges);
            }
        });
        all.sort(ORDER);
        return CompletableFuture.completedFuture(all);
    }

    @Override
    public synchronized CompletionStage<Void> deleteInstance(String instanceId) {
        byMemory.keySet().removeIf(memory -> memory.instanceId().equals(instanceId));
        return CompletableFuture.completedFuture(null);
    }
}
