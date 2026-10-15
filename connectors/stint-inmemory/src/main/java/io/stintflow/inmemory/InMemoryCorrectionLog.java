package io.stintflow.inmemory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import io.stintflow.spi.CorrectionLog;
import io.stintflow.spi.WorkflowRef;

/** {@link CorrectionLog} in memory, for tests and local runs (SDD 3a.4, RF7). */
public final class InMemoryCorrectionLog implements CorrectionLog {

    private final List<Correction> corrections = new CopyOnWriteArrayList<>();

    @Override
    public CompletionStage<Void> append(Correction correction) {
        corrections.add(correction);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<Correction>> of(WorkflowRef definition) {
        return CompletableFuture.completedFuture(
                corrections.stream().filter(c -> c.definition().equals(definition)).toList());
    }
}
