package io.stintflow.inmemory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

import io.stintflow.spi.AuditLog;

/** {@link AuditLog} in memory, for tests and local runs (SDD 3a.5, RF8). */
public final class InMemoryAuditLog implements AuditLog {

    private final List<AuditRecord> records = new CopyOnWriteArrayList<>();

    @Override
    public CompletionStage<Void> append(AuditRecord record) {
        records.add(record);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletionStage<List<AuditRecord>> of(String instanceId) {
        List<AuditRecord> mine = new ArrayList<>();
        for (AuditRecord record : records) {
            if (record.instanceId().equals(instanceId)) {
                mine.add(record);
            }
        }
        return CompletableFuture.completedFuture(mine);
    }

    /** Every record, oldest first. */
    public List<AuditRecord> all() {
        return List.copyOf(records);
    }
}
