package io.stintflow.worker.knowledge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.stintflow.spi.AuditLog;
import io.stintflow.spi.AuditLog.AuditRecord;

/**
 * SDD 3c.5, 4.4: what the audit logs hold of one execution of a scenario that crosses domains — each domain keeps its own log, and
 * each step is an instance. The trail is built from the logs and the instances the caller names, oldest first; it says which kinds
 * of step appear, so that "was anything left out?" is a question with an answer. It holds the records as they are (ids, levels,
 * counts — never a value of the data).
 */
public final class AuditTrail {

    private final List<AuditRecord> records;

    private AuditTrail(List<AuditRecord> records) {
        this.records = List.copyOf(records);
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<AuditRecord> records() {
        return records;
    }

    /** The kinds that appear, in the order they first do. */
    public Set<String> kinds() {
        Set<String> kinds = new LinkedHashSet<>();
        records.forEach(r -> kinds.add(r.kind()));
        return kinds;
    }

    /** Whether every one of {@code required} appears at least once. */
    public boolean covers(Set<String> required) {
        return kinds().containsAll(required);
    }

    public static final class Builder {

        private final List<AuditRecord> collected = new ArrayList<>();

        /** The records of {@code instanceIds} in {@code log}. */
        public Builder add(AuditLog log, String... instanceIds) {
            for (String id : instanceIds) {
                collected.addAll(log.of(id).toCompletableFuture().join());
            }
            return this;
        }

        public AuditTrail build() {
            List<AuditRecord> sorted = new ArrayList<>(collected);
            sorted.sort(Comparator.comparing(AuditRecord::at));
            return new AuditTrail(sorted);
        }
    }
}
