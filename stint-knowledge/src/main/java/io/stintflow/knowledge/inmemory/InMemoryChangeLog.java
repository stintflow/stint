package io.stintflow.knowledge.inmemory;

import java.util.ArrayList;
import java.util.List;

import io.stintflow.knowledge.ChangeEntry;

/**
 * The append-only change log of one scope (SDD 3b.1, 4.5, 4.9): entries are only ever added, with the next sequence number;
 * nothing here modifies or removes one. Copied and published whole with the rest of the scope's state.
 */
final class InMemoryChangeLog {

    private final List<ChangeEntry> entries;

    InMemoryChangeLog() {
        this.entries = new ArrayList<>();
    }

    private InMemoryChangeLog(List<ChangeEntry> entries) {
        this.entries = new ArrayList<>(entries);
    }

    InMemoryChangeLog copy() {
        return new InMemoryChangeLog(entries);
    }

    /** The sequence the next entry gets. */
    long nextSequence() {
        return entries.size() + 1L;
    }

    long lastSequence() {
        return entries.size();
    }

    void append(ChangeEntry entry) {
        if (entry.sequence() != nextSequence()) {
            throw new IllegalStateException("The change log has no gap: expected " + nextSequence() + ", got "
                    + entry.sequence());
        }
        entries.add(entry);
    }

    List<ChangeEntry> after(long sequence, int limit) {
        int from = (int) Math.max(0, Math.min(sequence, entries.size()));
        return List.copyOf(entries.subList(from, Math.min(entries.size(), from + Math.max(0, limit))));
    }

    List<ChangeEntry> all() {
        return List.copyOf(entries);
    }
}
