package io.stintflow.knowledge.inspector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.spi.Sensitivity;

/**
 * What the change log says exists (SDD 3b.6, 4.2): the log replayed entry by entry. An {@code ADD} or {@code UPDATE} makes a thing
 * alive; a {@code REMOVE}, an {@code EXPIRE} or an {@code ERASE} takes it out; a {@code REINDEX} changes no structure. The log
 * holds ids, kinds, levels and who wrote — never a name — so neither does this.
 * <p>
 * The same replay serves the snapshot at a past point (stop early) and the metrics (keep going, with a watermark).
 */
final class LogState {

    /** One thing that was alive, with the entry that added it and, if it is gone, when and how. */
    record Element(Kind kind, String id, String type, Sensitivity level, long addedSequence, Instant addedAt,
            String instanceId, String chainId, String definition, String schemaVersion, String documentId,
            Instant removedAt, ChangeEntry.Op removedBy) {

        Element removed(Instant at, ChangeEntry.Op by) {
            return new Element(kind, id, type, level, addedSequence, addedAt, instanceId, chainId, definition, schemaVersion,
                    documentId, at, by);
        }
    }

    private final Map<Kind, Map<String, Element>> alive = new LinkedHashMap<>();
    private final List<Element> gone = new ArrayList<>();
    private final Set<String> erased = new LinkedHashSet<>();
    private long sequence;
    private Instant lastAt;
    private int erasures;
    private int expirations;
    private int removals;
    private int reindexes;

    LogState() {
        for (Kind kind : Kind.values()) {
            alive.put(kind, new LinkedHashMap<>());
        }
    }

    void apply(ChangeEntry e) {
        sequence = e.sequence();
        lastAt = e.at();
        Map<String, Element> ofKind = alive.get(e.kind());
        switch (e.op()) {
            case ADD, UPDATE -> {
                Element before = ofKind.get(e.targetId());
                if (before == null) {
                    ofKind.put(e.targetId(), new Element(e.kind(), e.targetId(), e.elementType(), e.sensitivity(), e.sequence(),
                            e.at(), e.instanceId(), e.chainId(), e.definition(), e.schemaVersion(), e.documentId(), null, null));
                } else if (e.sensitivity() != before.level()) {
                    ofKind.put(e.targetId(), new Element(before.kind(), before.id(), before.type(), e.sensitivity(),
                            before.addedSequence(), before.addedAt(), before.instanceId(), before.chainId(), before.definition(),
                            before.schemaVersion(), before.documentId(), null, null));
                }
            }
            case REMOVE, EXPIRE, ERASE -> {
                Element was = ofKind.remove(e.targetId());
                if (was != null) {
                    gone.add(was.removed(e.at(), e.op()));
                }
                switch (e.op()) {
                    case ERASE -> {
                        erasures++;
                        erased.add(e.targetId());
                    }
                    case EXPIRE -> expirations++;
                    default -> removals++;
                }
            }
            case REINDEX -> reindexes++;
        }
    }

    long sequence() {
        return sequence;
    }

    Instant lastAt() {
        return lastAt;
    }

    Map<String, Element> alive(Kind kind) {
        return alive.get(kind);
    }

    /** Everything that was ever alive and is gone. */
    List<Element> gone() {
        return gone;
    }

    /** Ids an erasure took out (opaque: the log never held what they stood for). */
    Set<String> erased() {
        return erased;
    }

    int erasures() {
        return erasures;
    }

    int expirations() {
        return expirations;
    }

    int removals() {
        return removals;
    }

    int reindexes() {
        return reindexes;
    }
}
