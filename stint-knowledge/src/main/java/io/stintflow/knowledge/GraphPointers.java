package io.stintflow.knowledge;

import java.util.List;
import java.util.OptionalLong;

import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.knowledge.ChangeEntry.Op;

/**
 * SDD 3c.1, 4.2: the version of a node, from the change log. It is the sequence of the last entry about the entity — an add, an
 * update, or the one that records a new document holding it — so it moves when the node changes or gains evidence, and it does
 * <em>not</em> move for a reindexation (one entry for the whole base), nor for a change of another node. A node whose last entry
 * is a removal or an erasure has no version: it is gone.
 */
public final class GraphPointers {

    private GraphPointers() {
    }

    /** The version of {@code nodeId} in {@code log} (in order of sequence), or empty if it never was there or is gone. */
    public static OptionalLong versionOf(List<ChangeEntry> log, String nodeId) {
        ChangeEntry last = null;
        for (ChangeEntry entry : log) {
            if (entry.kind() == Kind.ENTITY && entry.targetId().equals(nodeId) && (last == null || entry.sequence() > last.sequence())) {
                last = entry;
            }
        }
        if (last == null || last.op() == Op.REMOVE || last.op() == Op.ERASE) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(last.sequence());
    }
}
