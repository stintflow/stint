package io.stintflow.knowledge;

import java.time.Instant;

import io.stintflow.spi.Sensitivity;

/**
 * One line of the append-only change log (SDD 3b.1, 4.5). Ids, kinds, levels and hashes — never a text, like the audit log
 * of SDD 3a.5. Written in the same atomic step as the change it records, and never modified afterwards; an erasure adds
 * entries and rewrites none (the ids are opaque, and mean nothing once the node and its pseudonym map are gone).
 *
 * @param sequence      strictly increasing within the scope, with no gap
 * @param at            when
 * @param batchId       the write (or the erasure request) that made the change
 * @param instanceId    the workflow instance that wrote, or {@code null}
 * @param chainId       its business chain, or {@code null}
 * @param definition    the definition with its version ({@code namespace:name:version}), or {@code null}
 * @param schemaVersion the graph schema version of the batch, or {@code null} for an erasure
 * @param op            what was done
 * @param kind          to what
 * @param targetId      the passage id, the entity id, the edge key, or {@code passageId>entityId} for a link
 * @param documentId    the document concerned, or {@code null}
 * @param sensitivity   the level of the thing
 * @param contentHash   a SHA-256 of its content, or {@code null} for a restricted thing (its hash would be derived data)
 * @param reason        for an erasure or an expiry, a code; otherwise {@code null}
 */
public record ChangeEntry(long sequence, Instant at, String batchId, String instanceId, String chainId,
        String definition, String schemaVersion, Op op, Kind kind, String targetId, String documentId,
        Sensitivity sensitivity, String contentHash, String reason) {

    public enum Op { ADD, UPDATE, REMOVE, EXPIRE, ERASE }

    public enum Kind { PASSAGE, ENTITY, RELATION, LINK }
}
