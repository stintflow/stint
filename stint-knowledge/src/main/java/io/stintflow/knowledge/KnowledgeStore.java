package io.stintflow.knowledge;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Port (SDD 3b.1): writes and reads what workflows learn — passages, entities and relations, each with a sensitivity level.
 * Lives outside the SPI: the engine never calls it, workers do.
 * <ul>
 *   <li><b>Levels.</b> Every item has one (absent reads as restricted). The content of a restricted item rests only in a blob
 *       of a retention class (SDD 3a.5, E1); a store with no class refuses to write one. {@link #retrieve} and
 *       {@link #entities} return nothing above the caller's {@code ceiling}, and nothing that tells it is there.</li>
 *   <li><b>Change log.</b> Every change is recorded in an append-only log in the <em>same atomic step</em> as the change itself:
 *       both happen, or neither. A write that changes nothing logs nothing.</li>
 *   <li><b>Idempotence.</b> A batch is the full state of one document; writing it again, or a redelivery, changes nothing
 *       ({@link WriteReceipt.Status#UNCHANGED}), and a batch older than what was applied is {@code STALE}.</li>
 *   <li><b>Erasure.</b> {@link #delete} removes by document or by person from every structure at once, and keeps a tombstone
 *       so a redelivered write does not resurrect it.</li>
 * </ul>
 * Every backend must pass {@code KnowledgeStoreContract} (this module's test-jar).
 */
public interface KnowledgeStore {

    /** The context closest to {@code query}, best first, at or below its ceiling. */
    CompletionStage<List<ContextItem>> retrieve(KnowledgeScope scope, RetrievalQuery query);

    /** Makes the base hold exactly {@code batch} for its document. @throws KnowledgeValidationException if refused */
    CompletionStage<WriteReceipt> write(KnowledgeScope scope, KnowledgeBatch batch);

    /** Erases (SDD 3b.1, 4.7). */
    CompletionStage<DeleteReceipt> delete(KnowledgeScope scope, DeleteRequest request);

    /** The log's entries with a sequence above {@code afterSequence}, oldest first, at most {@code limit}. */
    CompletionStage<List<ChangeEntry>> changes(KnowledgeScope scope, long afterSequence, int limit);

    /** Entities whose label or alias is the name asked for, at or below the ceiling. */
    CompletionStage<List<EntityMatch>> entities(KnowledgeScope scope, EntityQuery query);
}
