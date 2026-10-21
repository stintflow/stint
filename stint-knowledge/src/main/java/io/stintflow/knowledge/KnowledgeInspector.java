package io.stintflow.knowledge;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Port (SDD 3b.6): what a base looks like, for someone who looks and does not touch. Read-only by construction: nothing here
 * writes, erases or reads a blob, so what rests in a blob — a restricted label, a passage's text — never comes through.
 * <ul>
 *   <li>{@link #view}: the structure now, paged, at or below the caller's ceiling. A restricted node has an id and a type, no
 *       label. A node's properties and the text of a passage are not part of it.</li>
 *   <li>{@link #changes}: the change log, as {@link KnowledgeStore#changes} — ids, kinds, levels, hashes; never a text.</li>
 * </ul>
 * Every backend that holds a base passes {@code KnowledgeInspectorContract} (this module's test-jar).
 */
public interface KnowledgeInspector {

    CompletionStage<GraphView> view(KnowledgeScope scope, ViewQuery query);

    CompletionStage<List<ChangeEntry>> changes(KnowledgeScope scope, long afterSequence, int limit);
}
