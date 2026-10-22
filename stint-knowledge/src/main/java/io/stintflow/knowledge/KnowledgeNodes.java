package io.stintflow.knowledge;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Port (SDD 3c.2, 4.1): the content of nodes, for the worker that answers another domain's pointer. Read-only by construction —
 * nothing here writes or erases — and, unlike {@link KnowledgeInspector}, it returns a node's label and properties, so its
 * ceiling is required and never above {@code confidential}: what rests in a restricted blob does not come through this port.
 * <p>
 * A node that does not exist and a node above the ceiling are the same answer: absent. Every backend that holds a base passes
 * {@code KnowledgeNodesContract} (this module's test-jar).
 */
public interface KnowledgeNodes {

    /** The nodes of {@code query.ids()} that exist at or below the ceiling, in the order of their ids. */
    CompletionStage<List<NodeContent>> nodes(KnowledgeScope scope, NodesQuery query);
}
