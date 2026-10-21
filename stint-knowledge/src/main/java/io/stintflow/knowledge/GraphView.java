package io.stintflow.knowledge;

import java.util.List;

/**
 * A page of the structure now (SDD 3b.6, 4.1): nodes in id order, and the edges that start at them whose other end is also
 * visible. {@code next} is the {@code afterNodeId} of the following page, or {@code null} on the last. {@code lastSequence} is the
 * log's end when the page was read, so a client can ask for the changes after it.
 */
public record GraphView(List<NodeView> nodes, List<EdgeView> edges, String next, long lastSequence) {

    public GraphView {
        nodes = List.copyOf(nodes);
        edges = List.copyOf(edges);
    }
}
