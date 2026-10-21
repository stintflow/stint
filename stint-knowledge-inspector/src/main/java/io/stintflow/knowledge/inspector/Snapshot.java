package io.stintflow.knowledge.inspector;

import java.util.List;
import java.util.Map;

/**
 * The structure of a base at a point of its log (SDD 3b.6, 4.2), as ids, types, levels and origin; a {@code label} only for a
 * thing that still exists now and is not restricted — the log never held one. {@code aggregated} says the structure was too big to
 * list ({@code RF9}): then only the counts are there.
 *
 * @param sequence the last log entry applied
 * @param at       when it was written, or {@code null} for an empty log
 * @param erased   ids an erasure took out up to this point (opaque; there is nothing to say about what they were)
 */
public record Snapshot(long sequence, String at, boolean aggregated, List<Node> nodes, List<Edge> edges, int passages,
        int links, Map<String, Integer> nodesByType, Map<String, Integer> edgesByType, Map<String, Integer> nodesByChain,
        List<String> erased) {

    public Snapshot {
        nodes = List.copyOf(nodes);
        edges = List.copyOf(edges);
        nodesByType = Map.copyOf(nodesByType);
        edgesByType = Map.copyOf(edgesByType);
        nodesByChain = Map.copyOf(nodesByChain);
        erased = List.copyOf(erased);
    }

    public record Node(String id, String type, String level, String label, long addedSequence, String addedAt,
            String instanceId, String chainId, String definition, String schemaVersion, String documentId) {
    }

    public record Edge(String key, String type, String from, String to, String level, long addedSequence, String addedAt,
            String instanceId, String chainId) {
    }
}
