package io.stintflow.knowledge.inspector;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6, 4.2: the structure of a base at a point of its log, rebuilt from the log alone — a pure function of the entries. A
 * point is a sequence, or an instant (the last entry written at or before it).
 */
public final class Timeline {

    private Timeline() {
    }

    /** The last sequence written at or before {@code at}, or 0 if the log had not begun. */
    public static long sequenceAt(List<ChangeEntry> log, Instant at) {
        long last = 0;
        for (ChangeEntry e : log) {
            if (e.at().isAfter(at)) {
                break;
            }
            last = e.sequence();
        }
        return last;
    }

    /**
     * What existed after the entries up to {@code upToSequence}.
     *
     * @param labels  the labels of what exists now, by id; a thing that is gone, or restricted, has none
     * @param ceiling nothing above it is listed, as in the view
     * @param maxNodes more nodes than this are only counted (RF9)
     */
    public static Snapshot at(List<ChangeEntry> log, long upToSequence, Map<String, String> labels, Sensitivity ceiling,
            int maxNodes) {
        LogState state = new LogState();
        for (ChangeEntry e : log) {
            if (e.sequence() > upToSequence) {
                break;
            }
            state.apply(e);
        }
        return of(state, labels, ceiling, maxNodes);
    }

    static Snapshot of(LogState state, Map<String, String> labels, Sensitivity ceiling, int maxNodes) {
        List<LogState.Element> nodes = visible(state.alive(Kind.ENTITY).values(), ceiling);
        List<LogState.Element> edges = visible(state.alive(Kind.RELATION).values(), ceiling);
        Map<String, Integer> byType = new TreeMap<>();
        Map<String, Integer> byChain = new TreeMap<>();
        nodes.forEach(n -> {
            byType.merge(n.type() == null ? "(none)" : n.type(), 1, Integer::sum);
            byChain.merge(n.chainId() == null ? "(none)" : n.chainId(), 1, Integer::sum);
        });
        Map<String, Integer> edgesByType = new TreeMap<>();
        edges.forEach(e -> edgesByType.merge(e.type() == null ? "(none)" : e.type(), 1, Integer::sum));
        String at = state.lastAt() == null ? null : state.lastAt().toString();
        boolean aggregated = nodes.size() > maxNodes;
        List<Snapshot.Node> listedNodes = new ArrayList<>();
        List<Snapshot.Edge> listedEdges = new ArrayList<>();
        if (!aggregated) {
            Set<String> ids = new java.util.HashSet<>();
            for (LogState.Element n : nodes) {
                ids.add(n.id());
                listedNodes.add(new Snapshot.Node(n.id(), n.type(), n.level().name(),
                        n.level() == Sensitivity.RESTRICTED ? null : labels.get(n.id()), n.addedSequence(),
                        n.addedAt().toString(), n.instanceId(), n.chainId(), n.definition(), n.schemaVersion(), n.documentId()));
            }
            for (LogState.Element e : edges) {
                String[] parts = e.id().split("\\|", 3);
                if (parts.length == 3 && ids.contains(parts[1]) && ids.contains(parts[2])) {
                    listedEdges.add(new Snapshot.Edge(e.id(), e.type(), parts[1], parts[2], e.level().name(), e.addedSequence(),
                            e.addedAt().toString(), e.instanceId(), e.chainId()));
                }
            }
        }
        listedNodes.sort(Comparator.comparing(Snapshot.Node::id));
        listedEdges.sort(Comparator.comparing(Snapshot.Edge::key));
        return new Snapshot(state.sequence(), at, aggregated, listedNodes, listedEdges,
                visible(state.alive(Kind.PASSAGE).values(), ceiling).size(),
                visible(state.alive(Kind.LINK).values(), ceiling).size(), byType, edgesByType, byChain,
                new ArrayList<>(state.erased()));
    }

    private static List<LogState.Element> visible(java.util.Collection<LogState.Element> elements, Sensitivity ceiling) {
        List<LogState.Element> out = new ArrayList<>();
        for (LogState.Element e : elements) {
            if (e.level() == null || e.level().compareTo(ceiling) <= 0) {
                out.add(e);
            }
        }
        return out;
    }

    /** The ids of what {@code chainId} added and is still there in {@code snapshot}: what to highlight. */
    public static Set<String> createdBy(Snapshot snapshot, String chainId) {
        Set<String> ids = new java.util.TreeSet<>();
        snapshot.nodes().stream().filter(n -> chainId.equals(n.chainId())).forEach(n -> ids.add(n.id()));
        snapshot.edges().stream().filter(e -> chainId.equals(e.chainId())).forEach(e -> ids.add(e.key()));
        return ids;
    }
}
