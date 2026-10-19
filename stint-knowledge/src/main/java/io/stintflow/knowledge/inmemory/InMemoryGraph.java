package io.stintflow.knowledge.inmemory;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import io.stintflow.spi.Sensitivity;

/**
 * The nodes, edges and passage-to-entity links of one scope (SDD 3b.1, 4.9). Every node and edge keeps the set of documents
 * that assert it (its evidence): the same fact in two documents is one node and one edge, and goes when the last document
 * that sustains it goes. A restricted node keeps no label or properties here — only the pointer to its blob (E1).
 * <p>
 * Changed on a {@link #copy()} under the composite's lock and published whole, like {@link InMemoryVectorIndex}.
 */
final class InMemoryGraph {

    /** {@code label} and {@code properties} are empty when {@code contentRef} is set. */
    record Node(String id, String type, String label, Map<String, String> properties, URI contentRef,
            Sensitivity level, Set<String> sources) {
    }

    record Edge(String key, String type, String from, String to, Sensitivity level, Set<String> sources) {
    }

    private final Map<String, Node> nodes;
    private final Map<String, Edge> edges;
    /** passage id → entity ids */
    private final Map<String, Set<String>> links;

    InMemoryGraph() {
        this.nodes = new LinkedHashMap<>();
        this.edges = new LinkedHashMap<>();
        this.links = new LinkedHashMap<>();
    }

    private InMemoryGraph(InMemoryGraph other) {
        this.nodes = new LinkedHashMap<>(other.nodes);
        this.edges = new LinkedHashMap<>(other.edges);
        this.links = new LinkedHashMap<>();
        other.links.forEach((passage, entities) -> this.links.put(passage, new LinkedHashSet<>(entities)));
    }

    InMemoryGraph copy() {
        return new InMemoryGraph(this);
    }

    // --- nodes ---------------------------------------------------------------------------------------------------

    Node node(String id) {
        return nodes.get(id);
    }

    void putNode(Node node) {
        nodes.put(node.id(), new Node(node.id(), node.type(), node.label(), node.properties(), node.contentRef(),
                node.level(), Set.copyOf(new TreeSet<>(node.sources()))));
    }

    Node removeNode(String id) {
        return nodes.remove(id);
    }

    List<Node> nodes() {
        return List.copyOf(nodes.values());
    }

    // --- edges ---------------------------------------------------------------------------------------------------

    Edge edge(String key) {
        return edges.get(key);
    }

    void putEdge(Edge edge) {
        edges.put(edge.key(), new Edge(edge.key(), edge.type(), edge.from(), edge.to(), edge.level(),
                Set.copyOf(new TreeSet<>(edge.sources()))));
    }

    Edge removeEdge(String key) {
        return edges.remove(key);
    }

    List<Edge> edges() {
        return List.copyOf(edges.values());
    }

    /** The edges that have {@code entityId} at either end. */
    List<Edge> edgesOf(String entityId) {
        List<Edge> touching = new ArrayList<>();
        for (Edge edge : edges.values()) {
            if (edge.from().equals(entityId) || edge.to().equals(entityId)) {
                touching.add(edge);
            }
        }
        return touching;
    }

    // --- links ---------------------------------------------------------------------------------------------------

    boolean addLink(String passageId, String entityId) {
        return links.computeIfAbsent(passageId, k -> new LinkedHashSet<>()).add(entityId);
    }

    boolean removeLink(String passageId, String entityId) {
        Set<String> entities = links.get(passageId);
        if (entities == null || !entities.remove(entityId)) {
            return false;
        }
        if (entities.isEmpty()) {
            links.remove(passageId);
        }
        return true;
    }

    Set<String> entitiesOf(String passageId) {
        return links.getOrDefault(passageId, Set.of());
    }

    /** The passages that mention {@code entityId}. */
    List<String> passagesOf(String entityId) {
        List<String> passages = new ArrayList<>();
        links.forEach((passage, entities) -> {
            if (entities.contains(entityId)) {
                passages.add(passage);
            }
        });
        return passages;
    }

    /** Every link as {@code [passageId, entityId]}. */
    List<String[]> allLinks() {
        List<String[]> all = new ArrayList<>();
        links.forEach((passage, entities) -> entities.forEach(entity -> all.add(new String[] {passage, entity})));
        return all;
    }
}
