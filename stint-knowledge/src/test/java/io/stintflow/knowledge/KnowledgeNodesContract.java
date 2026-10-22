package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.stintflow.knowledge.NodeContent.Direction;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3c.2, CA11: what any {@link KnowledgeNodes} must do, through the port alone — the content of a node at or below the
 * ceiling, the same answer for a node that is absent and one that is above it, the neighbours within the ceiling and the limit.
 * A backend extends this class and says how to see its store as a {@link KnowledgeNodes}. Synthetic data only.
 */
public abstract class KnowledgeNodesContract extends KnowledgeInspectorContract {

    /** The node reading of a store {@link #newStore} made. */
    protected abstract KnowledgeNodes nodes(KnowledgeStore store);

    private static List<NodeContent> read(KnowledgeNodes nodes, Sensitivity ceiling, int neighbors, String... ids) throws Exception {
        return nodes.nodes(SCOPE, new NodesQuery(Set.of(ids), ceiling, neighbors)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private KnowledgeStore seeded() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, new B(SCOPE, "meeting:m-001")
                .entity("e-old", "Decision", "Old retry policy", Sensitivity.INTERNAL)
                .entity("e-new", "Decision", "New retry policy", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL)
                .entity("e-team", "Team", "Team SYNTH-PII-0201", Sensitivity.CONFIDENTIAL)
                .entity("e-person", "Person", "SYNTH-PII-0202", Sensitivity.RESTRICTED)
                .relation("supersedes", "e-new", "e-old", Sensitivity.INTERNAL)
                .relation("affects", "e-old", "e-checkout", Sensitivity.INTERNAL)
                .relation("owns", "e-team", "e-old", Sensitivity.CONFIDENTIAL)
                .relation("knows", "e-person", "e-old", Sensitivity.RESTRICTED).build());
        write(store, SCOPE, new B(SCOPE, "meeting:m-002")
                .entity("e-old", "Decision", "Old retry policy", Sensitivity.INTERNAL).build());
        return store;
    }

    @Test
    void cn1_a_node_comes_with_its_label_properties_level_and_the_documents_that_hold_it() throws Exception {
        KnowledgeStore store = newStore(true);
        KnowledgeBatch batch = new KnowledgeBatch("b-1", "meeting:m-001", "h", 1L, null, SCHEMA, List.of(),
                List.of(new Entity("e-old", "Decision", "Old retry policy", Map.of("status", "accepted"), Sensitivity.INTERNAL)),
                List.of(), List.of());
        write(store, SCOPE, batch);
        write(store, SCOPE, new KnowledgeBatch("b-2", "meeting:m-002", "h2", 1L, null, SCHEMA, List.of(),
                List.of(new Entity("e-old", "Decision", "Old retry policy", Map.of("status", "accepted"), Sensitivity.INTERNAL)),
                List.of(), List.of()));

        NodeContent node = read(nodes(store), Sensitivity.CONFIDENTIAL, 0, "e-old").getFirst();

        assertThat(node.id()).isEqualTo("e-old");
        assertThat(node.type()).isEqualTo("Decision");
        assertThat(node.label()).isEqualTo("Old retry policy");
        assertThat(node.properties()).containsExactly(Map.entry("status", "accepted"));
        assertThat(node.level()).isEqualTo(Sensitivity.INTERNAL);
        assertThat(node.documents()).containsExactly("meeting:m-001", "meeting:m-002");
        assertThat(node.neighbors()).isEmpty();
    }

    @Test
    void cn2_a_node_that_is_absent_and_a_node_above_the_ceiling_are_the_same_answer() throws Exception {
        KnowledgeNodes nodes = nodes(seeded());

        List<NodeContent> internal = read(nodes, Sensitivity.INTERNAL, 0, "e-old", "e-team", "e-person", "e-never");
        List<NodeContent> confidential = read(nodes, Sensitivity.CONFIDENTIAL, 0, "e-old", "e-team", "e-person", "e-never");

        assertThat(internal).extracting(NodeContent::id).containsExactly("e-old");
        assertThat(confidential).extracting(NodeContent::id).containsExactly("e-old", "e-team");
        assertThat(internal.toString()).doesNotContain("SYNTH-PII", "e-team", "e-person");
    }

    @Test
    void cn3_the_port_never_gives_restricted_content_and_refuses_a_restricted_ceiling_and_bad_sizes() {
        assertThatThrownBy(() -> new NodesQuery(Set.of("e-old"), Sensitivity.RESTRICTED, 0))
                .isInstanceOf(KnowledgeValidationException.class);
        assertThatThrownBy(() -> new NodesQuery(Set.of(), Sensitivity.INTERNAL, 0)).isInstanceOf(KnowledgeValidationException.class);
        Set<String> tooMany = new java.util.HashSet<>();
        for (int i = 0; i <= NodesQuery.MAX_IDS; i++) {
            tooMany.add("e-" + i);
        }
        assertThatThrownBy(() -> new NodesQuery(tooMany, Sensitivity.INTERNAL, 0)).isInstanceOf(KnowledgeValidationException.class);
        assertThatThrownBy(() -> new NodesQuery(Set.of("e-old"), Sensitivity.INTERNAL, NodesQuery.MAX_NEIGHBORS + 1))
                .isInstanceOf(KnowledgeValidationException.class);
        assertThatThrownBy(() -> new NodesQuery(Set.of("e-old"), null, 0)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void cn4_neighbours_come_with_the_relation_and_direction_and_only_within_the_ceiling_and_the_limit() throws Exception {
        KnowledgeNodes nodes = nodes(seeded());

        NodeContent internal = read(nodes, Sensitivity.INTERNAL, 20, "e-old").getFirst();
        NodeContent confidential = read(nodes, Sensitivity.CONFIDENTIAL, 20, "e-old").getFirst();
        NodeContent limited = read(nodes, Sensitivity.CONFIDENTIAL, 1, "e-old").getFirst();

        assertThat(internal.neighbors()).extracting(n -> n.relation() + ":" + n.direction() + ":" + n.node().id())
                .containsExactlyInAnyOrder("supersedes:IN:e-new", "affects:OUT:e-checkout");
        assertThat(confidential.neighbors()).extracting(n -> n.relation() + ":" + n.direction() + ":" + n.node().id())
                .containsExactlyInAnyOrder("supersedes:IN:e-new", "affects:OUT:e-checkout", "owns:IN:e-team");
        assertThat(confidential.neighbors()).allSatisfy(n -> assertThat(n.node().neighbors()).isEmpty());
        assertThat(limited.neighbors()).hasSize(1);
        assertThat(confidential.neighbors().stream().map(NodeContent.Neighbor::direction)).contains(Direction.IN, Direction.OUT);
        assertThat(confidential.toString()).doesNotContain("SYNTH-PII-0202", "e-person", "knows");
    }

    @Test
    void cn5_a_node_whose_last_document_was_erased_is_absent() throws Exception {
        KnowledgeStore store = seeded();
        erase(store, SCOPE, new DeleteRequest.ByDocument("req-1", "erasure:document", "meeting:m-001"));
        assertThat(read(nodes(store), Sensitivity.CONFIDENTIAL, 0, "e-old")).extracting(NodeContent::id).containsExactly("e-old");
        assertThat(read(nodes(store), Sensitivity.CONFIDENTIAL, 0, "e-old").getFirst().documents()).containsExactly("meeting:m-002");

        erase(store, SCOPE, new DeleteRequest.ByDocument("req-2", "erasure:document", "meeting:m-002"));

        assertThat(read(nodes(store), Sensitivity.CONFIDENTIAL, 0, "e-old")).isEmpty();
    }
}
