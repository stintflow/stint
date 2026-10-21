package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6, CA10: what any {@link KnowledgeInspector} must do, through the port alone — and what it must never show. A backend
 * extends this class (which extends the store's conformance suite) and says how to see its store as an inspector. Synthetic
 * data only.
 */
public abstract class KnowledgeInspectorContract extends KnowledgeStoreContract {

    /** The inspection of a store {@link #newStore} made. */
    protected abstract KnowledgeInspector inspector(KnowledgeStore store);

    private static GraphView view(KnowledgeInspector inspector, Sensitivity ceiling, int limit, String after, String type)
            throws Exception {
        return inspector.view(SCOPE, new ViewQuery(ceiling, limit, after, type)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static B mixed() {
        B b = new B(SCOPE, "meeting:m-100")
                .passage("The SYNTH-PII-0100 text of a passage that must never come through.", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL)
                .entity("e-payments", "System", "Payments API", Sensitivity.INTERNAL)
                .entity("e-team", "Team", "Team SYNTH-PII-0101", Sensitivity.CONFIDENTIAL)
                .entity("e-person", "Person", "SYNTH-PII-0102", Sensitivity.RESTRICTED)
                .relation("affects", "e-checkout", "e-payments", Sensitivity.INTERNAL)
                .relation("owns", "e-team", "e-checkout", Sensitivity.CONFIDENTIAL)
                .relation("knows", "e-person", "e-checkout", Sensitivity.RESTRICTED)
                .link(0, "e-checkout");
        b.entities.set(0, new Entity("e-checkout", "System", "Checkout Service", Map.of("note", "SYNTH-PII-0103 property"),
                Sensitivity.INTERNAL));
        return b;
    }

    @Test
    void ci1_the_view_shows_ids_types_levels_and_labels_but_never_a_restricted_label_a_property_or_a_passage() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, mixed().build());

        GraphView view = view(inspector(store), Sensitivity.RESTRICTED, 100, null, null);

        assertThat(view.nodes()).extracting(NodeView::id).containsExactly("e-checkout", "e-payments", "e-person", "e-team");
        NodeView person = view.nodes().stream().filter(n -> n.id().equals("e-person")).findFirst().orElseThrow();
        assertThat(person.label()).isNull();
        assertThat(person.type()).isEqualTo("Person");
        assertThat(person.level()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(view.nodes().stream().filter(n -> n.id().equals("e-checkout")).findFirst().orElseThrow().label())
                .isEqualTo("Checkout Service");
        assertThat(view.edges()).extracting(EdgeView::type).containsExactlyInAnyOrder("affects", "owns", "knows");
        assertThat(view.toString()).doesNotContain("SYNTH-PII-0100").doesNotContain("SYNTH-PII-0102")
                .doesNotContain("SYNTH-PII-0103");
    }

    @Test
    void ci2_nothing_above_the_ceiling_comes_back_nor_an_edge_that_would_tell_it_is_there() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, mixed().build());

        GraphView internal = view(inspector(store), Sensitivity.INTERNAL, 100, null, null);

        assertThat(internal.nodes()).extracting(NodeView::id).containsExactly("e-checkout", "e-payments");
        assertThat(internal.edges()).extracting(EdgeView::type).containsExactly("affects");
        assertThat(internal.toString()).doesNotContain("e-team").doesNotContain("e-person").doesNotContain("owns");
        GraphView confidential = view(inspector(store), Sensitivity.CONFIDENTIAL, 100, null, null);
        assertThat(confidential.nodes()).extracting(NodeView::id).containsExactly("e-checkout", "e-payments", "e-team");
        assertThat(confidential.edges()).extracting(EdgeView::type).containsExactlyInAnyOrder("affects", "owns");
    }

    @Test
    void ci3_pages_cover_every_node_once_and_the_filter_by_type_holds() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, mixed().build());
        KnowledgeInspector inspector = inspector(store);

        List<String> seen = new ArrayList<>();
        String after = null;
        int pages = 0;
        do {
            GraphView page = view(inspector, Sensitivity.RESTRICTED, 3, after, null);
            page.nodes().forEach(n -> seen.add(n.id()));
            after = page.next();
            pages++;
        } while (after != null && pages < 10);

        assertThat(seen).containsExactly("e-checkout", "e-payments", "e-person", "e-team");
        assertThat(pages).isEqualTo(2);
        assertThat(view(inspector, Sensitivity.RESTRICTED, 100, null, "System").nodes()).extracting(NodeView::id)
                .containsExactly("e-checkout", "e-payments");
        assertThatThrownBy(() -> new ViewQuery(null, 10, null, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ViewQuery(Sensitivity.INTERNAL, 0, null, null))
                .isInstanceOf(KnowledgeValidationException.class);
    }

    @Test
    void ci4_the_view_says_which_entry_added_a_node_how_many_documents_hold_it_and_where_the_log_ends() throws Exception {
        KnowledgeStore store = newStore(true);
        write(store, SCOPE, mixed().build());
        write(store, SCOPE, new B(SCOPE, "meeting:m-101").passage("Another meeting about checkout.", Sensitivity.INTERNAL)
                .entity("e-checkout", "System", "Checkout Service", Sensitivity.INTERNAL).link(0, "e-checkout").build());
        KnowledgeInspector inspector = inspector(store);

        GraphView view = view(inspector, Sensitivity.RESTRICTED, 100, null, null);

        NodeView checkout = view.nodes().stream().filter(n -> n.id().equals("e-checkout")).findFirst().orElseThrow();
        assertThat(checkout.evidence()).isEqualTo(2);
        ChangeEntry added = log(store, SCOPE).stream().filter(e -> e.op() == ChangeEntry.Op.ADD
                && e.kind() == ChangeEntry.Kind.ENTITY && e.targetId().equals("e-checkout")).findFirst().orElseThrow();
        assertThat(checkout.addedSequence()).isEqualTo(added.sequence());
        assertThat(view.lastSequence()).isEqualTo(log(store, SCOPE).getLast().sequence());
        assertThat(inspector.changes(SCOPE, 0, 1000).toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo(log(store, SCOPE));
    }

    @Test
    void ci5_an_erased_person_is_not_in_the_view_and_a_base_never_written_is_empty() throws Exception {
        KnowledgeStore store = newStore(true);
        assertThat(view(inspector(store), Sensitivity.RESTRICTED, 10, null, null).nodes()).isEmpty();
        write(store, SCOPE, mixed().build());

        erase(store, SCOPE, new DeleteRequest.ByPerson("er-1", "erasure:person", "e-person", null));

        GraphView view = view(inspector(store), Sensitivity.RESTRICTED, 100, null, null);
        assertThat(view.nodes()).extracting(NodeView::id).doesNotContain("e-person");
        assertThat(view.edges()).extracting(EdgeView::type).doesNotContain("knows");
    }
}
