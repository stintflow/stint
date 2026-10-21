package io.stintflow.knowledge.inspector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.stintflow.knowledge.NodeView;
import io.stintflow.spi.Sensitivity;

/** SDD 3b.6, 4.5 (RF5): what counts as a candidate for a duplicate, and what never does. Synthetic names. */
class DuplicatesTest {

    private static NodeView node(String id, String type, String label, Sensitivity level) {
        return new NodeView(id, type, level, label, 1, 1);
    }

    private static List<Duplicates.Candidate> find(NodeView... nodes) {
        return Duplicates.find(List.of(nodes));
    }

    @Test
    void a_small_edit_distance_is_a_candidate() {
        var found = find(node("a", "System", "Payments API", Sensitivity.INTERNAL), node("b", "System", "Payment API", Sensitivity.INTERNAL));

        assertThat(found).singleElement().satisfies(c -> assertThat(c.criterion()).isEqualTo("edit-distance"));
    }

    @Test
    void many_words_in_common_is_a_candidate() {
        var found = find(node("a", "System", "Checkout Service Gateway", Sensitivity.INTERNAL),
                node("b", "System", "Checkout Gateway Service Layer", Sensitivity.INTERNAL));

        assertThat(found).singleElement().satisfies(c -> assertThat(c.criterion()).isEqualTo("word-overlap"));
    }

    @Test
    void one_name_being_the_start_of_the_other_is_a_candidate() {
        var found = find(node("a", "System", "Payments", Sensitivity.INTERNAL), node("b", "System", "Payments API", Sensitivity.INTERNAL));

        assertThat(found).singleElement().satisfies(c -> assertThat(c.criterion()).isEqualTo("word-prefix"));
    }

    @Test
    void the_same_normal_form_is_not_a_candidate_it_is_one_id_and_other_types_or_far_names_are_not_either() {
        assertThat(find(node("a", "System", "Checkout Service", Sensitivity.INTERNAL),
                node("b", "System", "checkout-service", Sensitivity.INTERNAL))).isEmpty();
        assertThat(find(node("a", "System", "Payments API", Sensitivity.INTERNAL), node("b", "Team", "Payment API", Sensitivity.INTERNAL)))
                .isEmpty();
        assertThat(find(node("a", "System", "Checkout Service", Sensitivity.INTERNAL),
                node("b", "System", "Billing Service", Sensitivity.INTERNAL))).isEmpty();
        assertThat(find(node("a", "System", "API", Sensitivity.INTERNAL), node("b", "System", "APP", Sensitivity.INTERNAL))).isEmpty();
    }

    @Test
    void names_that_differ_by_a_number_are_two_things() {
        assertThat(find(node("a", "Meeting", "m-005", Sensitivity.INTERNAL), node("b", "Meeting", "m-006", Sensitivity.INTERNAL))).isEmpty();
        assertThat(find(node("a", "System", "Payments API v2", Sensitivity.INTERNAL),
                node("b", "System", "Payments API v3", Sensitivity.INTERNAL))).isEmpty();
        assertThat(find(node("a", "System", "Payments API v2", Sensitivity.INTERNAL),
                node("b", "System", "Payment API v2", Sensitivity.INTERNAL))).hasSize(1);
    }

    @Test
    void a_restricted_node_or_one_without_a_label_is_never_compared() {
        assertThat(find(node("a", "Person", "SYNTH-PII-0301", Sensitivity.RESTRICTED),
                node("b", "Person", "SYNTH-PII-0302", Sensitivity.RESTRICTED))).isEmpty();
        assertThat(find(node("a", "System", null, Sensitivity.INTERNAL), node("b", "System", "Payment API", Sensitivity.INTERNAL))).isEmpty();
    }

    @Test
    void the_nodes_involved_are_counted_once_each() {
        var found = find(node("a", "System", "Payments", Sensitivity.INTERNAL), node("b", "System", "Payments API", Sensitivity.INTERNAL),
                node("c", "System", "Payments API Gateway", Sensitivity.INTERNAL));

        assertThat(found).hasSizeGreaterThanOrEqualTo(2);
        assertThat(Duplicates.involved(found)).isEqualTo(3);
    }
}
