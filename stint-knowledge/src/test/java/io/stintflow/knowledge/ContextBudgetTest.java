package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.Sensitivity;

/** SDD 3b.4, CA5: the budget keeps whole items in the order of the ranking and says how many it left out. */
class ContextBudgetTest {

    private static ContextItem item(String id, int chars) {
        return new ContextItem("x".repeat(chars), new ContextItem.Origin(ContextItem.Kind.PASSAGE, id, "meeting:m-1",
                KnowledgeScope.named("examples", "b"), null, null), 0.5, List.of(), Sensitivity.INTERNAL);
    }

    @Test
    void an_absent_budget_cuts_nothing() {
        List<ContextItem> items = List.of(item("a", 4000), item("b", 4000));

        assertThat(ContextBudget.fit(items, null)).isEqualTo(new ContextBudget.Fit(items, 0));
    }

    @Test
    void the_estimate_is_a_quarter_of_a_character_rounded_up_plus_the_label() {
        assertThat(ContextBudget.estimate(item("a", 100))).isEqualTo(25 + ContextBudget.PER_ITEM_OVERHEAD);
        assertThat(ContextBudget.estimate(item("a", 101))).isEqualTo(26 + ContextBudget.PER_ITEM_OVERHEAD);
        assertThat(ContextBudget.estimate(item("a", 0))).isEqualTo(ContextBudget.PER_ITEM_OVERHEAD);
    }

    @Test
    void the_sum_never_passes_the_budget_and_the_cut_is_at_the_first_item_that_does_not_fit() {
        List<ContextItem> ranked = List.of(item("a", 100), item("b", 100), item("c", 400), item("d", 4));
        int two = 2 * (25 + ContextBudget.PER_ITEM_OVERHEAD);

        ContextBudget.Fit exact = ContextBudget.fit(ranked, two);
        ContextBudget.Fit oneShort = ContextBudget.fit(ranked, two - 1);

        assertThat(exact.kept()).extracting(i -> i.origin().id()).containsExactly("a", "b");
        assertThat(exact.trimmed()).isEqualTo(2);   // d is small enough, but c came first: the ranking is not reordered by size
        assertThat(oneShort.kept()).extracting(i -> i.origin().id()).containsExactly("a");
        assertThat(oneShort.trimmed()).isEqualTo(3);
    }

    @Test
    void one_item_alone_above_the_budget_leaves_and_is_counted() {
        ContextBudget.Fit fit = ContextBudget.fit(List.of(item("a", 4000)), 100);

        assertThat(fit.kept()).isEmpty();
        assertThat(fit.trimmed()).isEqualTo(1);
    }

    @Test
    void a_budget_of_zero_keeps_nothing_and_a_negative_one_is_refused() {
        assertThat(ContextBudget.fit(List.of(item("a", 1)), 0).kept()).isEmpty();
        assertThatThrownBy(() -> ContextBudget.fit(List.of(), -1)).isInstanceOf(KnowledgeValidationException.class);
    }
}
