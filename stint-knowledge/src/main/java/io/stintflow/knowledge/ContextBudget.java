package io.stintflow.knowledge;

import java.util.ArrayList;
import java.util.List;

/**
 * SDD 3b.4, 4.2: a ceiling on how much context one call hands a model. There is no tokenizer in the project, so a token is
 * <b>estimated</b>: a quarter of a character, rounded up, plus a few for the label the delivery puts around each item. Items go in
 * in the order of the ranking, whole; the first that does not fit ends it (a smaller one after it does not jump the queue).
 */
public final class ContextBudget {

    /** What the label and separators around one item cost, in estimated tokens. */
    static final int PER_ITEM_OVERHEAD = 8;

    private ContextBudget() {
    }

    /** What an item costs, estimated. */
    public static int estimate(ContextItem item) {
        return (item.content().length() + 3) / 4 + PER_ITEM_OVERHEAD;
    }

    /** {@code kept} is the items that fit, in order; {@code trimmed} how many were left out. */
    public record Fit(List<ContextItem> kept, int trimmed) {
    }

    /** The items that fit in {@code tokens}; a negative or absent budget is no cut. */
    public static Fit fit(List<ContextItem> ranked, Integer tokens) {
        if (tokens == null) {
            return new Fit(ranked, 0);
        }
        if (tokens < 0) {
            throw new KnowledgeValidationException("budgetTokens is 0 or more");
        }
        List<ContextItem> kept = new ArrayList<>();
        int used = 0;
        for (ContextItem item : ranked) {
            int cost = estimate(item);
            if (used + cost > tokens) {
                break;
            }
            used += cost;
            kept.add(item);
        }
        return new Fit(kept, ranked.size() - kept.size());
    }
}
