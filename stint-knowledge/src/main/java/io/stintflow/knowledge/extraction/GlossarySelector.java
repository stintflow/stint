package io.stintflow.knowledge.extraction;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.stintflow.knowledge.EntityIds;

/**
 * Picks the terms of a glossary a window of text can be about (SDD 3b.2, 4.7), so the prompt never carries the whole glossary.
 * A term is relevant when its normalized name or one of its aliases occurs in the text as a run of whole words. The most
 * mentioned come first, then by id; the list is cut at the limits of {@link ExtractionLimits} and says it was.
 */
public final class GlossarySelector {

    private GlossarySelector() {
    }

    /** @param truncated whether relevant terms were left out for the limits */
    public record Selection(List<Glossary.Term> terms, boolean truncated, int estimatedTokens) {
        public Selection {
            terms = List.copyOf(terms);
        }
    }

    /** The one line a term takes in the prompt: {@code id | name | up to 3 aliases}. */
    public static String line(Glossary.Term term) {
        StringBuilder out = new StringBuilder(term.id()).append(" | ").append(term.canonicalName());
        if (!term.aliases().isEmpty()) {
            out.append(" | ").append(String.join(", ", term.aliases().subList(0, Math.min(3, term.aliases().size()))));
        }
        return out.toString();
    }

    public static Selection select(Glossary glossary, String windowText, ExtractionLimits limits) {
        String text = " " + EntityIds.normalize(windowText) + " ";
        Map<Glossary.Term, Integer> hits = new HashMap<>();
        for (Glossary.Term term : glossary.terms()) {
            int count = 0;
            Set<String> names = new HashSet<>();
            names.add(EntityIds.normalize(term.canonicalName()));
            term.aliases().forEach(alias -> names.add(EntityIds.normalize(alias)));
            for (String name : names) {
                if (!name.isEmpty()) {
                    count += occurrences(text, " " + name + " ");
                }
            }
            if (count > 0) {
                hits.put(term, count);
            }
        }
        List<Glossary.Term> ranked = new ArrayList<>(hits.keySet());
        ranked.sort(Comparator.<Glossary.Term>comparingInt(hits::get).reversed().thenComparing(Glossary.Term::id));
        List<Glossary.Term> chosen = new ArrayList<>();
        int tokens = 0;
        boolean truncated = false;
        for (Glossary.Term term : ranked) {
            int cost = ExtractionLimits.tokens(line(term));
            if (chosen.size() >= limits.maxGlossaryTerms() || tokens + cost > limits.maxGlossaryTokens()) {
                truncated = true;
                continue;
            }
            chosen.add(term);
            tokens += cost;
        }
        chosen.sort(Comparator.comparing(Glossary.Term::id));   // a stable order, so the prompt prefix is the same every time
        return new Selection(chosen, truncated, tokens);
    }

    private static int occurrences(String text, String needle) {
        int count = 0;
        int from = 0;
        while ((from = text.indexOf(needle, from)) >= 0) {
            count++;
            from += needle.length() - 1;   // the space between two names may be shared
        }
        return count;
    }
}
