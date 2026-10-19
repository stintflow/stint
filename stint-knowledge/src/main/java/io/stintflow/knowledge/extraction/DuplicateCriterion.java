package io.stintflow.knowledge.extraction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.stintflow.knowledge.EntityIds;

/**
 * When two entities are candidates to be one (SDD 3b.2, 4.5) — the one criterion the extraction (which only <em>marks</em>
 * a candidate) and the history of the graph (SDD 3b.6, which reviews and merges) both call, so what one flags the other sees.
 * It is a pure function and it is versioned: a change of the threshold or of the rules changes {@link #VERSION}.
 * <ul>
 *   <li>only entities of the <em>same type</em>, never a {@code Person} (a pseudonym is not compared by similarity);</li>
 *   <li>a candidate if a normalized name or alias of one equals one of the other, <em>or</em> the same {@code url}, <em>or</em> the
 *       similarity of the normalized names (Jaro-Winkler over the words in sorted order) is at least {@value #THRESHOLD}.</li>
 * </ul>
 * A candidate is never a merge: merging wrongly destroys knowledge, duplicating only costs a review.
 */
public final class DuplicateCriterion {

    public static final String VERSION = "duplicate-criterion/1";
    public static final double THRESHOLD = 0.85;

    private DuplicateCriterion() {
    }

    /** What the criterion looks at in an entity: its type, its label and aliases, and its url if it has one. */
    public record Named(String type, String label, List<String> aliases, String url) {
        public Named {
            aliases = aliases == null ? List.of() : List.copyOf(aliases);
        }

        public Named(String type, String label) {
            this(type, label, List.of(), null);
        }
    }

    /** @param score 1.0 for an equal name, alias or url; else the similarity */
    public record Candidate(double score, List<String> reasons) {
        public Candidate {
            reasons = List.copyOf(reasons);
        }
    }

    public static Optional<Candidate> compare(Named a, Named b) {
        if (!a.type().equals(b.type()) || a.type().equals("Person")) {
            return Optional.empty();
        }
        List<String> reasons = new ArrayList<>();
        double score = 0;
        if (a.url() != null && a.url().equals(b.url())) {
            reasons.add("url");
            score = 1.0;
        }
        List<String> namesA = names(a);
        List<String> namesB = names(b);
        for (String x : namesA) {
            if (namesB.contains(x)) {
                if (!reasons.contains("alias")) {
                    reasons.add("alias");
                }
                score = 1.0;
            }
        }
        double similarity = similarity(sorted(EntityIds.normalize(a.label())), sorted(EntityIds.normalize(b.label())));
        if (similarity >= THRESHOLD) {
            reasons.add("similarity");
            score = Math.max(score, similarity);
        }
        return reasons.isEmpty() ? Optional.empty() : Optional.of(new Candidate(score, reasons));
    }

    private static List<String> names(Named n) {
        List<String> all = new ArrayList<>();
        all.add(EntityIds.normalize(n.label()));
        n.aliases().forEach(alias -> all.add(EntityIds.normalize(alias)));
        return all;
    }

    private static String sorted(String normalized) {
        String[] words = normalized.split(" ");
        Arrays.sort(words);
        return String.join(" ", words);
    }

    /** Jaro-Winkler similarity in [0, 1]. */
    static double similarity(String s1, String s2) {
        if (s1.equals(s2)) {
            return 1.0;
        }
        int len1 = s1.length();
        int len2 = s2.length();
        if (len1 == 0 || len2 == 0) {
            return 0.0;
        }
        int window = Math.max(0, Math.max(len1, len2) / 2 - 1);
        boolean[] matched1 = new boolean[len1];
        boolean[] matched2 = new boolean[len2];
        int matches = 0;
        for (int i = 0; i < len1; i++) {
            int from = Math.max(0, i - window);
            int to = Math.min(len2 - 1, i + window);
            for (int j = from; j <= to; j++) {
                if (!matched2[j] && s1.charAt(i) == s2.charAt(j)) {
                    matched1[i] = true;
                    matched2[j] = true;
                    matches++;
                    break;
                }
            }
        }
        if (matches == 0) {
            return 0.0;
        }
        int transpositions = 0;
        int k = 0;
        for (int i = 0; i < len1; i++) {
            if (matched1[i]) {
                while (!matched2[k]) {
                    k++;
                }
                if (s1.charAt(i) != s2.charAt(k)) {
                    transpositions++;
                }
                k++;
            }
        }
        double m = matches;
        double jaro = (m / len1 + m / len2 + (m - transpositions / 2.0) / m) / 3.0;
        int prefix = 0;
        for (int i = 0; i < Math.min(4, Math.min(len1, len2)); i++) {
            if (s1.charAt(i) == s2.charAt(i)) {
                prefix++;
            } else {
                break;
            }
        }
        return jaro + prefix * 0.1 * (1 - jaro);
    }
}
