package io.stintflow.knowledge.inspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.NodeView;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6, 4.5: pairs of nodes that may be one thing written two ways. Two nodes of the same type, neither restricted, whose names
 * — after {@link EntityIds#normalize}, which already makes case, accents and punctuation the same — still differ, and which
 * match by one of: a small edit distance, many words in common, or one name being the start of the other — unless the numbers in
 * the names differ, which tells two things apart (m-005 and m-006). A signal for someone to
 * look at; nothing here merges. A restricted node has no label to compare, and a node past {@link #MAX_PER_TYPE} of its type is
 * not compared.
 */
public final class Duplicates {

    public static final int MAX_PER_TYPE = 2000;
    static final int MAX_EDIT_DISTANCE = 2;
    static final double MIN_JACCARD = 0.6;

    public record Candidate(String type, String aId, String aLabel, int aEvidence, String bId, String bLabel, int bEvidence,
            String criterion) {
    }

    private Duplicates() {
    }

    public static List<Candidate> find(List<NodeView> nodes) {
        Map<String, List<NodeView>> byType = new LinkedHashMap<>();
        for (NodeView node : nodes) {
            if (node.level() != Sensitivity.RESTRICTED && node.label() != null) {
                byType.computeIfAbsent(node.type(), t -> new ArrayList<>()).add(node);
            }
        }
        List<Candidate> out = new ArrayList<>();
        byType.forEach((type, ofType) -> {
            List<NodeView> sorted = new ArrayList<>(ofType);
            sorted.sort(Comparator.comparing(NodeView::id));
            if (sorted.size() > MAX_PER_TYPE) {
                sorted = sorted.subList(0, MAX_PER_TYPE);
            }
            String[] normal = new String[sorted.size()];
            for (int i = 0; i < normal.length; i++) {
                normal[i] = EntityIds.normalize(sorted.get(i).label());
            }
            for (int i = 0; i < normal.length; i++) {
                for (int j = i + 1; j < normal.length; j++) {
                    if (normal[i].equals(normal[j])) {
                        continue;   // the same name is the same id: not a candidate, a node written twice
                    }
                    String criterion = criterion(normal[i], normal[j]);
                    if (criterion != null) {
                        NodeView a = sorted.get(i);
                        NodeView b = sorted.get(j);
                        out.add(new Candidate(type, a.id(), a.label(), a.evidence(), b.id(), b.label(), b.evidence(), criterion));
                    }
                }
            }
        });
        return out;
    }

    /** How many nodes take part in at least one candidate pair. */
    public static int involved(List<Candidate> candidates) {
        Set<String> ids = new HashSet<>();
        candidates.forEach(c -> {
            ids.add(c.aId());
            ids.add(c.bId());
        });
        return ids.size();
    }

    static String criterion(String a, String b) {
        // numbers tell things apart: m-005 and m-006, Payments v2 and v3
        if (!numbers(a).equals(numbers(b))) {
            return null;
        }
        String[] wa = a.split(" ");
        String[] wb = b.split(" ");
        if (a.length() >= 4 && b.length() >= 4 && Math.abs(a.length() - b.length()) <= MAX_EDIT_DISTANCE
                && distance(a, b) <= MAX_EDIT_DISTANCE) {
            return "edit-distance";
        }
        if (wa.length >= 2 && wb.length >= 2 && jaccard(wa, wb) >= MIN_JACCARD) {
            return "word-overlap";
        }
        if (wa.length != wb.length) {
            String[] shorter = wa.length < wb.length ? wa : wb;
            String[] longer = wa.length < wb.length ? wb : wa;
            if (Arrays.equals(shorter, Arrays.copyOf(longer, shorter.length))) {
                return "word-prefix";
            }
        }
        return null;
    }

    private static List<String> numbers(String name) {
        List<String> out = new ArrayList<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\d+").matcher(name);
        while (m.find()) {
            out.add(m.group());
        }
        return out;
    }

    private static double jaccard(String[] a, String[] b) {
        Set<String> sa = new HashSet<>(Arrays.asList(a));
        Set<String> sb = new HashSet<>(Arrays.asList(b));
        Set<String> both = new HashSet<>(sa);
        both.retainAll(sb);
        Set<String> any = new HashSet<>(sa);
        any.addAll(sb);
        return any.isEmpty() ? 0 : (double) both.size() / any.size();
    }

    /** Levenshtein distance. */
    static int distance(String a, String b) {
        int[] prev = new int[b.length() + 1];
        int[] cur = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) {
            prev[j] = j;
        }
        for (int i = 1; i <= a.length(); i++) {
            cur[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
                cur[j] = Math.min(Math.min(cur[j - 1] + 1, prev[j] + 1), prev[j - 1] + cost);
            }
            int[] swap = prev;
            prev = cur;
            cur = swap;
        }
        return prev[b.length()];
    }
}
