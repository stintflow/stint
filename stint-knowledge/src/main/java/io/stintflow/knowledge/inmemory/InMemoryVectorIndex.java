package io.stintflow.knowledge.inmemory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import io.stintflow.spi.Sensitivity;

/**
 * The passages of one scope and their vectors (SDD 3b.1, 4.9): exact cosine over every vector, in memory. A restricted
 * passage keeps no text here — only the pointer to its blob (E1); its vector is derived data and is held at the same level.
 * <p>
 * Not thread-safe by itself: {@link CompositeKnowledgeStore} changes a {@link #copy()} under its lock and publishes it whole,
 * so a reader sees the old index or the new one, never a half.
 */
final class InMemoryVectorIndex {

    /** A passage as the index holds it; {@code text} is {@code null} when {@code contentRef} is set. */
    record Indexed(String id, String documentId, int ordinal, String language, Sensitivity level, float[] vector,
            String text, URI contentRef) {
    }

    record Hit(Indexed passage, double score) {
    }

    private final Map<String, Indexed> passages;

    InMemoryVectorIndex() {
        this.passages = new LinkedHashMap<>();
    }

    private InMemoryVectorIndex(Map<String, Indexed> passages) {
        this.passages = new LinkedHashMap<>(passages);
    }

    InMemoryVectorIndex copy() {
        return new InMemoryVectorIndex(passages);
    }

    Indexed get(String id) {
        return passages.get(id);
    }

    void put(Indexed passage) {
        passages.put(passage.id(), passage);
    }

    Indexed remove(String id) {
        return passages.remove(id);
    }

    List<Indexed> all() {
        return List.copyOf(passages.values());
    }

    int size() {
        return passages.size();
    }

    /** The {@code topK} closest passages that pass {@code filter} and score at least {@code minScore}, best first, ties by id. */
    List<Hit> nearest(float[] query, Predicate<Indexed> filter, int topK, double minScore) {
        List<Hit> hits = new ArrayList<>();
        for (Indexed passage : passages.values()) {
            if (!filter.test(passage)) {
                continue;
            }
            double score = cosine(query, passage.vector());
            if (score >= minScore && score > 0) {
                hits.add(new Hit(passage, score));
            }
        }
        hits.sort(Comparator.comparingDouble(Hit::score).reversed().thenComparing(h -> h.passage().id()));
        return hits.size() > topK ? new ArrayList<>(hits.subList(0, topK)) : hits;
    }

    /** Cosine of two vectors, 0 when either is empty; the embedder's vectors are non-negative so it lies in [0, 1]. */
    static double cosine(float[] a, float[] b) {
        double dot = 0;
        double na = 0;
        double nb = 0;
        for (int i = 0; i < a.length && i < b.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na == 0 || nb == 0) {
            return 0;
        }
        return Math.max(0, Math.min(1, dot / (Math.sqrt(na) * Math.sqrt(nb))));
    }
}
