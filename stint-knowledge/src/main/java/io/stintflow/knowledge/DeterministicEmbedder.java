package io.stintflow.knowledge;

import java.util.Locale;
import java.util.Set;

/**
 * A bag of words hashed into {@value #DIMENSION} buckets and normalized: texts that share words are close, with no model
 * and the same vector every time. For tests and the local mode; not a semantic model.
 */
public final class DeterministicEmbedder implements Embedder {

    public static final int DIMENSION = 128;

    private final Set<String> languages;

    public DeterministicEmbedder() {
        this(Set.of("en"));
    }

    public DeterministicEmbedder(Set<String> languages) {
        this.languages = Set.copyOf(languages);
    }

    @Override
    public float[] embed(String text) {
        float[] vector = new float[DIMENSION];
        for (String word : EntityIds.normalize(text).split(" ")) {
            if (!word.isEmpty()) {
                vector[Math.floorMod(word.toLowerCase(Locale.ROOT).hashCode(), DIMENSION)] += 1f;
            }
        }
        double norm = 0;
        for (float v : vector) {
            norm += v * v;
        }
        if (norm > 0) {
            float scale = (float) (1 / Math.sqrt(norm));
            for (int i = 0; i < vector.length; i++) {
                vector[i] *= scale;
            }
        }
        return vector;
    }

    @Override
    public int dimension() {
        return DIMENSION;
    }

    @Override
    public String model() {
        return "deterministic-bag-of-words-" + DIMENSION;
    }

    @Override
    public Set<String> languages() {
        return languages;
    }
}
