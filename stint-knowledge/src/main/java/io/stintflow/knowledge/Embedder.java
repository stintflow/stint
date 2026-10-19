package io.stintflow.knowledge;

import java.util.Set;

/**
 * Turns text into a vector (SDD 3b.1, 4.2 and 4.8). A real one lives in a worker (an embedding model); the module has a
 * deterministic one without any model, for tests and the local mode.
 */
public interface Embedder {

    /** The vector of {@code text}; always {@link #dimension()} long. */
    float[] embed(String text);

    int dimension();

    /** The model, so a base never mixes vectors of two models. */
    String model();

    /** The languages it embeds (primary subtags, e.g. {@code en}); a write in another language is refused. */
    Set<String> languages();
}
