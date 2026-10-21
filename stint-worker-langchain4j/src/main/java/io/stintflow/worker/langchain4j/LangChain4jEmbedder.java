package io.stintflow.worker.langchain4j;

import java.util.Objects;
import java.util.Set;

import dev.langchain4j.model.embedding.EmbeddingModel;
import io.stintflow.knowledge.Embedder;

/**
 * SDD 3b.3, 4.7: the {@link Embedder} of a knowledge base over a LangChain4j {@link EmbeddingModel}. The worker holds the model
 * (ADR 3a.1-A: the provider lives in the worker, never in a definition); the name and the languages are configuration, because a
 * base never mixes the vectors of two models, and a write in a language the model was not meant for is refused by the store.
 * <p>
 * The dimension is the model's, read from its first vector when the model does not say.
 */
public final class LangChain4jEmbedder implements Embedder {

    private final EmbeddingModel model;
    private final String name;
    private final Set<String> languages;
    private volatile int dimension;

    /**
     * @param model     the embedding model
     * @param name      what the base records as the model (e.g. {@code nomic-embed-text-v1.5}); changing it asks for a reindex
     * @param languages the primary language subtags the model embeds (e.g. {@code en})
     */
    public LangChain4jEmbedder(EmbeddingModel model, String name, Set<String> languages) {
        this.model = Objects.requireNonNull(model, "model");
        this.name = Objects.requireNonNull(name, "name");
        if (name.isBlank()) {
            throw new IllegalArgumentException("The embedding model needs a name");
        }
        this.languages = Set.copyOf(languages);
        if (this.languages.isEmpty()) {
            throw new IllegalArgumentException("The embedding model needs at least one language");
        }
    }

    @Override
    public float[] embed(String text) {
        float[] vector = model.embed(text).content().vector();
        if (dimension == 0) {
            dimension = vector.length;
        } else if (dimension != vector.length) {
            throw new IllegalStateException("The embedding model changed its dimension (" + dimension + " to " + vector.length
                    + "): reindex the base with the new model");
        }
        return vector;
    }

    @Override
    public int dimension() {
        int known = dimension;
        if (known == 0) {
            known = model.dimension();   // asks the model; some read it from a first call
            dimension = known;
        }
        return known;
    }

    @Override
    public String model() {
        return name;
    }

    @Override
    public Set<String> languages() {
        return languages;
    }
}
