package io.stintflow.knowledge;

import java.util.Objects;
import java.util.Set;

import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;

/**
 * What a worker asks the base (SDD 3b.1, 4.2).
 *
 * @param text      the question, in natural language
 * @param topK      how many passages at most, 1 to {@value #MAX_TOP_K}
 * @param minScore  the lowest score kept, 0 to 1
 * @param ceiling   <b>required</b>: the highest level the caller may receive. Nothing above it comes back, and nothing about
 *                  what is above it (not its existence, not in a count, not in the order). See {@link Audience#ceiling}
 * @param provider  the model the answer is for, or {@code null}; kept for the caller's audit, the ceiling is what filters
 * @param entityIds only passages that mention one of these entities, or empty for any
 * @param language  only passages in this language, or {@code null} for any
 * @param hops      0 to {@value #MAX_HOPS}, counted from the passage: 1 adds the entities the passages mention and the relations
 *                  among them; 2 the neighbours of those entities and the relations that reach them; 3 their neighbours (SDD 3b.4, 4.1)
 */
public record RetrievalQuery(String text, int topK, double minScore, Sensitivity ceiling, ModelProvider provider,
        Set<String> entityIds, String language, int hops) {

    public static final int MAX_TOP_K = 50;
    public static final int MAX_HOPS = 3;

    public RetrievalQuery {
        Objects.requireNonNull(text, "text");
        Objects.requireNonNull(ceiling, "ceiling is required: there is no default level");
        if (topK < 1 || topK > MAX_TOP_K) {
            throw new KnowledgeValidationException("topK is 1 to " + MAX_TOP_K);
        }
        if (minScore < 0 || minScore > 1) {
            throw new KnowledgeValidationException("minScore is 0 to 1");
        }
        if (hops < 0 || hops > MAX_HOPS) {
            throw new KnowledgeValidationException("hops is 0 to " + MAX_HOPS);
        }
        entityIds = entityIds == null ? Set.of() : Set.copyOf(entityIds);
    }

    public RetrievalQuery(String text, int topK, Sensitivity ceiling) {
        this(text, topK, 0, ceiling, null, Set.of(), null, 0);
    }
}
