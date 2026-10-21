package io.stintflow.knowledge;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.4, 4.3: the entities of the base that a question names. No model and no outside call: the question is split into words,
 * and every run of one to {@value #MAX_WORDS} of them is looked up as a name in the base ({@link KnowledgeStore#entities}), at most
 * {@value #MAX_LOOKUPS} lookups. Names the caller already knows can be given, and go first. Only what the ceiling shows is found.
 */
public final class KnowledgeSeeds {

    public static final int MAX_WORDS = 4;
    public static final int MAX_LOOKUPS = 64;
    public static final int MAX_SEEDS = 20;
    static final int PER_NAME = 5;

    private KnowledgeSeeds() {
    }

    /** The ids of the entities found, in the order they were found, at most {@value #MAX_SEEDS}. */
    public static Set<String> find(KnowledgeStore store, KnowledgeScope scope, String question, Collection<String> names,
            Sensitivity ceiling) {
        Set<String> found = new LinkedHashSet<>();
        int lookups = 0;
        if (names != null) {
            for (String name : names) {
                String normal = EntityIds.normalize(name);
                if (!normal.isEmpty()) {
                    lookup(store, scope, normal, ceiling, found);
                }
            }
        }
        String[] words = EntityIds.normalize(question).split(" ");
        for (int start = 0; start < words.length && lookups < MAX_LOOKUPS && found.size() < MAX_SEEDS; start++) {
            StringBuilder phrase = new StringBuilder();
            for (int end = start; end < words.length && end < start + MAX_WORDS && lookups < MAX_LOOKUPS; end++) {
                if (words[end].isEmpty()) {
                    break;
                }
                phrase.append(end > start ? " " : "").append(words[end]);
                lookup(store, scope, phrase.toString(), ceiling, found);
                lookups++;
            }
        }
        return found.size() <= MAX_SEEDS ? found : new LinkedHashSet<>(List.copyOf(found).subList(0, MAX_SEEDS));
    }

    private static void lookup(KnowledgeStore store, KnowledgeScope scope, String normalName, Sensitivity ceiling,
            Set<String> into) {
        store.entities(scope, new EntityQuery(null, normalName, ceiling, PER_NAME)).toCompletableFuture().join()
                .forEach(match -> into.add(match.entityId()));
    }
}
