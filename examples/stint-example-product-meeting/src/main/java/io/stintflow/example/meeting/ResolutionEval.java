package io.stintflow.example.meeting;

import java.util.List;

import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.extraction.EntityResolver;
import io.stintflow.knowledge.extraction.EntityResolver.Kind;
import io.stintflow.knowledge.extraction.Glossary;

/**
 * The quality of the entity resolution over the evaluation set of the corpus (SDD 3b.2, 4.8), as numbers for the report of the
 * milestone (SDD 3c.6): the mentions the gold says a model would cite are resolved against the base the gold describes. The
 * test {@code ResolutionEvalTest} holds the thresholds; this class only measures.
 */
public final class ResolutionEval {

    /** @param wrongMerges something new folded into what exists, or a resolvable mention resolved to the wrong thing */
    public record Quality(int mentions, int resolvable, int resolvedRight, int newExpected, int newRight, int wrongMerges,
            int candidatesExpected, int candidatesFound) {

        public double recall() {
            return resolvable == 0 ? 1 : (double) resolvedRight / resolvable;
        }

        public double candidateRecall() {
            return candidatesExpected == 0 ? 1 : (double) candidatesFound / candidatesExpected;
        }
    }

    private ResolutionEval() {
    }

    public static Quality measure() {
        Corpus.Knowledge knowledge = Corpus.loadKnowledge();
        Glossary glossary = new Glossary("product", "1", knowledge.base().glossary().stream()
                .map(t -> new Glossary.Term(t.id(), t.type(), t.name(), t.aliases(), null)).toList());
        List<EntityResolver.Known> existing = knowledge.base().existing().stream()
                .map(t -> new EntityResolver.Known(t.id(), t.type(), t.name(), t.aliases(), null)).toList();
        EntityResolver resolver = new EntityResolver(glossary, new EntityResolver.Existing() {
            @Override
            public List<EntityResolver.Known> exact(String type, String normalizedName) {
                return existing.stream().filter(k -> k.type().equals(type)
                        && EntityIds.normalize(k.label()).equals(normalizedName)).toList();
            }

            @Override
            public List<EntityResolver.Known> ofType(String type) {
                return existing.stream().filter(k -> k.type().equals(type)).toList();
            }
        });
        int resolvable = 0;
        int right = 0;
        int newExpected = 0;
        int newRight = 0;
        int wrong = 0;
        int candidatesExpected = 0;
        int candidatesFound = 0;
        for (Corpus.KnowledgeMeeting meeting : knowledge.meetings()) {
            for (Corpus.Mention mention : meeting.mentions()) {
                EntityResolver.Resolution got = resolver.resolve(mention.type(), mention.surface(), null);
                if (mention.expect().equals("new")) {
                    newExpected++;
                    if (got.kind() == Kind.NEW) {
                        newRight++;
                    } else {
                        wrong++;
                    }
                    for (String candidate : mention.candidates()) {
                        candidatesExpected++;
                        if (got.candidates().stream().anyMatch(c -> c.other().equals(candidate))) {
                            candidatesFound++;
                        }
                    }
                    continue;
                }
                resolvable++;
                if (got.entityId().equals(mention.expect()) && got.kind() != Kind.NEW) {
                    right++;
                } else if (got.kind() != Kind.NEW) {
                    wrong++;
                }
            }
        }
        return new Quality((int) knowledge.mentionCount(), resolvable, right, newExpected, newRight, wrong, candidatesExpected,
                candidatesFound);
    }
}
