package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.extraction.EntityResolver;
import io.stintflow.knowledge.extraction.EntityResolver.Kind;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.worker.transcription.FakeTranscriber;

/**
 * SDD 3b.2, CA12 and 4.8: the entity-resolution evaluation set, over the synthetic English corpus of SDD 3a.6, measured
 * without a model — the mentions the gold says a model would cite are resolved by {@link EntityResolver} against the base
 * the gold describes. The thresholds are the SDD's: no wrong merge at all, alias and glossary resolution at least 95 %,
 * duplicate candidates at least 90 %. (The extraction with a model is a manual run and has no threshold.)
 */
class ResolutionEvalTest {

    private static final Corpus.Knowledge KNOWLEDGE = Corpus.loadKnowledge();

    private static Glossary glossary() {
        return new Glossary("product", "1", KNOWLEDGE.base().glossary().stream().map(t ->
                new Glossary.Term(t.id(), t.type(), t.name(), t.aliases(), null)).toList());
    }

    private static EntityResolver resolver() {
        List<EntityResolver.Known> existing = KNOWLEDGE.base().existing().stream()
                .map(t -> new EntityResolver.Known(t.id(), t.type(), t.name(), t.aliases(), null)).toList();
        return new EntityResolver(glossary(), new EntityResolver.Existing() {
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
    }

    @Test
    void the_set_is_whole_each_script_matches_the_manifest_each_surface_is_in_its_script_and_the_markers_are_listed() {
        assertThat(KNOWLEDGE.meetings()).hasSize(12);
        assertThat(KNOWLEDGE.meetings().stream().filter(Corpus.KnowledgeMeeting::inPipeline)).hasSize(8);   // the milestone's
        assertThat(FakeTranscriber.sha256(Corpus.bytes("corpus/kb/base.yaml"))).isEqualTo(KNOWLEDGE.base().sha256());
        for (Corpus.KnowledgeMeeting meeting : KNOWLEDGE.meetings()) {
            assertThat(FakeTranscriber.sha256(Corpus.bytes("corpus/meetings/" + meeting.id() + ".yaml")))
                    .as("hash of %s", meeting.id()).isEqualTo(meeting.scriptSha256());
            String text = meeting.text().toLowerCase(Locale.ROOT);
            for (Corpus.Mention mention : meeting.mentions()) {
                assertThat(text).as("%s: '%s' in the script", meeting.id(), mention.surface())
                        .contains(mention.surface().toLowerCase(Locale.ROOT));
                assertThat(mention.expect()).isNotBlank();
            }
            var markers = java.util.regex.Pattern.compile("SYNTH-PII-\\d{4}").matcher(meeting.text()).results()
                    .map(java.util.regex.MatchResult::group).distinct().toList();
            assertThat(markers).as("markers of %s", meeting.id()).containsExactlyInAnyOrderElementsOf(meeting.pii());
        }
        assertThat(KNOWLEDGE.mentionCount()).isGreaterThanOrEqualTo(120);
        assertThat(KNOWLEDGE.base().glossary()).extracting(Corpus.Term::id).doesNotHaveDuplicates();
    }

    @Test
    void ca12_resolution_makes_no_wrong_merge_and_meets_the_thresholds_of_the_sdd() {
        EntityResolver resolver = resolver();
        int wrongMerges = 0;
        int resolvable = 0;
        int resolvedRight = 0;
        int newExpected = 0;
        int newRight = 0;
        int candidatesExpected = 0;
        int candidatesFound = 0;
        List<String> misses = new ArrayList<>();
        Map<String, Integer> byHow = new java.util.TreeMap<>();
        for (Corpus.KnowledgeMeeting meeting : KNOWLEDGE.meetings()) {
            for (Corpus.Mention mention : meeting.mentions()) {
                EntityResolver.Resolution got = resolver.resolve(mention.type(), mention.surface(), null);
                if (mention.expect().equals("new")) {
                    newExpected++;
                    if (got.kind() == Kind.NEW) {
                        newRight++;
                    } else {
                        wrongMerges++;   // something that is new was folded into what exists
                        misses.add(meeting.id() + " '" + mention.surface() + "' merged into " + got.entityId());
                    }
                    for (String candidate : mention.candidates()) {
                        candidatesExpected++;
                        if (got.candidates().stream().anyMatch(c -> c.other().equals(candidate))) {
                            candidatesFound++;
                        } else {
                            misses.add(meeting.id() + " '" + mention.surface() + "' not flagged beside " + candidate);
                        }
                    }
                    continue;
                }
                resolvable++;
                byHow.merge(mention.how(), 1, Integer::sum);
                if (got.entityId().equals(mention.expect()) && got.kind() != Kind.NEW) {
                    resolvedRight++;
                } else {
                    misses.add(meeting.id() + " '" + mention.surface() + "' -> " + got.kind() + " " + got.entityId()
                            + " (expected " + mention.expect() + ")");
                    if (got.kind() != Kind.NEW) {
                        wrongMerges++;   // resolved to the wrong thing
                    }
                }
            }
        }
        // a homonym pair never becomes one: no mention of either resolves to the other
        for (List<String> pair : KNOWLEDGE.base().mustNotMerge()) {
            for (Corpus.KnowledgeMeeting meeting : KNOWLEDGE.meetings()) {
                for (Corpus.Mention mention : meeting.mentions()) {
                    if (mention.expect().equals(pair.get(0)) || mention.expect().equals(pair.get(1))) {
                        String other = mention.expect().equals(pair.get(0)) ? pair.get(1) : pair.get(0);
                        assertThat(resolver.resolve(mention.type(), mention.surface(), null).entityId())
                                .as("%s '%s' must not become %s", meeting.id(), mention.surface(), other).isNotEqualTo(other);
                    }
                }
            }
        }

        double recall = resolvable == 0 ? 1 : (double) resolvedRight / resolvable;
        double candidateRecall = candidatesExpected == 0 ? 1 : (double) candidatesFound / candidatesExpected;
        String report = "mentions " + KNOWLEDGE.mentionCount() + ", resolvable " + resolvable + " " + byHow + ", right "
                + resolvedRight + ", new " + newRight + "/" + newExpected + ", wrong merges " + wrongMerges + ", candidates "
                + candidatesFound + "/" + candidatesExpected + ", misses " + misses;
        System.out.println("RESOLUTION-EVAL " + report);

        assertThat(wrongMerges).as(report).isZero();
        assertThat(recall).as(report).isGreaterThanOrEqualTo(0.95);
        assertThat(candidateRecall).as(report).isGreaterThanOrEqualTo(0.90);
        assertThat(newRight).as(report).isEqualTo(newExpected);
        assertThat(byHow.keySet()).contains("glossary", "alias", "existing");
        assertThat(byHow.values().stream().collect(Collectors.summingInt(Integer::intValue))).isEqualTo(resolvable);
    }

    @Test
    void a_cited_glossary_id_of_the_right_type_wins_and_one_of_another_type_is_ignored() {
        EntityResolver resolver = resolver();

        assertThat(resolver.resolve("System", "whatever the model wrote", "sys-checkout").entityId()).isEqualTo("sys-checkout");
        assertThat(resolver.resolve("Term", "sla", "sys-checkout").entityId()).isEqualTo("term-sla");
    }
}
