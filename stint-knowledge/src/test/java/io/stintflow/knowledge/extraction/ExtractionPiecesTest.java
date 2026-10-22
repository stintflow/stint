package io.stintflow.knowledge.extraction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.KnowledgeValidationException;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.Source;
import io.stintflow.knowledge.WriteReceipt;
import io.stintflow.knowledge.extraction.BatchAssembler.Assembly;
import io.stintflow.knowledge.extraction.EntityResolver.Kind;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.knowledge.schema.SchemaRegistry;
import io.stintflow.knowledge.schema.SchemaValidator;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;

/**
 * SDD 3b.2: the schema, its validation, the glossary and the resolution of mentions, the duplicate criterion, the splitting of
 * a transcript and the assembly of a batch — everything of the extraction that needs no model.
 */
class ExtractionPiecesTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final KnowledgeSchema SCHEMA = KnowledgeSchema.meetings();
    private static final KnowledgeScope SCOPE = KnowledgeScope.of(new WorkflowRef("meetings", "notes", "1.0.0"));

    private static JsonNode json(String text) {
        try {
            return JSON.readTree(text.replace('\'', '"'));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    // --- CA1: the schema ---------------------------------------------------------------------------------------------

    @Test
    void ca1_the_reference_schema_has_the_seven_nodes_and_the_six_relations_and_a_version_is_immutable() {
        assertThat(SCHEMA.fullVersion()).isEqualTo("meetings@1.0.0");
        assertThat(SCHEMA.nodes().keySet()).containsExactlyInAnyOrder("Meeting", "Person", "Decision", "Requirement",
                "System", "Term", "ExternalReference");
        assertThat(SCHEMA.relations().keySet()).containsExactlyInAnyOrder("participated", "decided", "supersedes", "affects",
                "mentions", "motivated-by");
        assertThat(SCHEMA.nodes().get("Person").defaultSensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);

        SchemaRegistry registry = SchemaRegistry.withReferenceSchemas();
        assertThat(registry.resolve("meetings@1")).get().extracting(KnowledgeSchema::version).isEqualTo("1.0.0");
        JsonNode minor = json("{'schema':'meetings','version':'1.1.0','language':'en','nodes':{'Term':{'key':'name',"
                + "'defaultSensitivity':'internal','properties':{'name':{'type':'string','required':true}}}},'relations':{}}");
        registry.register(KnowledgeSchema.fromJson(minor));
        assertThat(registry.resolve("meetings@1")).get().extracting(KnowledgeSchema::version).isEqualTo("1.1.0");
        assertThat(registry.resolve("meetings@2")).isEmpty();
        JsonNode changed = json("{'schema':'meetings','version':'1.1.0','language':'en','nodes':{'Term':{'key':'name',"
                + "'defaultSensitivity':'confidential','properties':{'name':{'type':'string','required':true}}}},'relations':{}}");
        assertThatThrownBy(() -> registry.register(KnowledgeSchema.fromJson(changed)))
                .isInstanceOf(KnowledgeValidationException.class).hasMessageContaining("immutable");
        assertThatThrownBy(() -> registry.resolve("meetings")).isInstanceOf(KnowledgeValidationException.class);
    }

    // --- CA2 and CA3: what is out of the schema ----------------------------------------------------------------------

    private static final String VALID = "{'entities':["
            + "{'ref':'s1','type':'System','mention':'checkout service','glossaryId':'sys-checkout','passages':[0]},"
            + "{'ref':'p1','type':'Person','mention':'PERSON_a1b2c3','passages':[0,1]},"
            + "{'ref':'d1','type':'Decision','mention':'move the review','properties':{'summary':'Review moves to Friday',"
            + "'status':'accepted'},'passages':[1]}],"
            + "'relations':[{'type':'participated','from':'p1','to':'meeting'},{'type':'decided','from':'meeting','to':'d1'},"
            + "{'type':'affects','from':'d1','to':'s1'}]}";

    @Test
    void ca2_a_valid_answer_has_no_violation() {
        assertThat(SchemaValidator.violations(SCHEMA, json(VALID), 2)).isEmpty();
    }

    @Test
    void ca2_ca3_what_is_out_of_the_schema_is_named_by_field_and_rule_and_never_by_value() {
        String secret = "SYNTH-PII-0701";
        List<String> bad = List.of(
                "{'entities':[{'ref':'x','type':'Spaceship " + secret + "','mention':'m','passages':[0]}],'relations':[]}",
                "{'entities':[{'ref':'x','type':'System','mention':'m','passages':[0],'" + secret + "':1}],'relations':[]}",
                "{'entities':[{'ref':'x','type':'System','mention':'m','passages':[7]}],'relations':[]}",
                "{'entities':[{'ref':'x','type':'Meeting','mention':'" + secret + "','passages':[0]}],'relations':[]}",
                "{'entities':[{'ref':'x','type':'Person','mention':'Dana " + secret + " Rivera','passages':[0]}],'relations':[]}",
                "{'entities':[{'ref':'x','type':'Person','mention':'PERSON_a1b2c3','passages':[0],'properties':{'name':'"
                        + secret + "'}}],'relations':[]}",
                "{'entities':[{'ref':'x','type':'Decision','mention':'m','passages':[0],'properties':{'status':'maybe "
                        + secret + "'}}],'relations':[]}",
                "{'entities':[{'ref':'a','type':'System','mention':'m','passages':[0]},{'ref':'b','type':'Term','mention':'n',"
                        + "'passages':[0]}],'relations':[{'type':'affects','from':'a','to':'b'}]}",
                "{'entities':[{'ref':'a','type':'System','mention':'m','passages':[0]}],'relations':[{'type':'" + secret
                        + "','from':'a','to':'a'}]}",
                "{'entities':[{'ref':'a','type':'System','mention':'m','passages':[0]}],'relations':[{'type':'mentions',"
                        + "'from':'a','to':'" + secret + "'}]}",
                "{'entities':[],'extra':'" + secret + "'}",
                "['" + secret + "']");
        for (String answer : bad) {
            List<String> problems = SchemaValidator.violations(SCHEMA, json(answer), 2);
            assertThat(problems).as(answer).isNotEmpty();
            assertThat(String.join("\n", problems)).as("values in the violations of %s", answer)
                    .doesNotContain(secret).doesNotContain("Rivera");
        }
        // the specific rules
        assertThat(SchemaValidator.violations(SCHEMA, json(
                "{'entities':[{'ref':'x','type':'Person','mention':'PERSON_a1b2c3','passages':[0],'properties':{'name':'N'}}],"
                        + "'relations':[]}"), 2)).anyMatch(v -> v.contains("a Person takes no properties"));
        assertThat(SchemaValidator.violations(SCHEMA, json(
                "{'entities':[{'ref':'x','type':'Person','mention':'Dana Rivera','passages':[0]}],'relations':[]}"), 2))
                .anyMatch(v -> v.contains("a pseudonym, never a name"));
        assertThat(SchemaValidator.violations(SCHEMA, json(
                "{'entities':[{'ref':'a','type':'Decision','mention':'m','passages':[0],'properties':{'summary':'s'}},"
                        + "{'ref':'b','type':'Requirement','mention':'r','passages':[0],'properties':{'summary':'s'}}],"
                        + "'relations':[{'type':'supersedes','from':'a','to':'b'}]}"), 1))
                .anyMatch(v -> v.contains("two nodes of the same type"));
        assertThat(SchemaValidator.violations(SCHEMA, json(
                "{'entities':[{'ref':'d','type':'Decision','mention':'m','passages':[0]}],'relations':[]}"), 1))
                .anyMatch(v -> v.contains("summary: required for Decision"));
    }

    // --- CA5/CA6: resolution and duplicates --------------------------------------------------------------------------

    private static Glossary glossary() {
        return new Glossary("product", "1", List.of(
                new Glossary.Term("sys-checkout", "System", "Checkout Service", List.of("checkout", "the checkout svc"), null),
                new Glossary.Term("sys-payments-api", "System", "Payments API"),
                new Glossary.Term("sys-payments-ui", "System", "Payments UI"),
                new Glossary.Term("term-sla", "Term", "SLA", List.of("service level agreement"), "uptime promise")));
    }

    @Test
    void ca6_a_mention_resolves_by_exact_name_and_never_by_similarity() {
        EntityResolver resolver = new EntityResolver(glossary(), new EntityResolver.Existing() {
            @Override
            public List<EntityResolver.Known> exact(String type, String normalizedName) {
                return type.equals("Term") && normalizedName.equals("roadmap")
                        ? List.of(new EntityResolver.Known("e-roadmap", "Term", "Roadmap")) : List.of();
            }

            @Override
            public List<EntityResolver.Known> ofType(String type) {
                return List.of(new EntityResolver.Known("e-roadmap", "Term", "Roadmap"));
            }
        });

        assertThat(resolver.resolve("System", "anything", "sys-checkout")).extracting(r -> r.kind(), r -> r.entityId())
                .containsExactly(Kind.GLOSSARY, "sys-checkout");
        assertThat(resolver.resolve("Term", "service level agreement", null)).extracting(r -> r.kind(), r -> r.entityId())
                .containsExactly(Kind.ALIAS, "term-sla");
        assertThat(resolver.resolve("System", "The Checkout SVC!", null)).extracting(r -> r.kind(), r -> r.entityId())
                .containsExactly(Kind.ALIAS, "sys-checkout");
        assertThat(resolver.resolve("System", "checkout service", null).kind()).isEqualTo(Kind.GLOSSARY);
        assertThat(resolver.resolve("Term", "ROADMAP", null)).extracting(r -> r.kind(), r -> r.entityId())
                .containsExactly(Kind.EXISTING, "e-roadmap");
        // a cited id of another type is ignored; the name decides
        assertThat(resolver.resolve("Term", "sla", "sys-checkout").entityId()).isEqualTo("term-sla");

        // the homonyms: close, but two systems; neither merges into the other and each is a candidate of the other
        EntityResolver.Resolution api = resolver.resolve("System", "Payments Api v2", null);
        assertThat(api.kind()).isEqualTo(Kind.NEW);
        assertThat(api.entityId()).isEqualTo(EntityIds.of("System", "Payments Api v2"));
        assertThat(api.candidates()).extracting(EntityResolver.Candidate::other).contains("sys-payments-api");
        assertThat(resolver.resolve("System", "Payments UI", null).entityId()).isEqualTo("sys-payments-ui");   // exact
    }

    @Test
    void ca7_the_duplicate_criterion_is_by_type_alias_url_and_similarity_never_for_a_person() {
        var base = new DuplicateCriterion.Named("System", "Checkout Service", List.of("checkout"), "https://x.example/c");

        assertThat(DuplicateCriterion.compare(base, new DuplicateCriterion.Named("System", "Checkout", List.of(), null)))
                .get().extracting(DuplicateCriterion.Candidate::reasons).asList().contains("alias");
        assertThat(DuplicateCriterion.compare(base, new DuplicateCriterion.Named("System", "Other", List.of(),
                "https://x.example/c"))).get().extracting(DuplicateCriterion.Candidate::reasons).isEqualTo(List.of("url"));
        assertThat(DuplicateCriterion.compare(new DuplicateCriterion.Named("System", "Checkout Service"),
                new DuplicateCriterion.Named("System", "Checkout Servce"))).isPresent();   // a typo
        assertThat(DuplicateCriterion.compare(new DuplicateCriterion.Named("System", "Checkout"),
                new DuplicateCriterion.Named("Term", "Checkout"))).isEmpty();               // another type
        assertThat(DuplicateCriterion.compare(new DuplicateCriterion.Named("Person", "PERSON_a1b2c3"),
                new DuplicateCriterion.Named("Person", "PERSON_a1b2c4"))).isEmpty();        // never a person
        assertThat(DuplicateCriterion.compare(new DuplicateCriterion.Named("System", "Billing"),
                new DuplicateCriterion.Named("System", "Warehouse"))).isEmpty();
        assertThat(DuplicateCriterion.VERSION).isEqualTo("duplicate-criterion/1");
        assertThat(DuplicateCriterion.THRESHOLD).isEqualTo(0.85);
    }

    // --- CA8: the glossary in the prompt -----------------------------------------------------------------------------

    @Test
    void ca8_the_glossary_in_a_prompt_is_cut_at_forty_terms_and_fifteen_hundred_tokens_whatever_its_size() {
        List<Glossary.Term> terms = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 5_000; i++) {
            String name = "system" + i + " platform";
            terms.add(new Glossary.Term("sys-" + i, "System", name, List.of("sys" + i + " alias"), "a system of the product"));
            if (i % 5 == 0) {
                text.append(name).append(". ");   // 1 000 of them are named in the text
            }
        }
        Glossary glossary = new Glossary("big", "1", terms);

        GlossarySelector.Selection selection = GlossarySelector.select(glossary, text.toString(), ExtractionLimits.DEFAULT);

        assertThat(selection.terms()).hasSize(40);
        assertThat(selection.truncated()).isTrue();
        assertThat(selection.estimatedTokens()).isLessThanOrEqualTo(1_500);
        assertThat(selection.terms()).extracting(Glossary.Term::id).isSorted();   // stable order for the prefix
        // a small text names a few: no cut, only those
        GlossarySelector.Selection few = GlossarySelector.select(glossary, "We talked about system10 platform today.",
                ExtractionLimits.DEFAULT);
        assertThat(few.terms()).extracting(Glossary.Term::id).containsExactly("sys-10");
        assertThat(few.truncated()).isFalse();
        // a tighter budget in tokens cuts earlier
        assertThat(GlossarySelector.select(glossary, text.toString(), new ExtractionLimits(40, 100, 3000, 40000)).terms()
                .size()).isLessThan(40);
        assertThat(GlossarySelector.line(terms.get(0))).isEqualTo("sys-0 | system0 platform | sys0 alias");
    }

    // --- splitting ---------------------------------------------------------------------------------------------------

    @Test
    void passages_are_deterministic_bounded_and_grouped_into_windows() {
        List<PassageSplitter.Turn> turns = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            turns.add(new PassageSplitter.Turn("s" + (i % 2 + 1), "Turn number " + i + " of the weekly recap, with some words."));
        }
        List<String> first = PassageSplitter.split(turns, 300);
        List<String> second = PassageSplitter.split(turns, 300);

        assertThat(first).isEqualTo(second).allSatisfy(p -> assertThat(p.length()).isLessThanOrEqualTo(300));
        assertThat(String.join("\n", first)).contains("s1: Turn number 0").contains("s2: Turn number 29");
        assertThat(PassageSplitter.split(List.of(new PassageSplitter.Turn("s1", "word ".repeat(200))), 100))
                .hasSizeGreaterThan(5).allSatisfy(p -> assertThat(p.length()).isLessThanOrEqualTo(100));

        List<Passage> passages = PassageSplitter.passages(SCOPE, "meeting:m-1", first, "en", Sensitivity.CONFIDENTIAL);
        List<List<Passage>> windows = PassageSplitter.windows(passages, 150);
        assertThat(windows.stream().mapToInt(List::size).sum()).isEqualTo(passages.size());
        assertThat(windows).hasSizeGreaterThan(1);
        assertThat(passages).extracting(Passage::passageId).doesNotHaveDuplicates();
    }

    // --- assembling a batch and writing it ---------------------------------------------------------------------------

    @Test
    void the_assembled_batch_is_written_once_and_again_without_change_and_levels_follow_the_schema() throws Exception {
        List<Passage> passages = PassageSplitter.passages(SCOPE, "meeting:m-1",
                List.of("s1: PERSON_a1b2c3 says the checkout service needs the SLA.", "s2: Move the review to Friday."),
                "en", Sensitivity.CONFIDENTIAL);
        EntityResolver resolver = new EntityResolver(glossary(), EntityResolver.Existing.NONE);

        Assembly assembly = assemble(passages, resolver, Sensitivity.INTERNAL);

        assertThat(assembly.resolutions()).containsEntry("ALIAS", 1).containsEntry("NEW", 2);
        KnowledgeBatch batch = assembly.batch();
        assertThat(batch.schemaVersion()).isEqualTo("meetings@1.0.0");
        assertThat(batch.documentId()).isEqualTo("meeting:m-1");
        assertThat(batch.entities()).extracting(e -> e.type()).contains("Meeting", "Person", "System", "Decision");
        assertThat(batch.entities().stream().filter(e -> e.type().equals("Person")).findFirst().get().sensitivity())
                .isEqualTo(Sensitivity.CONFIDENTIAL);   // the type's default beats an internal instance
        assertThat(batch.entities().stream().filter(e -> e.type().equals("System")).findFirst().get().sensitivity())
                .isEqualTo(Sensitivity.INTERNAL);
        assertThat(batch.relations()).extracting(r -> r.type()).containsExactlyInAnyOrder("participated", "decided");
        assertThat(batch.relations().stream().filter(r -> r.type().equals("participated")).findFirst().get().sensitivity())
                .isEqualTo(Sensitivity.CONFIDENTIAL);   // at least that of both ends
        assertThat(batch.links()).hasSize(3);
        assertThat(assembly.duplicateCandidates()).isEmpty();

        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(Files.createTempDirectory("stint-3b2"), () -> Instant.parse("2026-10-19T12:00:00Z")),
                new RetentionClass("restricted", Duration.ofDays(30)), () -> Instant.parse("2026-10-19T12:00:00Z"));
        WriteReceipt first = store.write(SCOPE, batch).toCompletableFuture().get(10, TimeUnit.SECONDS);
        WriteReceipt again = store.write(SCOPE, assemble(passages, resolver, Sensitivity.INTERNAL).batch())
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(first.status()).isEqualTo(WriteReceipt.Status.APPLIED);
        assertThat(again.status()).isEqualTo(WriteReceipt.Status.UNCHANGED);   // the same extraction, written twice
        assertThat(store.changes(SCOPE, 0, 1000).toCompletableFuture().get(10, TimeUnit.SECONDS))
                .allSatisfy(c -> assertThat(c.schemaVersion()).isEqualTo("meetings@1.0.0"));
    }

    // --- SDD 3c.3b: the name of a glossary term does not inherit the sensitivity of the meeting ----------------------

    private static Sensitivity levelOf(Assembly assembly, String type) {
        return assembly.batch().entities().stream().filter(e -> e.type().equals(type)).findFirst().orElseThrow().sensitivity();
    }

    private static List<Passage> passagesOf(Sensitivity level) {
        return PassageSplitter.passages(SCOPE, "meeting:m-1",
                List.of("s1: PERSON_a1b2c3 says the checkout service needs the SLA.", "s2: Move the review to Friday."), "en", level);
    }

    @Test
    void ca1_a_system_the_glossary_resolved_keeps_the_level_of_the_schema_in_a_confidential_meeting_and_what_was_said_does_not() {
        Assembly assembly = assemble(passagesOf(Sensitivity.CONFIDENTIAL), new EntityResolver(glossary(), EntityResolver.Existing.NONE),
                Sensitivity.CONFIDENTIAL);

        assertThat(assembly.resolutions()).containsEntry("ALIAS", 1);
        assertThat(levelOf(assembly, "System")).isEqualTo(Sensitivity.INTERNAL);
        assertThat(levelOf(assembly, "Decision")).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(levelOf(assembly, "Meeting")).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(levelOf(assembly, "Person")).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(assembly.batch().relations()).allSatisfy(r -> assertThat(r.sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL));
        assertThat(assembly.batch().links()).hasSize(3);
    }

    @Test
    void ca2_a_name_the_glossary_does_not_know_keeps_the_level_of_the_meeting() {
        Glossary withoutCheckout = new Glossary("product", "1", List.of(new Glossary.Term("term-sla", "Term", "SLA")));

        Assembly assembly = assemble(passagesOf(Sensitivity.CONFIDENTIAL), new EntityResolver(withoutCheckout, EntityResolver.Existing.NONE),
                Sensitivity.CONFIDENTIAL);

        assertThat(assembly.resolutions()).containsEntry("NEW", 3).doesNotContainKey("ALIAS");
        assertThat(levelOf(assembly, "System")).isEqualTo(Sensitivity.CONFIDENTIAL);
    }

    @Test
    void ca2_a_name_that_was_already_in_the_base_outside_the_glossary_keeps_the_level_of_the_meeting() {
        EntityResolver.Existing known = new EntityResolver.Existing() {
            @Override
            public List<EntityResolver.Known> exact(String type, String normalizedName) {
                return type.equals("System") && normalizedName.equals("checkout")
                        ? List.of(new EntityResolver.Known("e-existing", "System", "Checkout")) : List.of();
            }

            @Override
            public List<EntityResolver.Known> ofType(String type) {
                return List.of();
            }
        };
        Glossary none = new Glossary("product", "1", List.of());

        Assembly assembly = assemble(passagesOf(Sensitivity.CONFIDENTIAL), new EntityResolver(none, known), Sensitivity.CONFIDENTIAL);

        assertThat(assembly.resolutions()).containsEntry("EXISTING", 1);
        assertThat(levelOf(assembly, "System")).isEqualTo(Sensitivity.CONFIDENTIAL);
    }

    @Test
    void ca3_in_a_restricted_meeting_the_rule_does_not_apply() {
        Assembly assembly = assemble(passagesOf(Sensitivity.RESTRICTED), new EntityResolver(glossary(), EntityResolver.Existing.NONE),
                Sensitivity.RESTRICTED);

        assertThat(assembly.resolutions()).containsEntry("ALIAS", 1);
        assertThat(levelOf(assembly, "System")).isEqualTo(Sensitivity.RESTRICTED);
    }

    @Test
    void ca4_below_confidential_the_reader_sees_the_name_and_not_what_the_meeting_said() throws Exception {
        Assembly assembly = assemble(passagesOf(Sensitivity.CONFIDENTIAL), new EntityResolver(glossary(), EntityResolver.Existing.NONE),
                Sensitivity.CONFIDENTIAL);
        CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(),
                new FilesystemBlobStore(Files.createTempDirectory("stint-3c3b"), () -> Instant.parse("2026-10-22T12:00:00Z")),
                new RetentionClass("restricted", Duration.ofDays(30)), () -> Instant.parse("2026-10-22T12:00:00Z"));
        store.write(SCOPE, assembly.batch()).toCompletableFuture().get(10, TimeUnit.SECONDS);

        var view = store.view(SCOPE, new io.stintflow.knowledge.ViewQuery(Sensitivity.INTERNAL, 100, null, null))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(view.nodes()).extracting(n -> n.type()).containsExactly("System");
        assertThat(view.nodes().getFirst().label()).isEqualTo("Checkout Service");
        assertThat(view.edges()).isEmpty();
        var found = store.entities(SCOPE, new io.stintflow.knowledge.EntityQuery("System",
                io.stintflow.knowledge.EntityIds.normalize("Checkout Service"), Sensitivity.INTERNAL, 5)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(found).hasSize(1);
        var seen = store.retrieve(SCOPE, new io.stintflow.knowledge.RetrievalQuery("checkout service needs", 5, 0, Sensitivity.INTERNAL,
                null, java.util.Set.of(), null, 2)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(seen.toString()).doesNotContain("PERSON_a1b2c3").doesNotContain("Move the review").doesNotContain("Review on Friday");
    }

    private static Assembly assemble(List<Passage> passages, EntityResolver resolver, Sensitivity level) {
        BatchAssembler assembler = new BatchAssembler(SCOPE, SCHEMA, "m-1", "2026-10-19", level, resolver);
        assembler.addWindow(passages, json("{'entities':["
                + "{'ref':'s1','type':'System','mention':'checkout','passages':[0]},"
                + "{'ref':'p1','type':'Person','mention':'PERSON_a1b2c3','passages':[0]},"
                + "{'ref':'d1','type':'Decision','mention':'move the review to Friday','properties':{'summary':'Review on Friday'},"
                + "'passages':[1]}],"
                + "'relations':[{'type':'participated','from':'p1','to':'meeting'},"
                + "{'type':'decided','from':'meeting','to':'d1'}]}"));
        return assembler.build("extract-m-1", new Source("inst-1", "chain-1", new WorkflowRef("meetings", "notes", "1.0.0")));
    }

    @SuppressWarnings("unused")
    private static Map<String, String> unused() {
        return Map.of();
    }
}
