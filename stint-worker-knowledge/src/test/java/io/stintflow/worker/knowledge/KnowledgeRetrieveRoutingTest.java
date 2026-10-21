package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.time.InstantSource;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Relation;
import io.stintflow.knowledge.Source;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3b.4, CA1, CA5, CA6: the mode a question is answered in, the graph followed from what it names, the budget and the audit.
 * Synthetic data only.
 */
class KnowledgeRetrieveRoutingTest {

    static final Instant T0 = Instant.parse("2026-10-21T10:00:00Z");
    static final String OLD = EntityIds.of("Decision", "Old retry policy");
    static final String NEW = EntityIds.of("Decision", "New retry policy");

    KnowledgeFixture fx;
    InMemoryAuditLog audit;
    KnowledgeRetrieveHandler handler;

    private static Entity entity(String type, String name) {
        return new Entity(EntityIds.of(type, name), type, name, Sensitivity.INTERNAL);
    }

    private static KnowledgeBatch doc(String meeting, String text, List<Entity> entities, List<Relation> relations,
            Entity mentioned) {
        String documentId = "meeting:" + meeting;
        Passage passage = new Passage(PassageIds.of(KnowledgeFixture.SCOPE, documentId, 0, text), 0, text, "en",
                Sensitivity.INTERNAL);
        return new KnowledgeBatch("extract:" + meeting, documentId, PassageIds.sha256Hex(text + entities), 1L,
                new Source("i-" + meeting, "chn-" + meeting, KnowledgeFixture.DEFINITION), "meetings@1.0.0", List.of(passage),
                entities, relations, List.of(new Link(passage.passageId(), mentioned.entityId())));
    }

    @BeforeEach
    void setUp() throws Exception {
        fx = new KnowledgeFixture();
        audit = new InMemoryAuditLog();
        handler = new KnowledgeRetrieveHandler(fx.store, KnowledgeFixture.allowed(), audit, InstantSource.fixed(T0));
        Entity old = entity("Decision", "Old retry policy");
        Entity renewed = entity("Decision", "New retry policy");
        Entity checkout = entity("System", "Checkout Service");
        fx.store.write(KnowledgeFixture.SCOPE, doc("m-001", "We keep the old retry policy for checkout.", List.of(old, checkout),
                List.of(new Relation("affects", old.entityId(), checkout.entityId(), Sensitivity.INTERNAL)), old))
                .toCompletableFuture().join();
        fx.store.write(KnowledgeFixture.SCOPE, doc("m-002", "The plan for payments is a longer story.", List.of(renewed, old),
                List.of(new Relation("supersedes", renewed.entityId(), old.entityId(), Sensitivity.INTERNAL)), renewed))
                .toCompletableFuture().join();
    }

    private ObjectNode query(String question) {
        ObjectNode in = Json.obj();
        in.put("query", question).put("topK", 20);
        in.set("scope", KnowledgeFixture.scopeNode());
        return in;
    }

    private JsonNode ask(ObjectNode in) {
        return handler.execute(KnowledgeFixture.task("i-7", in, Sensitivity.INTERNAL)).toCompletableFuture().join();
    }

    private static List<String> relations(JsonNode answer) {
        return items(answer).stream().filter(i -> i.get("kind").asText().equals("RELATION")).map(i -> i.get("content").asText())
                .toList();
    }

    private static List<JsonNode> items(JsonNode answer) {
        List<JsonNode> all = new java.util.ArrayList<>();
        answer.get("items").forEach(all::add);
        return all;
    }

    @Test
    void ca1_a_question_that_names_a_decision_is_answered_through_the_graph_and_says_where_the_answer_was_said() {
        JsonNode answer = ask(query("Which decision superseded the old retry policy?"));

        assertThat(answer.get("mode").asText()).isEqualTo("relational");
        assertThat(relations(answer)).contains("New retry policy -[supersedes]-> Old retry policy");
        JsonNode supersedes = items(answer).stream().filter(i -> i.get("content").asText().contains("supersedes")).findFirst()
                .orElseThrow();
        assertThat(supersedes.get("origin").get("evidence")).extracting(JsonNode::asText).containsExactly("meeting:m-002");
    }

    @Test
    void ca1_the_same_question_by_meaning_alone_does_not_follow_the_graph() {
        ObjectNode in = query("Which decision superseded the old retry policy?");
        in.put("mode", "semantic");

        JsonNode answer = ask(in);

        assertThat(answer.get("mode").asText()).isEqualTo("semantic");
        assertThat(relations(answer)).isEmpty();
    }

    @Test
    void a_question_that_names_nothing_the_base_knows_is_answered_by_meaning_and_a_forced_relational_mode_falls_back() {
        ObjectNode in = query("retry policy plan");

        assertThat(ask(in).get("mode").asText()).isEqualTo("semantic");
        in.put("mode", "relational");
        JsonNode answer = ask(in);
        assertThat(answer.get("mode").asText()).isEqualTo("semantic");   // no entity named: not an error
        assertThat(answer.get("items")).isNotEmpty();
    }

    @Test
    void names_given_by_the_caller_seed_the_graph_and_a_deeper_hops_goes_further() {
        ObjectNode in = query("what does it affect");
        in.putArray("entities").add("new retry policy");

        JsonNode shallow = ask(in);
        in.put("hops", 3);
        JsonNode deep = ask(in);

        assertThat(shallow.get("mode").asText()).isEqualTo("relational");
        assertThat(relations(shallow)).contains("New retry policy -[supersedes]-> Old retry policy")
                .doesNotContain("Old retry policy -[affects]-> Checkout Service");
        assertThat(relations(deep)).contains("Old retry policy -[affects]-> Checkout Service");
    }

    @Test
    void ca5_the_budget_keeps_the_best_whole_items_and_says_how_many_it_left_out() {
        ObjectNode in = query("Which decision superseded the old retry policy?");
        JsonNode all = ask(in);
        int total = all.get("items").size();
        assertThat(all.get("trimmed").asInt()).isZero();

        in.put("budgetTokens", 40);
        JsonNode cut = ask(in);

        assertThat(cut.get("items").size()).isLessThan(total);
        assertThat(cut.get("trimmed").asInt()).isEqualTo(total - cut.get("items").size());
        for (int i = 0; i < cut.get("items").size(); i++) {
            assertThat(cut.get("items").get(i)).isEqualTo(all.get("items").get(i));   // the best first, whole, in the same order
        }
        in.put("budgetTokens", 0);
        assertThat(ask(in).get("items")).isEmpty();
    }

    @Test
    void ca6_the_audit_has_the_mode_the_hops_and_the_counts_but_not_the_question_nor_the_names() {
        ObjectNode in = query("Which decision superseded the old retry policy?");
        in.put("budgetTokens", 40);
        ask(in);

        List<AuditRecord> records = audit.of("i-7").toCompletableFuture().join();

        assertThat(records).hasSize(1);
        assertThat(records.get(0).detail()).containsEntry("mode", "relational").containsEntry("hops", "2")
                .containsKeys("trimmed", "internal", "confidential", "restricted");
        assertThat(records.get(0).toString()).doesNotContain("superseded", "retry policy", "Old retry", "m-00");
    }

    @Test
    void a_mode_or_a_budget_that_does_not_exist_is_refused() {
        ObjectNode mode = query("anything");
        mode.put("mode", "fuzzy");
        assertThatThrownBy(() -> ask(mode)).cause().extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(400);

        ObjectNode budget = query("anything");
        budget.put("budgetTokens", -5);
        assertThatThrownBy(() -> ask(budget)).cause().extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(400);

        ObjectNode hops = query("anything");
        hops.put("hops", 4);
        assertThatThrownBy(() -> ask(hops)).cause().extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(400);
    }
}
