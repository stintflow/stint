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
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;

/** SDD 3b.3, CA6: the ceiling is the level of the instance, lowered by maxLevel and never raised; the audit holds counts only. */
class KnowledgeRetrieveHandlerTest {

    static final Instant T0 = Instant.parse("2026-10-20T10:00:00Z");

    KnowledgeFixture fx;
    InMemoryAuditLog audit;
    KnowledgeRetrieveHandler handler;

    @BeforeEach
    void setUp() throws Exception {
        fx = new KnowledgeFixture();
        audit = new InMemoryAuditLog();
        handler = new KnowledgeRetrieveHandler(fx.store, KnowledgeFixture.allowed(), audit, InstantSource.fixed(T0));
        fx.store.write(KnowledgeFixture.SCOPE, KnowledgeFixture.batch("m-int", 1L, Sensitivity.INTERNAL,
                "Internal note about the checkout retry policy.")).toCompletableFuture().join();
        fx.store.write(KnowledgeFixture.SCOPE, KnowledgeFixture.batch("m-con", 1L, Sensitivity.CONFIDENTIAL,
                "Confidential note about the checkout retry policy.")).toCompletableFuture().join();
        fx.store.write(KnowledgeFixture.SCOPE, KnowledgeFixture.batch("m-res", 1L, Sensitivity.RESTRICTED,
                "Restricted note about the checkout retry policy.")).toCompletableFuture().join();
    }

    private ObjectNode query() {
        ObjectNode in = Json.obj();
        in.put("query", "checkout retry policy").put("topK", 10);
        in.set("scope", KnowledgeFixture.scopeNode());
        return in;
    }

    private JsonNode ask(ObjectNode in, Sensitivity instanceLevel) {
        return handler.execute(KnowledgeFixture.task("i-9", in, instanceLevel)).toCompletableFuture().join();
    }

    private static List<String> levels(JsonNode answer) {
        return answer.get("items").findValuesAsText("level");
    }

    @Test
    void a_confidential_instance_never_receives_a_restricted_item() {
        JsonNode answer = ask(query(), Sensitivity.CONFIDENTIAL);
        assertThat(levels(answer)).isNotEmpty().doesNotContain("restricted");
        assertThat(answer.get("counts").get("restricted").asInt()).isZero();
        assertThat(answer.toString()).doesNotContain("Restricted note");
    }

    @Test
    void max_level_only_lowers_the_ceiling() {
        ObjectNode lowered = query();
        lowered.put("maxLevel", "internal");
        assertThat(levels(ask(lowered, Sensitivity.CONFIDENTIAL))).isNotEmpty().containsOnly("internal");

        ObjectNode raised = query();
        raised.put("maxLevel", "restricted");   // asking for more than the instance has changes nothing
        assertThat(levels(ask(raised, Sensitivity.CONFIDENTIAL))).doesNotContain("restricted");
    }

    @Test
    void an_instance_with_no_level_reads_as_restricted() {
        assertThat(levels(ask(query(), null))).contains("restricted", "confidential", "internal");
    }

    @Test
    void the_audit_holds_the_base_the_ceiling_and_counts_but_neither_the_question_nor_a_content() {
        ObjectNode in = query();
        ask(in, Sensitivity.CONFIDENTIAL);
        List<AuditRecord> records = audit.of("i-9").toCompletableFuture().join();
        assertThat(records).hasSize(1);
        AuditRecord record = records.get(0);
        assertThat(record.kind()).isEqualTo(KnowledgeRetrieveHandler.AUDIT_KIND);
        assertThat(record.at()).isEqualTo(T0);
        assertThat(record.sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(record.detail()).containsEntry("base", "examples/product-meetings").containsEntry("ceiling", "confidential")
                .containsEntry("restricted", "0").containsKeys("internal", "confidential", "topK");
        assertThat(record.toString()).doesNotContain("checkout retry policy", "note about");
    }

    @Test
    void without_an_audit_log_nothing_is_recorded_and_the_read_still_works() {
        KnowledgeRetrieveHandler quiet = new KnowledgeRetrieveHandler(fx.store, KnowledgeFixture.allowed(), null,
                InstantSource.fixed(T0));
        JsonNode answer = quiet.execute(KnowledgeFixture.task("i-9", query(), Sensitivity.CONFIDENTIAL)).toCompletableFuture()
                .join();
        assertThat(answer.get("items")).isNotEmpty();
        assertThat(audit.all()).isEmpty();
    }

    @Test
    void items_carry_their_entities_and_origin() {
        // the entity is shared by the three meetings, so it is as high as the highest of them: only a restricted instance sees it
        JsonNode first = ask(query(), Sensitivity.RESTRICTED).get("items").get(0);
        assertThat(first.get("kind").asText()).isEqualTo("PASSAGE");
        assertThat(first.get("origin").get("documentId").asText()).startsWith("meeting:");
        assertThat(first.get("entities").findValuesAsText("label")).contains("Checkout Service");
        JsonNode lower = ask(query(), Sensitivity.CONFIDENTIAL).get("items").get(0);
        assertThat(lower.get("entities").findValuesAsText("label")).doesNotContain("Checkout Service");
    }

    @Test
    void a_scope_not_in_the_list_and_a_missing_query_are_refused() {
        ObjectNode other = query();
        ((ObjectNode) other.get("scope")).put("base", "hr");
        assertThatThrownBy(() -> ask(other, Sensitivity.CONFIDENTIAL)).hasCauseInstanceOf(TaskFailure.class)
                .cause().extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(403);

        ObjectNode blank = query();
        blank.put("query", " ");
        assertThatThrownBy(() -> ask(blank, Sensitivity.CONFIDENTIAL)).cause()
                .extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(400);

        ObjectNode badLevel = query();
        badLevel.put("maxLevel", "secret");
        assertThatThrownBy(() -> ask(badLevel, Sensitivity.CONFIDENTIAL)).cause()
                .extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(400);

        ObjectNode tooMany = query();
        tooMany.put("topK", 500);
        assertThatThrownBy(() -> ask(tooMany, Sensitivity.CONFIDENTIAL)).cause()
                .extracting(c -> ((TaskFailure) c).toErrorInfo().status()).isEqualTo(400);
    }
}
