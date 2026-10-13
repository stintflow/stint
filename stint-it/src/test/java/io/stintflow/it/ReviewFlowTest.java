package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.StringWriter;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.dsl.DslValidationException;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.CorrectionLog.Correction;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.wire.review.CorrectionExport;

/**
 * SDD 3a.4: human review on {@code listen}. An uncertain decision waits for its reviewer and follows the branch of the
 * option the human chose (CA1); without an answer, a reminder, then an escalation, then the default (CA2); the
 * correction is recorded and exported with the decision's record intact (CA3). An answer from another role, from an
 * untrusted source, out of contract or late doesn't resume and is audited; a duplicate is idempotent; an approval
 * routes on its verdict and names rejected items. Controllable clock, no sleep, synthetic ids and pointers only.
 */
class ReviewFlowTest {

    static final String PORTAL = "https://portal.sandbox.example";
    static final WorkflowRef TRIAGE = new WorkflowRef("meetings", "triage", "1.0.0");
    static final WorkflowRef APPROVE = new WorkflowRef("meetings", "approve", "1.0.0");
    static final String MATERIAL = "s3://stint-blobs/redacted/meeting-1.md";

    static final String POLICY = """
            routes:
              restricted: ['*']
              confidential: ['*']
              internal: ['*']
            review:
              sources: [https://portal.sandbox.example]""";

    static final String TRIAGE_YAML = SensitivityPolicyTest.header("triage", POLICY) + """
            do:
              - meetingType:
                  call: decision
                  with:
                    task: decide-type
                    kind: choice
                    input: ${ . }
                    options:
                      - ata: { description: "Minutes", then: minutes }
                      - adr: { description: "A decision record", then: record }
                      - out: { description: "Out of scope", then: end }
                  onUncertain:
                    review:
                      role: business-rep
                      material: ${ $context.ref }
                      deadline: { hours: 24 }
                      remind: { hours: 20 }
                      escalate: { role: technical-rep, after: { hours: 24 } }
                      onTimeout: { value: out }
                  export: { as: '${ $context + {type: .value, reviewed: .reviewed} }' }
              - minutes:
                  set: '${ {done: "minutes"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - record:
                  set: '${ {done: "record"} }'
                  export: { as: '${ $context + . }' }
                  then: end
            """;

    static final String APPROVE_YAML = SensitivityPolicyTest.header("approve", POLICY) + """
            do:
              - approveSdds:
                  call: review
                  with:
                    kind: approval
                    role: technical-rep
                    material: ${ .batchRef }
                    items: ${ .sdds }
                    deadline: { hours: 48 }
                  onReject: revise
                  export: { as: '${ $context + {verdict: .verdict, rejected: .rejectedItems, note: .noteRef} }' }
                  then: publish
              - revise:
                  set: '${ {done: "revise"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - publish:
                  set: '${ {done: "publish"} }'
                  export: { as: '${ $context + . }' }
            """;

    /** A decision worker that is never sure: it says {@code ata} with low confidence. */
    private static PolicyFixture triage() throws Exception {
        return new PolicyFixture(TRIAGE_YAML).worker("decide-type", in -> Json.obj().put("value", "ata")
                .put("confidence", 0.41).put("uncertain", true)
                .set("provenance", Json.obj().put("adapter", "litjev").put("provider", "litjev")
                        .put("model", "synthetic/qwen").put("version", "1")));
    }

    private static List<CloudEvent> published(PolicyFixture f, String type) {
        return f.publisher.published().stream().filter(e -> e.getType().equals(type)).toList();
    }

    private static CloudEvent awaitPublished(PolicyFixture f, String type, int count) {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (published(f, type).size() < count && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
        List<CloudEvent> events = published(f, type);
        assertThat(events).as("published %s", type).hasSizeGreaterThanOrEqualTo(count);
        return events.get(count - 1);
    }

    private static void awaitAudit(PolicyFixture f, String id, Predicate<AuditRecord> match) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until
                && f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS).stream().noneMatch(match)) {
            Thread.onSpinWait();
        }
        assertThat(f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS)).anyMatch(match);
    }

    static CloudEvent answer(String eventId, String reviewId, String source, String role, ObjectNode data) {
        return CloudEventBuilder.v1().withId(eventId).withSource(URI.create(source))
                .withType(StintEvents.TYPE_REVIEW_COMPLETED).withSubject(reviewId)
                .withDataContentType("application/json").withData(Json.bytes(data))
                .withExtension(StintEvents.EXT_ROLE, role).build();
    }

    private static void deliver(PolicyFixture f, CloudEvent event) throws Exception {
        f.engine.onDomainEvent(event, Duration.ofMinutes(5)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static List<AuditRecord> reviewAudit(PolicyFixture f, String id) throws Exception {
        return f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
                .filter(r -> "review".equals(r.kind())).toList();
    }

    private static void advance(PolicyFixture f, Duration by) {
        f.now.set(f.now.get().plus(by));
        f.timer.tick();
    }

    @Test
    void ca1_an_uncertain_decision_waits_for_its_reviewer_and_follows_the_chosen_branch() throws Exception {
        PolicyFixture f = triage();
        String id = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL), Sensitivity.INTERNAL);

        CloudEvent request = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1);
        String reviewId = request.getSubject();
        assertThat(reviewId).isEqualTo(id + ":/do/0/meetingType/review:1");
        assertThat(request.getExtension(StintEvents.EXT_ROLE)).isEqualTo("business-rep");
        JsonNode asked = Json.read(request.getData().toBytes());
        assertThat(asked.path("materialRef").asText()).isEqualTo(MATERIAL);
        assertThat(asked.path("options").findValuesAsText("name")).containsExactly("ata", "adr", "out");
        assertThat(asked.path("deadline").asText()).isEqualTo(PolicyFixture.T0.plus(Duration.ofHours(24)).toString());
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.WAITING);

        deliver(f, answer("ans-1", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "rev-7f3a")));

        JsonNode done = f.awaitCompleted(id);
        assertThat(done.path("done").asText()).isEqualTo("record");
        assertThat(done.path("type").asText()).isEqualTo("adr");
        assertThat(done.path("reviewed").path("originalValue").asText()).isEqualTo("ata");
        JsonNode decisions = f.snapshot(id).decisions();
        assertThat(decisions.path("/do/0/meetingType").path("value").asText()).isEqualTo("ata");   // intact
        assertThat(decisions.path("/do/0/meetingType").path("uncertain").asBoolean()).isTrue();
        JsonNode review = decisions.path("/do/0/meetingType/review");
        assertThat(review.path("answer").path("value").asText()).isEqualTo("adr");
        assertThat(review.path("answeredBy").asText()).isEqualTo("business-rep");
        assertThat(review.path("reviewer").asText()).isEqualTo("rev-7f3a");
        assertThat(reviewAudit(f, id)).extracting(AuditRecord::outcome).containsExactly("requested", "answered");
    }

    @Test
    void ca2_without_an_answer_a_reminder_then_an_escalation_then_the_default() throws Exception {
        PolicyFixture f = triage();
        String id = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL), Sensitivity.INTERNAL);
        String reviewId = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getSubject();

        advance(f, Duration.ofHours(20));
        CloudEvent reminder = awaitPublished(f, StintEvents.TYPE_REVIEW_REMINDER, 1);
        assertThat(reminder.getSubject()).isEqualTo(reviewId);
        assertThat(reminder.getExtension(StintEvents.EXT_ROLE)).isEqualTo("business-rep");

        advance(f, Duration.ofHours(4));
        CloudEvent escalated = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 2);
        assertThat(escalated.getSubject()).isEqualTo(reviewId);
        assertThat(escalated.getExtension(StintEvents.EXT_ROLE)).isEqualTo("technical-rep");
        assertThat(Json.read(escalated.getData().toBytes()).path("escalatedFrom").asText()).isEqualTo("business-rep");

        advance(f, Duration.ofHours(24));
        JsonNode done = f.awaitCompleted(id);
        assertThat(done.path("type").asText()).isEqualTo("out");   // onTimeout: {value: out}, then: end
        assertThat(done.path("reviewed").path("defaulted").asBoolean()).isTrue();
        assertThat(reviewAudit(f, id)).extracting(AuditRecord::outcome)
                .containsExactly("requested", "reminded", "escalated");
        assertThat(f.corrections.of(TRIAGE).toCompletableFuture().get()).isEmpty();   // a default is no correction
    }

    @Test
    void after_an_escalation_the_earlier_role_is_late_and_the_next_role_answers() throws Exception {
        PolicyFixture f = triage();
        String id = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL), Sensitivity.INTERNAL);
        String reviewId = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getSubject();
        advance(f, Duration.ofHours(20));
        awaitPublished(f, StintEvents.TYPE_REVIEW_REMINDER, 1);
        advance(f, Duration.ofHours(4));
        awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 2);

        deliver(f, answer("late-1", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "ata").put("reviewer", "rev-1")));
        awaitAudit(f, id, r -> "late".equals(r.outcome()));
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.WAITING);

        deliver(f, answer("ans-2", reviewId, PORTAL, "technical-rep",
                Json.obj().put("value", "ata").put("reviewer", "rev-2")));
        assertThat(f.awaitCompleted(id).path("done").asText()).isEqualTo("minutes");
        Correction correction = f.corrections.of(TRIAGE).toCompletableFuture().get().getFirst();
        assertThat(correction.escalated()).isTrue();
        assertThat(correction.role()).isEqualTo("technical-rep");
    }

    @Test
    void wrong_role_untrusted_source_and_out_of_contract_answers_never_resume() throws Exception {
        PolicyFixture f = triage();
        String id = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL), Sensitivity.INTERNAL);
        String reviewId = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getSubject();

        deliver(f, answer("x-1", reviewId, PORTAL, "technician",
                Json.obj().put("value", "adr").put("reviewer", "rev-1")));
        deliver(f, answer("x-2", reviewId, "https://evil.example", "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "rev-1")));
        deliver(f, answer("x-3", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "rev-1").put("note", "free text")));
        deliver(f, answer("x-4", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "not-an-option").put("reviewer", "rev-1")));
        deliver(f, answer("x-5", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "someone@example.com")));

        awaitAudit(f, id, r -> "out-of-contract".equals(r.outcome()) && "x-5".equals(r.detail().get("eventId")));
        assertThat(reviewAudit(f, id)).extracting(AuditRecord::outcome).containsExactly("requested", "wrong-role",
                "untrusted-source", "out-of-contract", "out-of-contract", "out-of-contract");
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.WAITING);

        deliver(f, answer("ok", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "rev-1")));
        assertThat(f.awaitCompleted(id).path("done").asText()).isEqualTo("record");
    }

    @Test
    void a_duplicate_answer_is_idempotent() throws Exception {
        PolicyFixture f = triage();
        String id = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL), Sensitivity.INTERNAL);
        String reviewId = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getSubject();
        CloudEvent once = answer("ans-1", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "rev-1"));

        deliver(f, once);
        f.awaitCompleted(id);
        long version = f.snapshot(id).version();
        deliver(f, once);
        deliver(f, answer("ans-2", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "ata").put("reviewer", "rev-1")));

        assertThat(f.snapshot(id).version()).isEqualTo(version);
        assertThat(f.snapshot(id).context().path("done").asText()).isEqualTo("record");
        assertThat(f.corrections.of(TRIAGE).toCompletableFuture().get()).hasSize(1);
    }

    @Test
    void an_approval_routes_on_its_verdict_and_names_the_rejected_items() throws Exception {
        PolicyFixture f = new PolicyFixture(APPROVE_YAML);
        String approved = f.engine.start(APPROVE, Json.obj().put("batchRef", MATERIAL)
                .set("sdds", Json.MAPPER.createArrayNode().add("sdd-1").add("sdd-2")), Sensitivity.INTERNAL);
        String reviewA = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getSubject();
        assertThat(Json.read(awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getData().toBytes())
                .path("items").toString()).isEqualTo("[\"sdd-1\",\"sdd-2\"]");

        ObjectNode all = Json.obj().put("reviewer", "rev-9");
        all.putArray("items").add(Json.obj().put("id", "sdd-1").put("verdict", "approved"))
                .add(Json.obj().put("id", "sdd-2").put("verdict", "approved"));
        deliver(f, answer("a-1", reviewA, PORTAL, "technical-rep", all));
        assertThat(f.awaitCompleted(approved).path("done").asText()).isEqualTo("publish");

        String partial = f.engine.start(APPROVE, Json.obj().put("batchRef", MATERIAL)
                .set("sdds", Json.MAPPER.createArrayNode().add("sdd-1").add("sdd-2")), Sensitivity.INTERNAL);
        String reviewB = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 2).getSubject();
        ObjectNode some = Json.obj().put("reviewer", "rev-9").put("noteRef", "s3://notes/n-1.md");
        some.putArray("items").add(Json.obj().put("id", "sdd-1").put("verdict", "approved"))
                .add(Json.obj().put("id", "sdd-2").put("verdict", "rejected").put("noteRef", "s3://notes/n-2.md"));
        ObjectNode missingOne = Json.obj().put("reviewer", "rev-9");
        missingOne.putArray("items").add(Json.obj().put("id", "sdd-1").put("verdict", "approved"));
        deliver(f, answer("b-0", reviewB, PORTAL, "technical-rep", missingOne));   // every item needs a verdict
        deliver(f, answer("b-1", reviewB, PORTAL, "technical-rep", some));

        JsonNode done = f.awaitCompleted(partial);
        assertThat(done.path("done").asText()).isEqualTo("revise");
        assertThat(done.path("verdict").asText()).isEqualTo("rejected");
        assertThat(done.path("rejected").toString()).isEqualTo("[\"sdd-2\"]");
        assertThat(done.path("note").asText()).isEqualTo("s3://notes/n-1.md");
        assertThat(reviewAudit(f, partial)).extracting(AuditRecord::outcome)
                .containsExactly("requested", "out-of-contract", "answered");
    }

    @Test
    void ca3_the_correction_is_exported_with_the_decision_s_record_and_by_level() throws Exception {
        PolicyFixture f = triage();
        String internal = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL), Sensitivity.INTERNAL);
        String reviewId = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1).getSubject();
        deliver(f, answer("ans-1", reviewId, PORTAL, "business-rep",
                Json.obj().put("value", "adr").put("reviewer", "rev-1").put("noteRef", "s3://notes/why.md")));
        f.awaitCompleted(internal);
        String restricted = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL));   // no level: restricted
        String restrictedReview = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 2).getSubject();
        deliver(f, answer("ans-2", restrictedReview, PORTAL, "business-rep",
                Json.obj().put("value", "ata").put("reviewer", "rev-2")));
        f.awaitCompleted(restricted);

        List<Correction> corrections = f.corrections.of(TRIAGE).toCompletableFuture().get();
        StringWriter internalOnly = new StringWriter();
        StringWriter everything = new StringWriter();
        assertThat(CorrectionExport.write(corrections, Sensitivity.INTERNAL, internalOnly)).isEqualTo(1);
        assertThat(CorrectionExport.write(corrections, Sensitivity.RESTRICTED, everything)).isEqualTo(2);

        JsonNode line = Json.read(internalOnly.toString().strip().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        assertThat(line.path("decision").path("value").asText()).isEqualTo("ata");
        assertThat(line.path("decision").path("confidence").asDouble()).isEqualTo(0.41);
        assertThat(line.path("decision").path("provenance").path("adapter").asText()).isEqualTo("litjev");
        assertThat(line.path("human").path("value").asText()).isEqualTo("adr");
        assertThat(line.path("human").path("noteRef").asText()).isEqualTo("s3://notes/why.md");
        assertThat(line.path("agrees").asBoolean()).isFalse();
        assertThat(line.path("role").asText()).isEqualTo("business-rep");
        assertThat(line.path("materialRef").asText()).isEqualTo(MATERIAL);
        assertThat(line.path("requestedAt").asText()).isNotBlank();
        assertThat(everything.toString().lines()).hasSize(2).anyMatch(l -> l.contains("\"agrees\":true"));
        // the original decision record is still the adapter's
        assertThat(f.snapshot(internal).decisions().path("/do/0/meetingType").path("value").asText())
                .isEqualTo("ata");
    }

    @Test
    void a_restricted_instance_s_review_correlates_without_data_and_carries_no_content() throws Exception {
        PolicyFixture f = triage();
        String id = f.engine.start(TRIAGE, Json.obj().put("ref", MATERIAL).put("text", "SYNTH-PII-0501"),
                Sensitivity.RESTRICTED);
        CloudEvent request = awaitPublished(f, StintEvents.TYPE_REVIEW_REQUESTED, 1);

        assertThat(request.getData()).isNotNull();   // ids, names and pointers: inline at any level
        assertThat(request.getExtension(StintEvents.EXT_SENSITIVITY)).isEqualTo("restricted");
        assertThat(new String(CeWire.toJson(request), java.nio.charset.StandardCharsets.UTF_8))
                .doesNotContain("SYNTH-PII");
        deliver(f, answer("ans-1", request.getSubject(), PORTAL, "business-rep",
                Json.obj().put("value", "out").put("reviewer", "rev-1")));

        assertThat(f.awaitCompleted(id).path("type").asText()).isEqualTo("out");
        assertThat(f.snapshot(id).toString()).doesNotContain("SYNTH-PII");
        assertThat(f.audit.of(id).toCompletableFuture().get().toString()).doesNotContain("SYNTH-PII");
        assertThat(f.corrections.of(TRIAGE).toCompletableFuture().get().toString()).doesNotContain("SYNTH-PII");
    }

    @Test
    void strict_rejects_what_a_review_step_does_not_know() {
        String base = SensitivityPolicyTest.header("bad", POLICY);
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - ask:
                      call: review
                      with: { kind: approval, role: technical-rep, material: '${ .ref }', deadline: { hours: 1 },
                              secret: x }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("/do/0/ask/with/secret");
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - ask:
                      call: review
                      with: { kind: choice, role: tech, material: '${ .ref }', deadline: { hours: 1 },
                              options: [ { a: {} }, { b: {} } ] }
                      onReject: x
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("only for 'kind: approval'");
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - ask:
                      call: review
                      with: { kind: approval, role: tech, material: '${ .ref }', deadline: { hours: 1 },
                              remind: { hours: 2 } }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("before the 'deadline'");
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - fake:
                      emit:
                        event:
                          with: { source: https://x.example, type: io.stintflow.review.completed.v1 }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("reserved for review steps");
        assertThatThrownBy(() -> PolicyFixture.load(SensitivityPolicyTest.header("nosrc", """
                routes:
                  internal: ['*']""") + """
                do:
                  - ask:
                      call: review
                      with: { kind: approval, role: tech, material: '${ .ref }', deadline: { hours: 1 } }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("review/sources");
    }
}
