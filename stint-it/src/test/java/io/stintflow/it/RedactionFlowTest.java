package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.dsl.DslValidationException;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.redaction.LayeredRedactor;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionHandler;
import io.stintflow.worker.redaction.RedactionLayer;

/**
 * SDD 3a.5, RF5/RF6 (engine side of the redaction): {@code call: redact} sends the data to the redactor's route, and
 * only a valid result without doubt lowers the instance — to {@code lowersTo}, never above where it was. The remote
 * model then sees pseudonyms, never the markers; a doubtful result goes to {@code onDoubt} at the same level; a
 * failing or out-of-contract redactor fails closed. The audit trail records counts and levels, never a value.
 * Synthetic data ({@code SYNTH-PII-*} markers) only.
 */
class RedactionFlowTest {

    static final WorkflowRef REF = new WorkflowRef("meetings", "summarize", "1.0.0");
    static final Pseudonyms PSEUDONYMS = new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8));

    static final String POLICY = """
            routes:
              restricted: [redactor, summarize-local]
              confidential: [redactor, summarize-local, summarize-remote]
              internal: ['*']""";

    static final String TRANSCRIPT = "Speaker A: write to SYNTH-PII-0001@example.com or call +55 11 98765-4321.";

    static String flow(String redactExtra) {
        return SensitivityPolicyTest.header("summarize", POLICY) + """
                do:
                  - redact:
                      call: redact
                      with:
                        task: redactor
                        input: ${ .transcript }
                        lowersTo: confidential
                %s
                  - remote:
                      call: remote
                      with:
                        task: summarize-remote
                        input: ${ .output }
                      export: { as: '${ $context + . }' }
                      then: end
                  - local:
                      call: remote
                      with:
                        task: summarize-local
                        input: ${ .output }
                      export: { as: '${ $context + . }' }
                """.formatted(redactExtra.indent(6).stripTrailing());
    }

    static final String WITH_ON_DOUBT = flow("onDoubt: local");

    private final List<JsonNode> remoteSaw = new CopyOnWriteArrayList<>();

    private PolicyFixture fixture(String yaml, RedactionHandler redactor) throws Exception {
        return new PolicyFixture(yaml).handler("redactor", redactor)
                .worker("summarize-remote", in -> {
                    remoteSaw.add(in);
                    return Json.obj().put("summary", "remote");
                })
                .worker("summarize-local", in -> Json.obj().put("summary", "local"));
    }

    private static List<AuditRecord> redactions(PolicyFixture f, String id) throws Exception {
        return f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
                .filter(r -> AuditRecord.REDACTION.equals(r.kind())).toList();
    }

    private static List<String> routes(PolicyFixture f) {
        return f.dispatched.stream().map(TaskInvocation::routingKey).toList();
    }

    @Test
    void a_clean_redaction_lowers_the_level_and_the_remote_model_sees_only_pseudonyms() throws Exception {
        PolicyFixture f = fixture(WITH_ON_DOUBT, new RedactionHandler(LayeredRedactor.reference(), PSEUDONYMS));

        String id = f.engine.start(REF, Json.obj().put("transcript", TRANSCRIPT), Sensitivity.RESTRICTED);

        assertThat(f.awaitCompleted(id).path("summary").asText()).isEqualTo("remote");
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(routes(f)).containsExactly("redactor", "summarize-remote");
        assertThat(f.dispatched.get(0).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(f.dispatched.get(1).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(remoteSaw).hasSize(1);
        assertThat(remoteSaw.get(0).toString()).doesNotContain("SYNTH-PII", "98765").contains("EMAIL_", "PHONE_");

        List<AuditRecord> records = redactions(f, id);
        assertThat(records).hasSize(1);
        AuditRecord record = records.get(0);
        assertThat(record.outcome()).isEqualTo("lowered");
        assertThat(record.sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(record.route()).isEqualTo("redactor");
        assertThat(record.detail()).containsEntry("from", "restricted").containsEntry("counts.email", "1")
                .containsEntry("counts.phone", "1").containsEntry("doubt", "false")
                .containsEntry("provenance.redactor", "patterns");
        assertThat(record.detail().toString()).doesNotContain("SYNTH-PII", "EMAIL_", "Speaker");
    }

    @Test
    void a_doubtful_redaction_keeps_the_level_and_goes_to_on_doubt() throws Exception {
        PolicyFixture f = fixture(WITH_ON_DOUBT, new RedactionHandler(LayeredRedactor.reference(), PSEUDONYMS));

        String id = f.engine.start(REF, Json.obj().put("transcript", "Account 1234-5678-9012 of SYNTH-PII-0002"),
                Sensitivity.RESTRICTED);

        assertThat(f.awaitCompleted(id).path("summary").asText()).isEqualTo("local");
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(routes(f)).containsExactly("redactor", "summarize-local");
        assertThat(remoteSaw).isEmpty();
        assertThat(redactions(f, id)).singleElement().satisfies(r -> {
            assertThat(r.outcome()).isEqualTo("kept");
            assertThat(r.detail()).containsEntry("doubt", "true").doesNotContainKey("from");
        });
    }

    @Test
    void without_on_doubt_a_doubtful_result_leaves_the_instance_restricted_and_the_remote_route_refused()
            throws Exception {
        PolicyFixture f = fixture(flow(""), new RedactionHandler(LayeredRedactor.reference(), PSEUDONYMS));

        String id = f.engine.start(REF, Json.obj().put("transcript", "Account 1234-5678-9012"), Sensitivity.RESTRICTED);

        assertThatThrownBy(() -> f.awaitCompleted(id)).isInstanceOf(Exception.class);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(routes(f)).containsExactly("redactor");
        assertThat(remoteSaw).isEmpty();
    }

    @Test
    void a_failing_redactor_fails_closed() throws Exception {
        RedactionLayer broken = new RedactionLayer() {
            @Override
            public String name() {
                return "broken";
            }

            @Override
            public Result apply(String text, Scope scope) {
                throw new IllegalStateException("synthetic failure");
            }
        };
        PolicyFixture f = fixture(WITH_ON_DOUBT,
                new RedactionHandler(new LayeredRedactor("broken", "1", List.of(broken)), PSEUDONYMS));

        String id = f.engine.start(REF, Json.obj().put("transcript", TRANSCRIPT), Sensitivity.RESTRICTED);

        assertThatThrownBy(() -> f.awaitCompleted(id)).isInstanceOf(Exception.class);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(remoteSaw).isEmpty();
        assertThat(redactions(f, id)).isEmpty();
    }

    @Test
    void an_out_of_contract_answer_never_lowers_the_level() throws Exception {
        PolicyFixture f = new PolicyFixture(WITH_ON_DOUBT)
                .worker("redactor", in -> Json.obj().set("output", in.path("input")))   // no counts, doubt, provenance
                .worker("summarize-remote", in -> {
                    remoteSaw.add(in);
                    return Json.obj();
                });

        String id = f.engine.start(REF, Json.obj().put("transcript", TRANSCRIPT), Sensitivity.RESTRICTED);

        assertThatThrownBy(() -> f.awaitCompleted(id)).isInstanceOf(Exception.class);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(remoteSaw).isEmpty();
    }

    @Test
    void a_redaction_never_raises_the_level() throws Exception {
        PolicyFixture f = fixture(WITH_ON_DOUBT, new RedactionHandler(LayeredRedactor.reference(), PSEUDONYMS));

        String id = f.engine.start(REF, Json.obj().put("transcript", "nothing personal here"), Sensitivity.INTERNAL);

        assertThat(f.awaitCompleted(id).path("summary").asText()).isEqualTo("remote");
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.INTERNAL);
        assertThat(redactions(f, id)).singleElement().satisfies(r -> assertThat(r.outcome()).isEqualTo("kept"));
    }

    @Test
    void strict_loading_rejects_a_malformed_redact() {
        String base = SensitivityPolicyTest.header("summarize", POLICY);
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - redact:
                      call: redact
                      with:
                        task: redactor
                        lowersTo: public
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("is not a level");
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - redact:
                      call: redact
                      with:
                        task: redactor
                        secret: x
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("secret");
        assertThatThrownBy(() -> PolicyFixture.load(base + """
                do:
                  - redact:
                      call: redact
                      with:
                        input: ${ . }
                """)).isInstanceOf(DslValidationException.class).hasMessageContaining("with.task");
        assertThat(AuditLog.AuditRecord.REDACTION).isEqualTo("redaction");
    }
}
