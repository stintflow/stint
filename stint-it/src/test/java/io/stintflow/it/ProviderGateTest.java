package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.stintflow.it.AiFixture.Chat;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.redaction.LayeredRedactor;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionHandler;

/**
 * SDD 3a.5, ADR 3a.5-A (C), over the real wire: the engine gates the route, the worker gates the provider. A route the
 * level allows but whose worker runs a remote model is refused by the worker — {@code authorization} 403, caught in a
 * real workflow — and the model never sees the data (CA1, worker side). Redacted first, the data reaches an approved
 * remote model with pseudonyms only (CA2). The audit trail records the refusals and each AI call's provider, never a
 * value. Fake models, synthetic data.
 */
class ProviderGateTest {

    static final WorkflowRef REF = new WorkflowRef("meetings", "summarize", "1.0.0");
    static final SensitivityPolicy WORKER_POLICY = SensitivityPolicy.fromProperties(
            Map.of("stint.policy.confidential.providers", "approved-cloud"));
    static final String TRANSCRIPT = "Speaker A: mail SYNTH-PII-0001@example.com, CPF 123.456.789-09.";

    static final String POLICY = """
            routes:
              restricted: [redactor, summarize-local]
              confidential: [redactor, summarize-local, summarize-remote]
              internal: ['*']""";

    static TaskHandler summarizer(LangChain4jWorker worker) {
        return worker.handler(Chat.class, (ai, task) -> Json.obj().put("summary", ai.reply(task.input().toString())));
    }

    private static List<AuditRecord> audit(PolicyFixture f, String id, String kind) throws Exception {
        return f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
                .filter(r -> kind.equals(r.kind())).toList();
    }

    @Test
    void a_local_route_backed_by_a_remote_model_is_refused_by_the_worker_and_caught() throws Exception {
        RuleModel model = new RuleModel(request -> "never");
        PolicyFixture f = new PolicyFixture(SensitivityPolicyTest.header("summarize", POLICY) + """
                do:
                  - guarded:
                      try:
                        - summarize:
                            call: remote
                            with:
                              task: summarize-local
                              input: ${ . }
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/authorization
                        do:
                          - note:
                              set: '${ {refused: .status} }'
                  - outcome:
                      set: '${ . }'
                      export:
                        as: '${ . }'
                """).handler("summarize-local", summarizer(LangChain4jWorker.builder().chatModel(model)
                .policy(WORKER_POLICY).clock(() -> PolicyFixture.T0).build()));   // misconfigured: undeclared, so remote

        String id = f.engine.start(REF, Json.obj().put("transcript", TRANSCRIPT));

        assertThat(f.awaitCompleted(id).path("refused").asInt()).isEqualTo(403);
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(model.requests).isEmpty();
        assertThat(audit(f, id, AuditRecord.POLICY)).extracting(AuditRecord::outcome)
                .containsExactly("allowed", "refused");
        assertThat(audit(f, id, AuditRecord.POLICY).get(1).detail()).containsEntry("by", "worker");
        assertThat(audit(f, id, AuditRecord.AI_CALL)).isEmpty();
    }

    @Test
    void redacted_data_reaches_an_approved_remote_model_with_pseudonyms_only() throws Exception {
        RuleModel model = new RuleModel(request -> "a synthetic summary");
        PolicyFixture f = new PolicyFixture(SensitivityPolicyTest.header("summarize", POLICY) + """
                do:
                  - redact:
                      call: redact
                      with:
                        task: redactor
                        input: ${ .transcript }
                        lowersTo: confidential
                  - summarize:
                      call: remote
                      with:
                        task: summarize-remote
                        input: ${ .output }
                      export: { as: '${ $context + . }' }
                """)
                .handler("redactor", new RedactionHandler(LayeredRedactor.reference(),
                        new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8))))
                .handler("summarize-remote", summarizer(LangChain4jWorker.builder().chatModel(model)
                        .provider("approved-cloud", "rule-model", true).policy(WORKER_POLICY).clock(() -> PolicyFixture.T0).build()));

        String id = f.engine.start(REF, Json.obj().put("transcript", TRANSCRIPT));

        assertThat(f.awaitCompleted(id).path("summary").asText()).isEqualTo("a synthetic summary");
        assertThat(model.requests).hasSize(1);
        assertThat(model.requests.get(0).messages().toString()).doesNotContain("SYNTH-PII", "123.456")
                .contains("EMAIL_", "DOCUMENT_");

        List<AuditRecord> calls = audit(f, id, AuditRecord.AI_CALL);
        assertThat(calls).singleElement().satisfies(call -> {
            assertThat(call.route()).isEqualTo("summarize-remote");
            assertThat(call.sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
            assertThat(call.outcome()).isEqualTo("answered");
            assertThat(call.detail()).containsEntry("provider", "approved-cloud")
                    .containsEntry("model", "rule-model").containsEntry("remote", "true");
        });
        assertThat(f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS).toString())
                .doesNotContain("SYNTH-PII", "EMAIL_", "synthetic summary");
    }
}
