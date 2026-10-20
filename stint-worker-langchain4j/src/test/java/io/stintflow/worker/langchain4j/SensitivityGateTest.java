package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.failure;
import static io.stintflow.worker.langchain4j.Fakes.run;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskReport;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.Logs;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;

/**
 * SDD 3a.5, 4.7 (RF9, CA1 worker side): before every execution, the LLM worker checks the level the invoke carries
 * against its declared provider — undeclared counts as remote. A refusal is {@code authorization} 403 and the model is
 * never called; an execution reports its provider for the audit trail. Fake model, synthetic data.
 */
class SensitivityGateTest {

    static final SensitivityPolicy POLICY = SensitivityPolicy.fromProperties(
            Map.of("stint.policy.confidential.providers", "approved-cloud"));

    private static TaskContext task(Sensitivity level, TaskReport report) {
        return new TaskContext("/do/0/ask", "i-A", "c-1", 1, Json.obj().put("text", "SYNTH-PII-0001"),
                new WorkflowRef("test", "ai", "1.0.0"), "chn-i-A", Lineage.NONE, null, level, report);
    }

    private static TaskHandler step(LangChain4jWorker worker) {
        return worker.handler(Chat.class, (ai, t) -> ai.reply(t.input().path("text").asText()));
    }

    @Test
    void an_undeclared_model_counts_as_remote_and_restricted_data_never_reaches_it() {
        ScriptedModel model = ScriptedModel.replying("never");
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).policy(POLICY).build();
        TaskReport report = new TaskReport();

        ErrorInfo error;
        try (Logs logs = new Logs()) {
            error = failure(step(worker), task(Sensitivity.RESTRICTED, report)).toErrorInfo();
            assertThat(logs.joined()).contains("the sensitivity policy refused the model").doesNotContain("SYNTH-PII");
        }

        assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.AUTHORIZATION.uri());
        assertThat(error.status()).isEqualTo(403);
        assertThat(error.detail()).contains("may not see restricted data").doesNotContain("SYNTH-PII");
        assertThat(model.requests).isEmpty();
        assertThat(report.provider()).isNull();
    }

    @Test
    void no_level_is_restricted_and_a_local_provider_may_see_it() {
        ScriptedModel remote = ScriptedModel.replying("never");
        LangChain4jWorker remoteWorker = LangChain4jWorker.builder().chatModel(remote)
                .provider("approved-cloud", "m-1", true).policy(POLICY).build();
        assertThat(failure(step(remoteWorker), task(null, new TaskReport())).toErrorInfo().status()).isEqualTo(403);
        assertThat(remote.requests).isEmpty();

        ScriptedModel local = ScriptedModel.replying("local answer");
        LangChain4jWorker localWorker = LangChain4jWorker.builder().chatModel(local).provider("litjev", false)
                .policy(POLICY).build();
        TaskReport report = new TaskReport();
        assertThat(run(step(localWorker), task(null, report)).asText()).isEqualTo("local answer");
        assertThat(report.provider()).isEqualTo(new ModelProvider("litjev", null, false));
    }

    @Test
    void confidential_data_goes_to_an_approved_remote_provider_only() {
        ScriptedModel approved = ScriptedModel.replying("approved");
        TaskReport report = new TaskReport();
        assertThat(run(step(LangChain4jWorker.builder().chatModel(approved).provider("approved-cloud", "m-1", true)
                .policy(POLICY).build()), task(Sensitivity.CONFIDENTIAL, report)).asText()).isEqualTo("approved");
        assertThat(report.provider()).isEqualTo(new ModelProvider("approved-cloud", "m-1", true));

        ScriptedModel other = ScriptedModel.replying("never");
        assertThat(failure(step(LangChain4jWorker.builder().chatModel(other).provider("other-cloud", true)
                .policy(POLICY).build()), task(Sensitivity.CONFIDENTIAL, new TaskReport())).toErrorInfo().detail())
                .contains("not approved for confidential data");
        assertThat(other.requests).isEmpty();
    }

    @Test
    void without_a_policy_nothing_is_refused_and_the_provider_is_still_reported() {
        ScriptedModel model = ScriptedModel.replying("ok");
        TaskReport report = new TaskReport();

        assertThat(run(step(LangChain4jWorker.builder().chatModel(model).build()),
                task(Sensitivity.RESTRICTED, report)).asText()).isEqualTo("ok");
        assertThat(report.provider().remote()).isTrue();
    }
}
