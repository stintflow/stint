package io.stintflow.worker.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskReport;

/**
 * SDD 3a.5, 4.7 (RF9): restricted — and no level — only to a local provider; confidential to a local one or an approved
 * remote one; internal to any. A refusal is {@code authorization} 403 without calling the adapter, and reports no
 * provider; an allowed call reports its provider for the audit trail.
 */
class SensitivityPolicyTest {

    static final SensitivityPolicy POLICY = SensitivityPolicy.fromProperties(Map.of(
            "stint.policy.confidential.providers", "litjev, approved-cloud",
            "stint.decision.routes.x.adapter", "ignored: not a policy key"));

    static final ModelProvider LOCAL = new ModelProvider("litjev", null, false);
    static final ModelProvider APPROVED = new ModelProvider("approved-cloud", "m-1", true);
    static final ModelProvider OTHER = new ModelProvider("other-cloud", "m-2", true);

    @Test
    void the_matrix_of_levels_and_providers() {
        for (Sensitivity level : new Sensitivity[] {null, Sensitivity.RESTRICTED, Sensitivity.CONFIDENTIAL,
                Sensitivity.INTERNAL}) {
            assertThat(POLICY.refusal(level, LOCAL)).as("local at %s", level).isEmpty();
        }
        assertThat(POLICY.refusal(null, APPROVED)).hasValueSatisfying(r -> assertThat(r).contains("restricted"));
        assertThat(POLICY.refusal(Sensitivity.RESTRICTED, APPROVED)).isPresent();
        assertThat(POLICY.refusal(Sensitivity.CONFIDENTIAL, APPROVED)).isEmpty();
        assertThat(POLICY.refusal(Sensitivity.CONFIDENTIAL, OTHER))
                .hasValue("the remote provider 'other-cloud' is not approved for confidential data");
        assertThat(POLICY.refusal(Sensitivity.INTERNAL, OTHER)).isEmpty();
    }

    @Test
    void only_the_confidential_list_is_configurable() {
        assertThatThrownBy(() -> SensitivityPolicy.fromProperties(Map.of("stint.policy.restricted.providers", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("stint.policy.restricted.providers: unknown policy key");
        assertThat(SensitivityPolicy.fromProperties(Map.of()).refusal(Sensitivity.CONFIDENTIAL, APPROVED)).isPresent();
    }

    /** An adapter that counts its calls, with any descriptor. */
    static final class CountingAdapter implements DecisionAdapter {
        final AtomicInteger calls = new AtomicInteger();
        final AdapterDescriptor descriptor;

        CountingAdapter(String provider, boolean remote) {
            descriptor = new AdapterDescriptor("counting", provider, "m-1", "1", remote);
        }

        @Override
        public AdapterDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task) {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture(Decision.of(JsonNodeFactory.instance.booleanNode(true), 1.0));
        }
    }

    private static TaskHandler handler(DecisionAdapter adapter) {
        return DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.urgent.adapter", adapter.descriptor().id())))
                .adapter(adapter, ConfidenceCalibration.IDENTITY).policy(POLICY).build().handlers().get("urgent");
    }

    private static TaskContext task(Sensitivity level, TaskReport report) {
        return new TaskContext("/do/0/urgent", "i-1", "c-1", 1, DecisionHandlerTest.URGENT.toJson(),
                new WorkflowRef("test", "decide", "1.0.0"), "chn-1", Lineage.NONE, null, level, report);
    }

    @Test
    void the_handler_refuses_a_remote_adapter_for_restricted_data_without_calling_it() {
        CountingAdapter remote = new CountingAdapter("other-cloud", true);
        TaskReport report = new TaskReport();

        assertThatThrownBy(() -> handler(remote).execute(task(Sensitivity.RESTRICTED, report))
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> {
                    ErrorInfo error = ((TaskFailure) e.getCause()).toErrorInfo();
                    assertThat(error.type()).isEqualTo(ErrorInfo.StandardType.AUTHORIZATION.uri());
                    assertThat(error.status()).isEqualTo(403);
                    assertThat(error.detail()).contains("may not see restricted data");
                });
        assertThat(remote.calls).hasValue(0);
        assertThat(report.provider()).isNull();
    }

    @Test
    void an_allowed_call_reports_its_provider() {
        CountingAdapter local = new CountingAdapter("litjev", false);
        TaskReport report = new TaskReport();

        JsonNode result = handler(local).execute(task(null, report)).toCompletableFuture().join();

        assertThat(result.path("value").asBoolean()).isTrue();
        assertThat(local.calls).hasValue(1);
        assertThat(report.provider()).isEqualTo(new ModelProvider("litjev", "m-1", false));
    }
}
