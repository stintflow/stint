package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.FlowDirective;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.SwitchNode;
import io.stintflow.core.model.TryNode;

/** SDD 1.4, RF2: {@code switch} and {@code try}/{@code catch}/{@code retry} compilation. */
class DslLoaderControlFlowTest {

    private static InputStream yaml(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void switch_compiles_cases_in_order_with_a_default_last() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  name: sample
                  version: '1.0.0'
                do:
                  - route:
                      switch:
                        - electronic:
                            when: .orderType == "electronic"
                            then: processElectronic
                        - default:
                            then: processOther
                """;

        WorkflowDefinition def = new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.STRICT);
        SwitchNode node = (SwitchNode) def.root().tasks().get(0);

        assertThat(node.cases()).hasSize(2);
        SwitchNode.Case first = node.cases().get(0);
        assertThat(first.name()).isEqualTo("electronic");
        assertThat(((Expr.Jq) first.when()).source()).isEqualTo(".orderType == \"electronic\"");
        assertThat(first.then()).isEqualTo(FlowDirective.goTo("processElectronic"));

        SwitchNode.Case second = node.cases().get(1);
        assertThat(second.when()).isNull();
        assertThat(second.then()).isEqualTo(FlowDirective.goTo("processOther"));
    }

    @Test
    void try_catch_retry_compiles_into_a_tryNode_matching_the_sdd_1_3_shape() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  name: sample
                  version: '1.0.0'
                do:
                  - trySomething:
                      try:
                        - invalidCall:
                            call: remote
                            with:
                              task: flaky-route
                      catch:
                        errors:
                          with:
                            status: 503
                        when: .status == 503
                        retry:
                          delay:
                            seconds: 2
                          backoff:
                            exponential: {}
                          limit:
                            attempt:
                              count: 5
                            duration:
                              seconds: 60
                        then: end
                """;

        WorkflowDefinition def = new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.STRICT);
        TryNode node = (TryNode) def.root().tasks().get(0);

        assertThat(node.body()).isInstanceOf(CallRemoteNode.class);
        assertThat(node.body().routingKey()).isEqualTo("flaky-route");

        TryNode.Catch catchClause = node.catchClause();
        assertThat(((Expr.Jq) catchClause.errorFilter()).source()).isEqualTo("(.status == 503)");
        assertThat(((Expr.Jq) catchClause.when()).source()).isEqualTo(".status == 503");
        assertThat(catchClause.then()).isEqualTo(FlowDirective.END);

        RetryPolicy retry = catchClause.retry();
        assertThat(retry.delay()).isEqualTo(Duration.ofSeconds(2));
        assertThat(retry.backoff()).isEqualTo(RetryPolicy.Backoff.EXPONENTIAL);
        assertThat(retry.maxAttempts()).isEqualTo(5);
        assertThat(retry.maxDuration()).isEqualTo(Duration.ofSeconds(60));
        assertThat(retry.jitterRatio()).isEqualTo(0.0);
    }
}
