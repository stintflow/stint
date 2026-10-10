package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.TryNode;

/** SDD 2.3, RF1: {@code listen} compiles from DSL 1.0 YAML; what this phase leaves out is reported (sec. 8f). */
class DslLoaderListenTest {

    private static final String HEADER = """
            document:
              dsl: '1.0.0'
              name: sample
              version: '1.0.0'
            do:
            """;

    private static InputStream yaml(String text) {
        return new ByteArrayInputStream((HEADER + text).getBytes(StandardCharsets.UTF_8));
    }

    private static WorkflowDefinition load(String doBlock) {
        return new DslLoader().load(yaml(doBlock), "sample.yaml", LoadOptions.STRICT);
    }

    @Test
    void compiles_the_sdd_example_all_with_correlation_inside_a_try_with_a_timeout() {
        WorkflowDefinition def = load("""
                  - awaitReview:
                      try:
                        - wait:
                            listen:
                              to:
                                all:
                                  - with: { type: io.stintflow.review.completed.v1 }
                                    correlate:
                                      instance: { from: '${ .data.instanceId }', expect: '${ $workflow.id }' }
                                  - with: { type: io.stintflow.billing.approved.v1, source: https://acme.example/billing }
                                    correlate:
                                      instance: { from: '${ .data.instanceId }', expect: '${ $workflow.id }' }
                              read: envelope
                            timeout: { after: { minutes: 10 } }
                      catch:
                        errors: { with: { type: 'stint://errors/timeout' } }
                """);

        TryNode tryNode = (TryNode) def.root().tasks().get(0);
        ListenNode listen = tryNode.listen();
        assertThat(listen.strategy()).isEqualTo(ListenNode.Strategy.ALL);
        assertThat(listen.read()).isEqualTo(ListenNode.Read.ENVELOPE);
        assertThat(listen.timeout()).isEqualTo(Duration.ofMinutes(10));
        assertThat(listen.filters()).extracting(ListenNode.Filter::type)
                .containsExactly("io.stintflow.review.completed.v1", "io.stintflow.billing.approved.v1");
        assertThat(listen.filters().get(1).with()).containsEntry("source", "https://acme.example/billing");
        ListenNode.Correlation instance = listen.filters().get(0).correlate().get("instance");
        assertThat(((Expr.Jq) instance.from()).source()).isEqualTo(".data.instanceId");
        assertThat(((Expr.Jq) instance.expect()).source()).isEqualTo("($workflow.id)");

        new WorkflowRegistry(Duration.ofMinutes(15)).register(def); // and the registry accepts it
    }

    @Test
    void one_defaults_to_read_data_and_accepts_a_constant_expect() {
        WorkflowDefinition def = load("""
                  - awaitReview:
                      listen:
                        to:
                          one:
                            with: { type: io.acme.done.v1 }
                            correlate:
                              order: { from: .data.orderId, expect: A-1 }
                """);

        ListenNode listen = (ListenNode) def.root().tasks().get(0);
        assertThat(listen.strategy()).isEqualTo(ListenNode.Strategy.ONE);
        assertThat(listen.read()).isEqualTo(ListenNode.Read.DATA);
        assertThat(listen.timeout()).isNull();
        assertThat(((Expr.Jq) listen.filters().get(0).correlate().get("order").expect()).source()).isEqualTo("\"A-1\"");
    }

    @Test
    void what_this_phase_leaves_out_is_reported_with_its_pointer() {
        assertUnsupported("""
                  - l:
                      listen:
                        to:
                          any: []
                """, "/do/0/l/listen/to/any");
        assertUnsupported("""
                  - l:
                      listen:
                        to:
                          any:
                            - with: { type: a.v1 }
                              correlate: { k: { from: .data.k, expect: x } }
                          until: '${ false }'
                """, "/do/0/l/listen/to/until");
        assertUnsupported("""
                  - l:
                      listen:
                        to:
                          one:
                            with: { type: a.v1 }
                            correlate: { k: { from: .data.k, expect: x } }
                        foreach:
                          item: event
                """, "/do/0/l/listen/foreach");
        assertUnsupported("""
                  - l:
                      listen:
                        to:
                          one:
                            with: { type: a.v1, subject: '${ .x }' }
                            correlate: { k: { from: .data.k, expect: x } }
                """, "/do/0/l/listen/to/one/with/subject");
        assertUnsupported("""
                  - l:
                      listen:
                        to:
                          one:
                            with: { type: a.v1 }
                            correlate: { k: { from: .data.k } }
                """, "/do/0/l/listen/to/one/correlate/k/expect");
    }

    private static void assertUnsupported(String doBlock, String pointer) {
        assertThatThrownBy(() -> load(doBlock))
                .isInstanceOf(DslValidationException.class)
                .satisfies(e -> assertThat(((DslValidationException) e).violations())
                        .anySatisfy(v -> assertThat(v.pointer()).isEqualTo(pointer)));
    }
}
