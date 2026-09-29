package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.DoNode;
import io.stintflow.spi.WorkflowRef;

/** SDD 1.4: core {@link DslLoader} behaviour — call:remote compilation, CA4, sec. 8a/8b decisions. */
class DslLoaderTest {

    private static InputStream yaml(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void compiles_call_remote_with_with_input_reading_context_and_export_merging_output() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  namespace: test
                  name: sample
                  version: '1.0.0'
                do:
                  - extractData:
                      call: remote
                      with:
                        task: query-and-stage
                        input:
                          query: ${ $context.reportQuery }
                      export:
                        as: '${ $context + {pointer, rows} }'
                """;

        WorkflowDefinition def = new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.STRICT);

        assertThat(def.ref()).isEqualTo(new WorkflowRef("test", "sample", "1.0.0"));
        DoNode root = def.root();
        assertThat(root.tasks()).hasSize(1);
        CallRemoteNode node = (CallRemoteNode) root.tasks().get(0);
        assertThat(node.name()).isEqualTo("extractData");
        assertThat(node.pointer()).isEqualTo("/do/0/extractData");
        assertThat(node.routingKey()).isEqualTo("query-and-stage");
        assertThat(((Expr.Jq) node.dataFlow().inputFrom()).source()).contains("$context.reportQuery");
        assertThat(((Expr.Jq) node.dataFlow().exportAs()).source()).isEqualTo("$context + {pointer, rows}");
    }

    @Test
    void call_remote_with_a_timeout_after_is_compiled_into_the_node() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  name: sample
                  version: '1.0.0'
                do:
                  - slow:
                      call: remote
                      with:
                        task: slow-route
                      timeout:
                        after:
                          seconds: 30
                """;

        WorkflowDefinition def = new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.STRICT);
        CallRemoteNode node = (CallRemoteNode) def.root().tasks().get(0);
        assertThat(node.timeout()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void invalid_yaml_fails_with_file_and_pointer_info_ca4() {
        String broken = "document: [unterminated";

        assertThatThrownBy(() -> new DslLoader().load(yaml(broken), "broken.yaml", LoadOptions.STRICT))
                .isInstanceOf(DslValidationException.class)
                .satisfies(e -> {
                    DslValidationException ex = (DslValidationException) e;
                    assertThat(ex.file()).isEqualTo("broken.yaml");
                    assertThat(ex.violations()).isNotEmpty();
                });
    }

    @Test
    void an_invalid_jq_expression_fails_at_load_time_citing_the_pointer_sec_8b() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  name: sample
                  version: '1.0.0'
                do:
                  - broken:
                      call: remote
                      with:
                        task: some-route
                        input:
                          query: '${ .this is not ) valid ( jq }'
                """;

        assertThatThrownBy(() -> new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.STRICT))
                .isInstanceOf(DslValidationException.class)
                .satisfies(e -> {
                    DslValidationException ex = (DslValidationException) e;
                    assertThat(ex.violations()).anySatisfy(v -> assertThat(v.pointer()).contains("/with/input"));
                });
    }

    @Test
    void strict_mode_rejects_an_unsupported_task_construct_naming_it_and_its_pointer_rf3() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  name: sample
                  version: '1.0.0'
                do:
                  - notify:
                      listen:
                        to:
                          one:
                            with:
                              type: something.v1
                """;

        assertThatThrownBy(() -> new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.STRICT))
                .isInstanceOf(DslValidationException.class)
                .satisfies(e -> {
                    DslValidationException ex = (DslValidationException) e;
                    assertThat(ex.violations()).anySatisfy(v -> {
                        assertThat(v.pointer()).contains("/do/0/notify/listen");
                        assertThat(v.message()).contains("listen");
                    });
                });
    }

    @Test
    void permissive_mode_drops_an_unsupported_task_construct_and_still_compiles_the_rest_rf3() {
        String doc = """
                document:
                  dsl: '1.0.0'
                  name: sample
                  version: '1.0.0'
                do:
                  - callIt:
                      call: remote
                      with:
                        task: some-route
                  - notify:
                      listen:
                        to:
                          one:
                            with:
                              type: something.v1
                """;

        WorkflowDefinition def = new DslLoader().load(yaml(doc), "sample.yaml", LoadOptions.PERMISSIVE);
        assertThat(def.root().tasks()).hasSize(1);
        assertThat(def.root().tasks().get(0).name()).isEqualTo("callIt");
    }

    @Test
    void loadClasspath_with_explicit_resources_bypasses_the_generated_manifest_sec_8a_escape_hatch() {
        List<WorkflowDefinition> defs = new DslLoader()
                .loadClasspath("fixtures/*.yaml", LoadOptions.withExplicitResources(List.of("fixtures/a.yaml", "fixtures/b.yaml")));

        assertThat(defs).hasSize(2);
        assertThat(defs.get(0).ref().name()).isEqualTo("fixture-a");
        assertThat(defs.get(1).ref().name()).isEqualTo("fixture-b");
    }
}
