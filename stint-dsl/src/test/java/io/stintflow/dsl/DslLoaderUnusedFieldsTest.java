package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.dsl.DslValidationException.Violation;

/**
 * SDD 2.9, RF4/CA5: nothing declared in a definition is ignored in silence. In STRICT a key the compiler doesn't use
 * fails the load, naming the key and its path; PERMISSIVE loads it with a warning.
 */
class DslLoaderUnusedFieldsTest {

    private static final String HEADER = """
            document:
              dsl: '1.0.0'
              namespace: test
              name: unused
              version: '1.0.0'
            """;

    @Test
    void an_unknown_key_of_a_remote_call_fails_strict_naming_the_key_and_its_path() {
        assertViolation("""
                do:
                  - work:
                      call: remote
                      with:
                        task: route
                        headers: { x: 1 }
                """, "/do/0/work/with/headers", "'headers' is declared but not supported");
    }

    @Test
    void the_then_of_a_try_body_fails_strict_because_only_the_try_then_applies() {
        assertViolation("""
                do:
                  - guarded:
                      try:
                        - work:
                            call: remote
                            with:
                              task: route
                            then: end
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                """, "/then", "'then' of a try's body is not used");
    }

    @Test
    void dsl_properties_this_version_does_not_implement_fail_strict() {
        assertViolation("""
                do:
                  - work:
                      if: '${ .go }'
                      set: '${ . }'
                """, "/do/0/work/if", "'if' is declared");
        assertViolation("""
                do:
                  - work:
                      set: '${ . }'
                      timeout:
                        after: { seconds: 5 }
                """, "/do/0/work/timeout", "'timeout' is declared");
        assertViolation("""
                do:
                  - work:
                      input:
                        schema: { format: json }
                      set: '${ . }'
                """, "/do/0/work/input/schema", "'schema' is declared");
        assertViolation("""
                use:
                  functions: {}
                do:
                  - work:
                      set: '${ . }'
                """, "/use", "'use' is declared");
    }

    @Test
    void two_constructs_in_one_task_fail_strict() {
        assertViolation("""
                do:
                  - work:
                      set: '${ . }'
                      emit:
                        event:
                          with: { source: https://x.example, type: io.acme.a.v1 }
                """, "/do/0/work", "more than one construct");
    }

    @Test
    void a_misspelled_duration_unit_fails_strict_instead_of_becoming_zero() {
        assertViolation("""
                do:
                  - work:
                      call: remote
                      with:
                        task: route
                      timeout:
                        after: { minute: 5 }
                """, "/do/0/work/timeout/after/minute", "'minute' is declared");
    }

    @Test
    void retry_jitter_and_a_custom_error_name_fail_strict_instead_of_a_warning() {
        String yaml = """
                do:
                  - guarded:
                      try:
                        - work:
                            call: remote
                            with:
                              task: route
                      catch:
                        as: failure
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                        retry:
                          delay: { seconds: 2 }
                          jitter: { from: { seconds: 1 }, to: { seconds: 2 } }
                """;
        assertViolation(yaml, "/catch/retry/jitter", "jitter");
        assertViolation(yaml, "/catch/as", "custom error variable names");
    }

    @Test
    void permissive_loads_the_same_definition_with_a_warning() {
        WorkflowDefinition def = new DslLoader().load(yaml(HEADER + """
                do:
                  - work:
                      call: remote
                      with:
                        task: route
                        headers: { x: 1 }
                """), "unused.yaml", LoadOptions.PERMISSIVE);

        assertThat(def.root().tasks()).hasSize(1);
    }

    @Test
    void the_informative_dsl_keys_are_accepted() {
        WorkflowDefinition def = new DslLoader().load(yaml("""
                document:
                  dsl: '1.0.0'
                  namespace: test
                  name: informative
                  version: '1.0.0'
                  title: A title
                  summary: What it does
                  tags: { team: core }
                  metadata: { owner: someone }
                do:
                  - work:
                      metadata: { note: kept }
                      call: remote
                      with:
                        task: route
                        input: '${ . }'
                      input:
                        from: '${ . }'
                      output:
                        as: '${ . }'
                      export:
                        as: '${ . }'
                      timeout:
                        after: { minutes: 1 }
                """), "informative.yaml", LoadOptions.STRICT);

        assertThat(def.root().tasks()).hasSize(1);
    }

    private static void assertViolation(String tasks, String pointerSuffix, String message) {
        assertThatThrownBy(() -> new DslLoader().load(yaml(HEADER + tasks), "unused.yaml", LoadOptions.STRICT))
                .isInstanceOfSatisfying(DslValidationException.class, e -> assertThat(e.violations())
                        .anySatisfy((Violation v) -> {
                            assertThat(v.pointer()).endsWith(pointerSuffix);
                            assertThat(v.message()).contains(message);
                        }));
    }

    private static InputStream yaml(String text) {
        return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    }
}
