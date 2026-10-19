package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.KnowledgeDeclaration;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.core.expr.Expr;

/**
 * SDD 3b.2, CA5: {@code document.metadata.x-stint-knowledge} is checked for its form by the STRICT loader (the schema itself is
 * resolved by the worker), and {@code x-stint-policy.pseudonyms.stable: scope} makes a {@code call: redact} ask for pseudonyms
 * that are the same in every meeting of the base.
 */
class DslLoaderKnowledgeTest {

    private static WorkflowDefinition load(String metadata, String tasks) {
        String doc = """
                document:
                  dsl: '1.0.0'
                  namespace: meetings
                  name: notes
                  version: '2.0.0'
                  metadata:
                %s
                do:
                %s
                """.formatted(metadata.indent(4).stripTrailing(), tasks.indent(2).stripTrailing());
        return new DslLoader().load(new ByteArrayInputStream(doc.getBytes(StandardCharsets.UTF_8)), "notes.yaml",
                LoadOptions.STRICT);
    }

    private static final String SET = "- a: { set: '${ . }' }";
    private static final String POLICY_ROUTES = """
            x-stint-policy:
              routes:
                internal: ['*']
                confidential: ['*']
                restricted: ['*']""";

    @Test
    void the_declaration_is_read_and_the_version_of_the_definition_is_not_part_of_the_base() {
        WorkflowDefinition def = load("""
                x-stint-knowledge:
                  schema: meetings@1
                  glossary: product@1
                """, SET);

        assertThat(def.knowledge()).isEqualTo(new KnowledgeDeclaration(null, "meetings@1", "product@1"));
        assertThat(def.knowledge().baseOf(def.ref())).isEqualTo("notes");   // the name, never namespace:name:version
        assertThat(load("x-stint-knowledge: { base: product-meetings, schema: meetings@1 }", SET).knowledge()
                .baseOf(new WorkflowRef("meetings", "notes", "9.9.9"))).isEqualTo("product-meetings");
        assertThat(load("owner: me", SET).knowledge()).isNull();   // nothing declared: as before
    }

    @Test
    void an_unknown_key_or_a_malformed_value_fails_the_load_with_its_path() {
        assertThatThrownBy(() -> load("x-stint-knowledge: { schema: meetings@1, store: dynamo }", SET))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("/document/metadata/x-stint-knowledge/store");
        assertThatThrownBy(() -> load("x-stint-knowledge: { schema: meetings }", SET))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("x-stint-knowledge/schema")
                .hasMessageContaining("<id>@<major>");
        assertThatThrownBy(() -> load("x-stint-knowledge: { schema: meetings@1.0.0 }", SET))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("x-stint-knowledge/schema");
        assertThatThrownBy(() -> load("x-stint-knowledge: { base: 'a b' , schema: meetings@1 }", SET))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("x-stint-knowledge/base");
        assertThatThrownBy(() -> load("x-stint-knowledge: { base: notes }", SET))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("'schema' is required");
        assertThatThrownBy(() -> load("x-stint-knowledge: meetings@1", SET))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("is an object");
    }

    private static final String REDACT = """
            - clean:
                call: redact
                with:
                  task: redact-pii
                  input: ${ .transcript }
                  lowersTo: confidential
            """;

    private static String requestOf(WorkflowDefinition def) {
        CallRemoteNode call = (CallRemoteNode) def.root().tasks().get(0);
        return ((Expr.Jq) call.withInput()).source();
    }

    @Test
    void stable_pseudonyms_make_the_redaction_request_name_the_scope_of_the_base() {
        WorkflowDefinition plain = load(POLICY_ROUTES, REDACT);
        assertThat(requestOf(plain)).doesNotContain("pseudonyms");   // off by default

        WorkflowDefinition stable = load(POLICY_ROUTES + "\n  pseudonyms: { stable: scope }", REDACT);
        assertThat(requestOf(stable)).contains("\"pseudonyms\"").contains("\"mode\": \"scope\"")
                .contains("\"scope\": \"meetings/notes\"");

        WorkflowDefinition named = load(POLICY_ROUTES + "\n  pseudonyms: { stable: scope }\n"
                + "x-stint-knowledge: { base: product-meetings, schema: meetings@1 }", REDACT);
        assertThat(requestOf(named)).contains("\"scope\": \"meetings/product-meetings\"");
    }

    @Test
    void the_pseudonyms_option_only_knows_stable_scope() {
        assertThatThrownBy(() -> load(POLICY_ROUTES + "\n  pseudonyms: { stable: always }", REDACT))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("pseudonyms/stable");
        assertThatThrownBy(() -> load(POLICY_ROUTES + "\n  pseudonyms: { reversible: true }", REDACT))
                .isInstanceOf(DslValidationException.class).hasMessageContaining("pseudonyms/reversible");
    }
}
