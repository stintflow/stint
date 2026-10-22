package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;

import io.stintflow.knowledge.schema.KnowledgeSchema;

/**
 * SDD 3c.5, CA5, CA6 (the document and its hash): the contract of exposure loads whole or not at all, and says in the error where.
 */
class BridgeContractTest {

    static final String VALID = """
            {"bridge": 1, "base": "examples/product-meetings", "version": "1.0.0",
             "consumers": [{"definition": "examples:architecture-adr", "types": ["Decision"], "view": "adr", "ceiling": "confidential"},
                           {"definition": "examples:architecture-resolve", "types": ["Decision"], "view": "plain", "ceiling": "internal"}],
             "views": {"plain": {"types": {"Decision": {"label": true, "properties": ["status"]}}},
                       "adr": {"allowSensitive": true, "types": {"Decision": {"label": true, "properties": ["status", "summary"]}}}}}
            """;

    private static BridgeContract parse(String json) {
        return BridgeContract.parse(json.getBytes(StandardCharsets.UTF_8), KnowledgeSchema.meetings());
    }

    @Test
    void a_valid_contract_loads_with_its_version_its_base_and_a_hash() {
        BridgeContract contract = parse(VALID);

        assertThat(contract.version()).isEqualTo("1.0.0");
        assertThat(contract.base().key()).isEqualTo("examples/product-meetings");
        assertThat(contract.contentHash()).hasSize(64);
        assertThat(contract.policy()).isNotNull();
    }

    @Test
    void the_hash_does_not_depend_on_the_order_of_the_keys_and_changes_with_the_content() {
        String reordered = """
                {"version": "1.0.0", "bridge": 1, "base": "examples/product-meetings",
                 "views": {"adr": {"types": {"Decision": {"properties": ["status", "summary"], "label": true}}, "allowSensitive": true},
                           "plain": {"types": {"Decision": {"properties": ["status"], "label": true}}}},
                 "consumers": [{"ceiling": "confidential", "view": "adr", "types": ["Decision"], "definition": "examples:architecture-adr"},
                               {"ceiling": "internal", "view": "plain", "types": ["Decision"], "definition": "examples:architecture-resolve"}]}
                """;

        assertThat(parse(reordered).contentHash()).isEqualTo(parse(VALID).contentHash());
        assertThat(parse(VALID.replace("\"internal\"", "\"confidential\"")).contentHash()).isNotEqualTo(parse(VALID).contentHash());
    }

    @Test
    void ca5_a_contract_that_breaks_a_rule_does_not_load_and_says_where() {
        assertRefused(VALID.replace("\"ceiling\": \"internal\"", "\"ceiling\": \"restricted\""), "$.consumers[1].ceiling");
        assertRefused(VALID.replace("\"allowSensitive\": true, ", ""), "$.views.adr");   // summary is sensitive
        assertRefused(VALID.replace("\"view\": \"plain\"", "\"view\": \"missing\""), "$.consumers[1].view");
        assertRefused(VALID.replace("\"types\": [\"Decision\"], \"view\": \"plain\"", "\"types\": []"), "$.consumers[1].types");
        assertRefused(VALID.replace("\"Decision\": {\"label\": true, \"properties\": [\"status\"]}",
                "\"Spaceship\": {\"label\": true}"), "$.views.plain");   // a type the schema does not know
        assertRefused(VALID.replace("[\"status\"]", "[\"colour\"]"), "$.views.plain");   // a property it does not know
        assertRefused(VALID.replace("examples:architecture-resolve", "examples:architecture-adr"), "$.consumers[1].definition");
        assertRefused(VALID.replace("\"types\": [\"Decision\"], \"view\": \"plain\"", "\"types\": [\"Requirement\"], \"view\": \"plain\""),
                "$.consumers[1].types");   // the view does not publish it
        assertRefused(VALID.replace("\"version\": \"1.0.0\"", "\"version\": \"one\""), "$.version");
        assertRefused("not json", "$");
    }

    private static void assertRefused(String json, String path) {
        assertThatThrownBy(() -> parse(json)).isInstanceOf(BridgeContract.Invalid.class).hasMessageContaining(path);
    }
}
