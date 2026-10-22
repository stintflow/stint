package io.stintflow.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** SDD 3c.1, CA3: the form of a graph pointer, strict, with a code for the refusal and none of the text. */
class GraphPointerTest {

    static final String NODE = "e-0123456789abcdef0123456789abcdef";
    static final String GOOD = "stint-kg://examples/product-meetings/" + NODE + "?v=42";

    @Test
    void a_pointer_is_made_parsed_and_written_back_the_same() {
        GraphPointer pointer = GraphPointer.parse(GOOD);

        assertThat(pointer.namespace()).isEqualTo("examples");
        assertThat(pointer.base()).isEqualTo("product-meetings");
        assertThat(pointer.nodeId()).isEqualTo(NODE);
        assertThat(pointer.version()).isEqualTo(42);
        assertThat(pointer.format()).isEqualTo(GOOD).isEqualTo(pointer.toString());
        assertThat(pointer.scope()).isEqualTo(KnowledgeScope.named("examples", "product-meetings"));
        assertThat(GraphPointer.of(KnowledgeScope.named("examples", "product-meetings"), NODE, 42)).isEqualTo(pointer);
        assertThat(GraphPointer.parse(GOOD.replace("v=42", "v=9223372036854775807")).version()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void the_same_node_is_the_same_whatever_the_version() {
        GraphPointer a = GraphPointer.parse(GOOD);
        GraphPointer b = GraphPointer.parse(GOOD.replace("v=42", "v=43"));
        GraphPointer other = GraphPointer.parse(GOOD.replace("product-meetings", "architecture"));

        assertThat(a.sameNodeAs(b)).isTrue();
        assertThat(a.sameNodeAs(other)).isFalse();
        assertThat(a.nodeKey()).isEqualTo(b.nodeKey()).isEqualTo("stint-kg://examples/product-meetings/" + NODE);
    }

    @Test
    void ca3_every_refusal_has_its_code_and_none_echoes_the_text() {
        Map<String, String> refused = new java.util.LinkedHashMap<>();
        refused.put("", "length");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=" + "1".repeat(300), "length");
        refused.put("https://examples/product-meetings/" + NODE + "?v=1", "scheme");
        refused.put("STINT-KG://examples/product-meetings/" + NODE + "?v=1", "scheme");
        refused.put("stint-kg:/examples/product-meetings/" + NODE + "?v=1", "scheme");
        refused.put("stint-kg://Examples/product-meetings/" + NODE + "?v=1", "domain");
        refused.put("stint-kg://examples/Product/" + NODE + "?v=1", "domain");
        refused.put("stint-kg://user@examples/product-meetings/" + NODE + "?v=1", "domain");
        refused.put("stint-kg://examples:8080/product-meetings/" + NODE + "?v=1", "domain");
        refused.put("stint-kg://-examples/product-meetings/" + NODE + "?v=1", "domain");
        refused.put("stint-kg://examples/product-meetings/" + NODE.toUpperCase() + "?v=1", "node");
        refused.put("stint-kg://examples/product-meetings/-leading?v=1", "node");
        refused.put("stint-kg://examples/product-meetings/" + "a".repeat(129) + "?v=1", "node");
        refused.put("stint-kg://examples/product-meetings/..%2F" + NODE + "?v=1", "node");
        refused.put("stint-kg://examples/product-meetings/" + NODE, "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=0", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=-1", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=007", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=1.5", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=9223372036854775808", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?", "version");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=1&x=2", "extra");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?x=1", "extra");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "?v=1#frag", "extra");
        refused.put("stint-kg://examples/product-meetings/" + NODE + "/?v=1", "extra");
        refused.put("stint-kg://examples/product-meetings/extra/" + NODE + "?v=1", "extra");
        refused.put("stint-kg://examples/" + NODE + "?v=1", "extra");

        refused.forEach((text, code) -> assertThatThrownBy(() -> GraphPointer.parse(text)).as(text.length() > 80 ? "long" : text)
                .isInstanceOf(KnowledgeValidationException.class).hasMessage("Invalid graph pointer (" + code + ")"));
        assertThatThrownBy(() -> GraphPointer.parse(null)).hasMessage("Invalid graph pointer (length)");
    }

    @Test
    void a_glossary_may_name_its_nodes_and_the_pointer_takes_them() {
        GraphPointer pointer = GraphPointer.parse("stint-kg://examples/product-meetings/sys-checkout?v=3");

        assertThat(pointer.nodeId()).isEqualTo("sys-checkout");
        assertThat(pointer.format()).isEqualTo("stint-kg://examples/product-meetings/sys-checkout?v=3");
    }

    @Test
    void a_pointer_of_a_blob_is_not_a_pointer_of_a_graph() {
        assertThatThrownBy(() -> GraphPointer.parse("file:///tmp/blobs/minutes/m-001.md"))
                .hasMessage("Invalid graph pointer (scheme)");
        assertThatThrownBy(() -> GraphPointer.parse("{\"__stint_claim_check__\":\"file:///x\"}"))
                .hasMessage("Invalid graph pointer (scheme)");
        assertThatThrownBy(() -> new GraphPointer("examples", "b", NODE, 0)).isInstanceOf(KnowledgeValidationException.class);
    }

    @Test
    void the_version_is_the_last_entry_of_the_entity_and_gone_with_a_removal() {
        List<ChangeEntry> log = List.of(entry(1, ChangeEntry.Op.ADD, ChangeEntry.Kind.ENTITY, NODE),
                entry(2, ChangeEntry.Op.ADD, ChangeEntry.Kind.ENTITY, "e-other"),
                entry(3, ChangeEntry.Op.UPDATE, ChangeEntry.Kind.ENTITY, NODE),
                entry(4, ChangeEntry.Op.ADD, ChangeEntry.Kind.PASSAGE, NODE),
                entry(5, ChangeEntry.Op.REINDEX, ChangeEntry.Kind.PASSAGE, "base"));

        assertThat(GraphPointers.versionOf(log, NODE)).hasValue(3);
        assertThat(GraphPointers.versionOf(log, "e-other")).hasValue(2);
        assertThat(GraphPointers.versionOf(log, "e-never")).isEmpty();
        List<ChangeEntry> removed = new java.util.ArrayList<>(log);
        removed.add(entry(6, ChangeEntry.Op.REMOVE, ChangeEntry.Kind.ENTITY, NODE));
        assertThat(GraphPointers.versionOf(removed, NODE)).isEmpty();
        removed.add(entry(7, ChangeEntry.Op.ADD, ChangeEntry.Kind.ENTITY, NODE));
        assertThat(GraphPointers.versionOf(removed, NODE)).hasValue(7);
    }

    @Test
    void the_architecture_schema_is_a_reference_schema_with_the_domain_reference_and_the_relation_that_informs_it() {
        var registry = io.stintflow.knowledge.schema.SchemaRegistry.withReferenceSchemas();
        var schema = registry.find("architecture", "1.0.0").orElseThrow();

        assertThat(registry.resolve("architecture@1").orElseThrow().fullVersion()).isEqualTo("architecture@1.1.0");
        assertThat(schema.fullVersion()).isEqualTo("architecture@1.0.0");
        assertThat(schema.nodes()).containsOnlyKeys("DomainReference", "System");
        assertThat(schema.nodes().get("DomainReference").properties()).containsOnlyKeys("pointer", "remoteType", "domain",
                "observedVersion").allSatisfy((name, property) -> assertThat(property.required()).as(name).isTrue());
        assertThat(schema.relations().get("informs").from()).containsExactly("System");
        assertThat(schema.relations().get("informs").to()).containsExactly("DomainReference");
        assertThat(io.stintflow.knowledge.schema.SchemaRegistry.withReferenceSchemas().resolve("meetings@1")).isPresent();
    }

    private static ChangeEntry entry(long sequence, ChangeEntry.Op op, ChangeEntry.Kind kind, String target) {
        return new ChangeEntry(sequence, java.time.Instant.parse("2026-10-21T10:00:00Z"), "b", null, null, null, null, op, kind,
                target, "meeting:m-1", io.stintflow.spi.Sensitivity.INTERNAL, null, null, "Decision");
    }
}
