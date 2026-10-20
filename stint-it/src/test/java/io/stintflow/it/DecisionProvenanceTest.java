package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.InstanceSnapshot.InstanceStatus;

/** SDD 3a.1, CA3: the decision's provenance is in the snapshot, whatever the author does with $context. */
class DecisionProvenanceTest {

    static final String REPLACES_CONTEXT = """
            do:
              - classify:
                  call: decision
                  with:
                    task: decide
                    kind: choice
                    input: '${ {text: .text} }'
                    options:
                      - approve: { description: "Approved" }
                      - reject: { description: "Rejected" }
                  export: { as: '${ {decided: .value} }' }
              - wait:
                  call: remote
                  with:
                    task: never-answers
            """;

    @Test
    void snapshot_records_the_decision_provenance() throws Exception {
        DecisionFixture f = new DecisionFixture(REPLACES_CONTEXT);

        String id = f.start("item: aprovado");
        // The instance moves on to a task nobody answers, so the decision is read back from a saved, suspended snapshot.
        var suspended = f.awaitSaved(s -> s.instanceId().equals(id) && s.status() == InstanceStatus.WAITING
                && s.position().equals("/do/1/wait"));

        assertThat(suspended.context().toString()).isEqualTo("{\"decided\":\"approve\"}"); // the author's export.as
        assertProvenance(f.decision(id, "/do/0/classify"), f.dispatched.peek().correlationId());
    }

    static void assertProvenance(JsonNode record, String correlationId) {
        assertThat(record.get("kind").asText()).isEqualTo("choice");
        assertThat(record.get("value").asText()).isEqualTo("approve");
        assertThat(record.get("confidence").asDouble()).isEqualTo(0.9);
        assertThat(record.get("uncertain").asBoolean()).isFalse();
        assertThat(record.get("provenance").toString())
                .isEqualTo("{\"adapter\":\"fake-rules\",\"provider\":null,\"model\":null,\"version\":\"1\"}");
        assertThat(record.get("attempt").asInt()).isEqualTo(1);
        assertThat(record.get("correlationId").asText()).isEqualTo(correlationId);
        assertThat(record.get("decidedAt").asText()).isEqualTo(DecisionFixture.T0.toString());
        assertThat(record.get("count").asInt()).isEqualTo(1);
        assertThat(record.has("previousError")).isFalse();
        // Never the input, the options, or the adapter's extra fields.
        assertThat(record.has("input")).isFalse();
        assertThat(record.has("options")).isFalse();
        assertThat(record.has("reasoning")).isFalse();
    }
}
