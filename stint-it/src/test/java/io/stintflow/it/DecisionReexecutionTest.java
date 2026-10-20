package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.wire.Json;

/**
 * SDD 3a.1, CA6: a decision re-executed by the transport or by a retry may answer differently (an LLM is not
 * deterministic); the first result applied is the one that counts, and the record shows the attempt that produced it
 * and the previous attempt's error.
 */
class DecisionReexecutionTest {

    static final String GUARDED = """
            do:
              - guarded:
                  try:
                    - classify:
                        call: decision
                        with:
                          task: decide
                          kind: choice
                          input: '${ {text: .text} }'
                          options:
                            - approve: {}
                            - reject: {}
                        timeout: { after: { seconds: 30 } }
                        export: { as: '${ {decided: .value} }' }
                  catch:
                    errors:
                      with:
                        type: https://serverlessworkflow.io/spec/1.0.0/errors/timeout
                    retry:
                      delay: { seconds: 1 }
                      limit:
                        attempt: { count: 2 }
            """;

    @Test
    void duplicate_result_keeps_the_first_decision() throws Exception {
        DecisionFixture f = new DecisionFixture(DecisionProvenanceTest.REPLACES_CONTEXT).manual();
        String id = f.start("item: aprovado");
        TaskInvocation inv = f.awaitDispatch();
        TaskResult first = f.answer(inv);
        f.deliver(first);
        f.awaitSaved(s -> s.instanceId().equals(id) && s.position().equals("/do/1/wait"));
        JsonNode recorded = f.decision(id, "/do/0/classify");

        // The same attempt answered again (a redelivery, or a worker that ran twice) — this time differently.
        TaskResult duplicate = TaskResult.completed(inv.correlationId(), Json.obj().put("value", "reject")
                .put("confidence", 0.99).put("uncertain", false)
                .set("provenance", Json.obj().put("adapter", "fake-rules").put("version", "1")));
        f.deliver(duplicate);
        f.deliver(first);

        assertThat(f.decision(id, "/do/0/classify")).isEqualTo(recorded);
        assertThat(f.snapshot(id).context().toString()).isEqualTo("{\"decided\":\"approve\"}");
    }

    @Test
    void late_result_after_timeout_is_ignored() throws Exception {
        DecisionFixture f = new DecisionFixture(GUARDED).manual();
        String id = f.start("item: aprovado");
        TaskInvocation attempt1 = f.awaitDispatch();
        f.awaitSaved(s -> s.instanceId().equals(id) && s.waitingKey() != null
                && s.waitingKey().equals("task:" + attempt1.correlationId()));

        f.fireTimer("timer:" + attempt1.correlationId());           // attempt 1 times out
        var retrying = f.awaitSaved(s -> s.instanceId().equals(id) && s.waitingKey() != null
                && s.waitingKey().startsWith("retry:"));
        f.fireTimer(retrying.waitingKey());
        TaskInvocation attempt2 = f.awaitDispatch();
        assertThat(attempt2.attempt()).isEqualTo(2);

        TaskResult late = f.answer(attempt1);                       // the slow worker finally answers attempt 1
        f.deliver(f.answer(withText(attempt2, "item: rejeitado"))); // attempt 2 answers first, differently
        f.deliver(late);

        assertThat(f.awaitCompleted(id).get("decided").asText()).isEqualTo("reject");
        JsonNode record = f.decision(id, "/do/0/guarded/do/0/classify");
        assertThat(record.get("value").asText()).isEqualTo("reject");
        assertThat(record.get("correlationId").asText()).isEqualTo(attempt2.correlationId());
    }

    @Test
    void retry_records_attempt_and_previous_error() throws Exception {
        DecisionFixture f = new DecisionFixture(GUARDED).manual();
        String id = f.start("item: aprovado");
        TaskInvocation attempt1 = f.awaitDispatch();
        f.awaitSaved(s -> s.instanceId().equals(id) && ("task:" + attempt1.correlationId()).equals(s.waitingKey()));
        f.fireTimer("timer:" + attempt1.correlationId());
        var retrying = f.awaitSaved(s -> s.instanceId().equals(id) && s.waitingKey() != null
                && s.waitingKey().startsWith("retry:"));
        f.fireTimer(retrying.waitingKey());
        f.deliver(f.answer(f.awaitDispatch()));

        f.awaitCompleted(id);
        JsonNode record = f.decision(id, "/do/0/guarded/do/0/classify");
        assertThat(record.get("attempt").asInt()).isEqualTo(2);
        assertThat(record.get("count").asInt()).isEqualTo(1);
        assertThat(record.get("previousError").toString())
                .isEqualTo("{\"type\":\"" + DecisionFixture.TIMEOUT + "\",\"status\":408}");
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    /** {@code inv}, with another synthetic text as the decision's input (what attempt 2 is asked about). */
    private static TaskInvocation withText(TaskInvocation inv, String text) {
        var input = inv.input().deepCopy();
        ((com.fasterxml.jackson.databind.node.ObjectNode) input).set("input", Json.obj().put("text", text));
        return new TaskInvocation(inv.workflowInstanceId(), inv.taskId(), inv.correlationId(), inv.definition(),
                inv.routingKey(), input, inv.attempt(), inv.lineage());
    }
}
