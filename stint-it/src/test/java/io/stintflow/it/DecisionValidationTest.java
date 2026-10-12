package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.stintflow.spi.InstanceSnapshot.InstanceStatus;

/**
 * SDD 3a.1, CA4: an output out of the decision contract is the standard {@code validation} error (400), raised at the
 * task, and catchable like any failure of a try's body.
 */
class DecisionValidationTest {

    static final String UNCAUGHT = """
            do:
              - classify:
                  call: decision
                  with:
                    task: decide
                    kind: choice
                    input: '${ {text: .text} }'
                    options:
                      - approve: {}
                      - reject: {}
            """;

    static final String RETRY = """
                    retry:
                      delay: { seconds: 1 }
                      limit:
                        attempt: { count: 2 }
            """.stripTrailing();

    static final String CAUGHT = """
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
                            - approve: { then: approved }
                            - reject: {}
                  catch:
                    errors:
                      with:
                        type: https://serverlessworkflow.io/spec/1.0.0/errors/validation
            %s
                    then: fallback
              - fallback:
                  set: '${ {outcome: "fallback"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - approved:
                  set: '${ {outcome: "approved"} }'
                  export: { as: '${ $context + . }' }
                  then: end
            """;

    @Test
    void out_of_contract_output_is_a_validation_error() throws Exception {
        DecisionFixture f = new DecisionFixture(UNCAUGHT);

        String failure = f.startFailing("quebrado");

        assertThat(failure).contains("type=" + DecisionFixture.VALIDATION).contains("status=400")
                .contains("title=Decision output out of contract")
                .contains("value is not one of the options").contains("confidence must be a number in [0,1]")
                .contains("uncertain must be a boolean").contains("instance=/do/0/classify");
    }

    @Test
    void uncaught_validation_error_fails_the_instance() throws Exception {
        DecisionFixture f = new DecisionFixture(UNCAUGHT);

        String id = f.start("quebrado");
        f.awaitSaved(snap -> snap.instanceId().equals(id) && snap.status() == InstanceStatus.FAILED);

        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.snapshot(id).decisions()).isNull(); // an invalid output is never recorded
    }

    @Test
    void validation_error_is_caught_and_retried() throws Exception {
        DecisionFixture f = new DecisionFixture(CAUGHT.formatted(RETRY));

        String id = f.start("quebra-uma-vez: aprovado");
        var retrying = f.awaitSaved(s -> s.instanceId().equals(id) && s.waitingKey() != null
                && s.waitingKey().startsWith("retry:"));
        assertThat(retrying.retryState().lastError().type()).hasToString(DecisionFixture.VALIDATION);
        f.fireTimer(retrying.waitingKey());

        assertThat(f.awaitCompleted(id).get("outcome").asText()).isEqualTo("approved");
        assertThat(f.dispatched).hasSize(2);
    }

    @Test
    void validation_error_caught_without_retry_follows_the_catch() throws Exception {
        DecisionFixture f = new DecisionFixture(CAUGHT.formatted(""));

        assertThat(f.awaitCompleted(f.start("quebrado")).get("outcome").asText()).isEqualTo("fallback");
    }
}
