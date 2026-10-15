package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * SDD 3a.1, CA7 (RF7): a decision's input never reaches a log line or an error. The synthetic text carries a marker;
 * the adapter echoes it back in an out-of-contract output, which is the worst case.
 */
class DecisionLoggingTest {

    private static final String MARKER = "SYNTHETIC-MARKER-7f3a";

    @Test
    void the_decision_input_never_reaches_logs_or_errors() throws Exception {
        try (DecisionFixture.LogCapture logs = new DecisionFixture.LogCapture()) {
            DecisionFixture valid = new DecisionFixture(DecisionBranchingTest.CLASSIFY);
            valid.awaitCompleted(valid.start("aprovado " + MARKER));

            DecisionFixture broken = new DecisionFixture(DecisionValidationTest.UNCAUGHT);
            String failure = broken.startFailing("quebrado " + MARKER);

            assertThat(failure).doesNotContain(MARKER);
            assertThat(logs.lines).noneMatch(line -> line.contains(MARKER));
            // What is logged about an applied decision: its record, without the input.
            assertThat(logs.lines).anySatisfy(line -> assertThat(line)
                    .contains("Recorded the output of /do/0/classify (attempt 1)")
                    .contains("\"value\":\"approve\"").contains("\"adapter\":\"fake-rules\""));
            // The expansion is logged at DEBUG while loading (RF8).
            assertThat(logs.lines).anySatisfy(line -> assertThat(line)
                    .contains("'call: decision' expands to").contains("classify/route"));
        }
    }
}
