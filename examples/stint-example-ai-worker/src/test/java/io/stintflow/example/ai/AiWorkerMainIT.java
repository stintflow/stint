package io.stintflow.example.ai;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;

/**
 * SDD 3a.3, CA4: the packaged example — the JVM jar on {@code mvn verify}, the native binary with {@code -Pnative} —
 * runs two LLM steps of one instance that share its conversation memory.
 */
@QuarkusMainIntegrationTest
class AiWorkerMainIT {

    @Test
    @Launch(exitCode = 0)
    void two_llm_steps_share_the_instance_memory(LaunchResult result) {
        assertThat(result.getOutput()).contains("AI-WORKER OK")
                .contains("\"topic\":\"quarterly planning\"")
                .contains("\"contextMessages\":3");
    }
}
