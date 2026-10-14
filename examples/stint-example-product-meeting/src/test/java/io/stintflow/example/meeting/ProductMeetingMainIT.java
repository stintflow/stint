package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.main.Launch;
import io.quarkus.test.junit.main.LaunchResult;
import io.quarkus.test.junit.main.QuarkusMainIntegrationTest;

/**
 * SDD 3a.6, CA1 (packaged): the JVM jar on {@code mvn verify}, the native binary with {@code -Pnative}, runs every
 * meeting of the corpus with fake models — each on its key's path, published once to the allowed space, its fact
 * emitted — and writes the metrics report.
 */
@QuarkusMainIntegrationTest
class ProductMeetingMainIT {

    @Test
    @Launch(exitCode = 0)
    void every_meeting_of_the_corpus_reaches_its_published_minutes(LaunchResult result) {
        assertThat(result.getOutput()).contains("PRODUCT-MEETING OK").contains("pages=8 facts=8")
                .contains("PRODUCT-MEETING m-008 COMPLETED docType=ata").contains("\"docTypeAccuracy\":1.0")
                .contains("PRODUCT-MEETING report=").doesNotContain("SYNTH-PII");
    }
}
