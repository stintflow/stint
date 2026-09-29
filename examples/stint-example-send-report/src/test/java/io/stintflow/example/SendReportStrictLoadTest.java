package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.DslValidationException;
import io.stintflow.dsl.LoadOptions;

/**
 * SDD 1.4, CA3: {@code send-report.yaml} loaded strictly (the default) fails, citing the unsupported
 * construct it contains — {@code schedule}. SDD 2.2 (sec. 8h, assertion change approved by Kayan on
 * 29/09/2026): {@code emit} is supported now, so it is no longer cited.
 */
class SendReportStrictLoadTest {

    @Test
    void strict_load_fails_citing_schedule_only() {
        assertThatThrownBy(() -> new DslLoader().loadClasspath("stint/workflows/*.yaml", LoadOptions.STRICT))
                .isInstanceOf(DslValidationException.class)
                .satisfies(e -> {
                    DslValidationException ex = (DslValidationException) e;
                    assertThat(ex.violations()).anySatisfy(v -> assertThat(v.pointer()).isEqualTo("/schedule"));
                    assertThat(ex.violations()).noneSatisfy(v -> assertThat(v.pointer()).contains("emit"));
                });
    }
}
