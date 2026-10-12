package io.stintflow.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneId;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;

/**
 * SDD 1.4, CA3, as amended twice:
 * <ul>
 *   <li>SDD 2.2 (sec. 8h, assertion change approved by Kayan on 29/09/2026): {@code emit} is supported.</li>
 *   <li>SDD 2.4 (assertion change approved by Kayan on 01/10/2026): {@code schedule} is supported too, so
 *       {@code send-report.yaml} now loads strictly, and its schedule is compiled — {@code 0 9 * * MON} in UTC.</li>
 * </ul>
 */
class SendReportStrictLoadTest {

    @Test
    void strict_load_compiles_the_schedule() {
        List<WorkflowDefinition> defs = new DslLoader().loadClasspath("stint/workflows/*.yaml", LoadOptions.STRICT);

        WorkflowDefinition sendReport = defs.stream().filter(d -> d.ref().equals(SendReport.REF)).findFirst().orElseThrow();
        assertThat(sendReport.schedule().cron().expression()).isEqualTo("0 9 * * MON");
        assertThat(sendReport.schedule().zone()).isEqualTo(ZoneId.of("UTC"));
    }
}
