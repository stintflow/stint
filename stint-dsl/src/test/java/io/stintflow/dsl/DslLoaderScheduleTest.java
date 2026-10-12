package io.stintflow.dsl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.model.Schedule;

/**
 * SDD 2.4, RF1: DSL 1.0 {@code schedule} compiles — {@code cron}, {@code every} (as a cron), {@code on}
 * ({@code one}/{@code any}) and the time zone of the Stint extension {@code document.metadata}; what has no
 * equivalent is reported with its pointer (sec. 8b/8c/8f).
 */
class DslLoaderScheduleTest {

    private static InputStream yaml(String metadata, String schedule) {
        String doc = """
                document:
                  dsl: '1.0.0'
                  namespace: reports
                  name: sample
                  version: '1.0.0'
                """ + metadata + schedule + """
                do:
                  - noop:
                      set: { ok: true }
                """;
        return new ByteArrayInputStream(doc.getBytes(StandardCharsets.UTF_8));
    }

    private static WorkflowDefinition load(String metadata, String schedule) {
        return new DslLoader().load(yaml(metadata, schedule), "sample.yaml", LoadOptions.STRICT);
    }

    @Test
    void cron_in_the_time_zone_of_the_stint_metadata_extension() {
        WorkflowDefinition def = load("""
                  metadata:
                    stint.schedule.timezone: America/Sao_Paulo
                """, """
                schedule:
                  cron: '0 7 * * 1-5'
                """);

        Schedule schedule = def.schedule();
        assertThat(schedule.cron().expression()).isEqualTo("0 7 * * 1-5");
        assertThat(schedule.zone()).isEqualTo(ZoneId.of("America/Sao_Paulo"));
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        assertThat(registry.scheduleSpecs()).singleElement()
                .satisfies(spec -> assertThat(spec.zone()).isEqualTo(ZoneId.of("America/Sao_Paulo")));
    }

    @Test
    void utc_by_default_and_every_compiles_into_a_cron() {
        WorkflowDefinition def = load("", """
                schedule:
                  every: { minutes: 15 }
                """);

        assertThat(def.schedule().zone()).isEqualTo(ZoneId.of("UTC"));
        assertThat(def.schedule().cron().expression()).isEqualTo("*/15 * * * *");
    }

    @Test
    void on_compiles_exact_match_start_filters() {
        WorkflowDefinition def = load("", """
                schedule:
                  on:
                    any:
                      - with: { type: io.stintflow.reports.report-sent.v1, source: https://stintflow.io/reports/send-report }
                      - with: { type: io.acme.manual-archive.v1 }
                """);

        assertThat(def.schedule().cron()).isNull();
        assertThat(def.schedule().on()).hasSize(2);
        assertThat(def.schedule().on().get(0).type()).isEqualTo("io.stintflow.reports.report-sent.v1");
        assertThat(def.schedule().on().get(0).with()).containsEntry("source", "https://stintflow.io/reports/send-report");
    }

    @Test
    void what_has_no_equivalent_is_reported_with_its_pointer() {
        assertRejected("", "schedule:\n  cron: '0 0 1 * MON'\n", "/schedule/cron");
        assertRejected("", "schedule:\n  cron: '0 0 * * */2'\n", "/schedule/cron");
        assertRejected("", "schedule:\n  every: { minutes: 7 }\n", "/schedule/every");
        assertRejected("", "schedule:\n  after: { hours: 1 }\n", "/schedule/after");
        assertRejected("", "schedule:\n  on:\n    all:\n      - with: { type: a.v1 }\n", "/schedule/on");
        assertRejected("", "schedule:\n  on:\n    one:\n      with: { type: a.v1 }\n      correlate: { k: { from: .x, expect: y } }\n",
                "/schedule/on/one/correlate");
        assertRejected("  metadata:\n    stint.schedule.timezone: Mars/Olympus\n", "schedule:\n  cron: '0 7 * * *'\n",
                "/document/metadata/stint.schedule.timezone");
    }

    private static void assertRejected(String metadata, String schedule, String pointer) {
        assertThatThrownBy(() -> load(metadata, schedule))
                .isInstanceOf(DslValidationException.class)
                .satisfies(e -> assertThat(((DslValidationException) e).violations())
                        .anySatisfy(v -> assertThat(v.pointer()).isEqualTo(pointer)));
    }
}
