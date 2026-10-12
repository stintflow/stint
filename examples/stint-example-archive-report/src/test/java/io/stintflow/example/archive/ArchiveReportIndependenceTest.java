package io.stintflow.example.archive;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.TaskNode;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;

/**
 * SDD 2.6, RF5/sec. 8c: archive-report is independent of the workflow publishing its fact. Besides the build's
 * {@code bannedDependencies} rule, nothing in this module — sources or resources — may name that workflow, its
 * definition or its routes. The only bridge is the fact type. Removing the banned words below would hide a
 * coupling, not fix it.
 */
class ArchiveReportIndependenceTest {

    /** What would betray knowledge of the publisher (compared case-insensitively). */
    private static final List<String> FORBIDDEN = List.of(
            "send-report", "send_report", "sendreport", "reports:send-report", "send-email");

    @Test
    void no_source_or_resource_names_the_publishing_workflow() throws IOException {
        Path module = Path.of("").toAbsolutePath();
        List<Path> files;
        try (Stream<Path> walk = Files.walk(module.resolve("src/main"))) {
            files = walk.filter(Files::isRegularFile).toList();
        }
        assertThat(files).isNotEmpty();
        for (Path file : files) {
            String text = Files.readString(file, StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
            for (String forbidden : FORBIDDEN) {
                assertThat(text).as("%s must not mention %s", module.relativize(file), forbidden)
                        .doesNotContain(forbidden);
            }
        }
    }

    @Test
    void the_definition_starts_only_on_the_fact_type_and_calls_only_its_own_route() {
        WorkflowDefinition def = new DslLoader()
                .loadClasspath("stint/workflows/*.yaml", LoadOptions.withExplicitResources(
                        List.of("stint/workflows/archive-report.yaml"))).get(0);

        assertThat(def.ref()).isEqualTo(ArchiveReport.REF);
        assertThat(def.schedule().cron()).isNull();
        assertThat(def.schedule().on()).singleElement().satisfies(filter -> {
            assertThat(filter.type()).isEqualTo(ArchiveReport.REPORT_SENT_TYPE);
            assertThat(filter.with()).isEmpty(); // not even the publisher's source
        });
        List<TaskNode> tasks = def.root().tasks();
        assertThat(tasks).allSatisfy(task -> assertThat(task).isInstanceOfSatisfying(CallRemoteNode.class,
                call -> assertThat(call.routingKey()).isEqualTo(ArchiveReport.ROUTE_ARCHIVE_RECORD)));
    }
}
