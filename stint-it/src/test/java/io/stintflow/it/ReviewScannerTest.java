package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.it.AiNoContentTest.AllLogs;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.wire.review.CorrectionExport;

/**
 * SDD 3a.4, the scanner (extras, 4.6): a restricted meeting's uncertain decision goes to review; a channel answers
 * once out of contract with a free-text note carrying a marker, then properly, then again after the instance ended.
 * The synthetic marker ({@code SYNTH-PII-*}) is in no log record at any level, no span, no published event, no inbox
 * entry, no stored snapshot, no audit record, no correction and no export line. Synthetic data only.
 */
class ReviewScannerTest {

    static final String INPUT_MARKER = "SYNTH-PII-0601";
    static final String NOTE_MARKER = "SYNTH-PII-0602";

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build().get("stint-sdd3a4"));

    @Test
    void no_marker_in_events_inbox_logs_spans_snapshot_audit_or_export() throws Exception {
        PolicyFixture f = new PolicyFixture(tracer, ReviewFlowTest.TRIAGE_YAML).worker("decide-type", in -> Json.obj()
                .put("value", "ata").put("confidence", 0.3).put("uncertain", true)
                .set("provenance", Json.obj().put("adapter", "litjev").put("version", "1")));
        String id;
        StringWriter export = new StringWriter();
        AllLogs logs = new AllLogs();
        try (logs) {
            id = f.engine.start(ReviewFlowTest.TRIAGE, Json.obj().put("ref", ReviewFlowTest.MATERIAL)
                    .put("text", "Speaker " + INPUT_MARKER), Sensitivity.RESTRICTED);
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (f.publisher.published().isEmpty() && System.nanoTime() < until) {
                Thread.onSpinWait();
            }
            String reviewId = f.publisher.published().getFirst().getSubject();

            deliver(f, ReviewFlowTest.answer("bad", reviewId, ReviewFlowTest.PORTAL, "business-rep",
                    Json.obj().put("value", "adr").put("reviewer", "rev-1").put("note", "about " + NOTE_MARKER)));
            deliver(f, ReviewFlowTest.answer("good", reviewId, ReviewFlowTest.PORTAL, "business-rep",
                    Json.obj().put("value", "adr").put("reviewer", "rev-1")));
            f.awaitCompleted(id);
            deliver(f, ReviewFlowTest.answer("after", reviewId, ReviewFlowTest.PORTAL, "business-rep",
                    Json.obj().put("value", "ata").put("reviewer", "rev-1").put("note", NOTE_MARKER)));

            CorrectionExport.write(f.corrections.of(ReviewFlowTest.TRIAGE).toCompletableFuture().get(),
                    Sensitivity.RESTRICTED, export);
        }

        List<String> places = new ArrayList<>();
        places.add("logs: " + String.join("\n", logs.lines));
        places.add("spans: " + spans.getFinishedSpanItems());
        for (CloudEvent event : f.publisher.published()) {
            places.add("event: " + new String(CeWire.toJson(event), StandardCharsets.UTF_8));
        }
        places.add("inbox: " + f.inbox.kept);
        places.add("snapshot: " + f.snapshot(id));
        places.add("audit: " + f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS));
        places.add("corrections: " + f.corrections.of(ReviewFlowTest.TRIAGE).toCompletableFuture().get());
        places.add("export: " + export);
        assertThat(places).allSatisfy(place -> assertThat(place).doesNotContain(INPUT_MARKER, NOTE_MARKER));

        assertThat(f.publisher.published()).anyMatch(e -> e.getType().equals(StintEvents.TYPE_REVIEW_REQUESTED));
        assertThat(export.toString()).contains("\"agrees\":false");
        assertThat(f.audit.of(id).toCompletableFuture().get()).anyMatch(r -> "out-of-contract".equals(r.outcome()));
        assertThat(logs.lines).anyMatch(line -> line.contains("out of the review contract; dropped, not kept"));
        assertThat(spans.getFinishedSpanItems()).isNotEmpty();
    }

    private static void deliver(PolicyFixture f, CloudEvent event) throws Exception {
        f.engine.onDomainEvent(event, Duration.ofMinutes(5)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
