package io.stintflow.example.ai;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerPool;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.AdapterDescriptor;
import io.stintflow.worker.decision.Decision;
import io.stintflow.worker.decision.DecisionAdapter;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.DecisionTask;

/**
 * SDD 3a.2b, CA6 — the manual run, never the build: every synthetic meeting of a folder through
 * {@code meeting-triage} (two decisions), both answered by the Jev adapter's local profile against a real LitJev. Each
 * meeting file holds {@code {"label": {"doc": ..., "urgent": ...}, "transcript": ...}}; the label never reaches the
 * model. Prints one line per meeting — expected and decided values, the confidence by highest probability and by
 * LitJev's Gini, the latency — and the hits, confidences and latencies of the run.
 */
final class MeetingTriage {

    private MeetingTriage() {
    }

    static int run(Path folder, DecisionAdapter litjev) throws Exception {
        List<Path> meetings;
        try (Stream<Path> files = Files.list(folder)) {
            meetings = files.filter(p -> p.getFileName().toString().endsWith(".json")).sorted().toList();
        }
        if (meetings.isEmpty()) {
            System.out.println("TRIAGE no meeting (*.json) in " + folder);
            return 2;
        }
        Recording recording = new Recording(litjev);
        DecisionHandlers decisions = DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                        "stint.decision.routes.classify-meeting.adapter", litjev.descriptor().id(),
                        "stint.decision.routes.classify-meeting.instructions",
                        "Which document should this meeting produce?",
                        "stint.decision.routes.is-urgent.adapter", litjev.descriptor().id(),
                        "stint.decision.routes.is-urgent.instructions", "Does this meeting need action today?")))
                .adapter(recording).build();
        BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-triage"));
        TaskHandlerRegistry handlers = decisions.registerAll(new TaskHandlerRegistry());
        List<Row> rows = new ArrayList<>();
        try (WorkerPool pool = new WorkerPool(new WorkerRuntime(handlers, blobs), 1)) {
            InMemoryTaskTransport transport = new InMemoryTaskTransport();
            transport.connectWorker(invoke -> {
                CompletableFuture<CloudEvent> result = new CompletableFuture<>();
                try {
                    pool.submit(invoke, event -> {
                        result.complete(event);
                        return CompletableFuture.completedFuture(null);
                    }, () -> { });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    result.completeExceptionally(e);
                }
                return result;
            });
            WorkflowRegistry registry = new WorkflowRegistry();
            WorkflowDefinition def;
            try (InputStream yaml = MeetingTriage.class.getClassLoader()
                    .getResourceAsStream("stint/workflows/meeting-triage.yaml")) {
                def = new DslLoader().load(yaml, "meeting-triage.yaml", LoadOptions.STRICT);
            }
            registry.register(def);
            InMemoryStateStore states = new InMemoryStateStore();
            WorkflowEngine engine = new WorkflowEngine(registry, transport, states, new InMemoryTimerService(), blobs);

            System.out.println("TRIAGE adapter=" + litjev.descriptor());
            for (Path meeting : meetings) {
                JsonNode file = read(meeting);
                String id = engine.start(def.ref(), Json.obj().put("transcript", file.path("transcript").asText()));
                engine.awaitCompletion(id, Duration.ofMinutes(10)).toCompletableFuture().get(11, TimeUnit.MINUTES);
                JsonNode records = states.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow()
                        .decisions();
                Row row = new Row(meeting.getFileName().toString(), file.path("label"),
                        records.path("/do/1/classify"), records.path("/do/2/urgent"),
                        recording.of(id, "choice"), recording.of(id, "boolean"));
                rows.add(row);
                System.out.println(row);
            }
        }
        summary(rows);
        return 0;
    }

    private static JsonNode read(Path meeting) throws IOException {
        return Json.read(Files.readAllBytes(meeting));
    }

    private static void summary(List<Row> rows) {
        long docHits = rows.stream().filter(Row::docHit).count();
        long urgentHits = rows.stream().filter(Row::urgentHit).count();
        System.out.printf(Locale.ROOT, "TRIAGE doc: %d/%d hits; urgent: %d/%d hits%n", docHits, rows.size(),
                urgentHits, rows.size());
        System.out.println("TRIAGE doc confidence (highest probability), hits:   "
                + stats(rows.stream().filter(Row::docHit).map(r -> r.doc.confidence).toList()));
        System.out.println("TRIAGE doc confidence (highest probability), misses: "
                + stats(rows.stream().filter(r -> !r.docHit()).map(r -> r.doc.confidence).toList()));
        System.out.println("TRIAGE doc confidence (Gini), all:                   "
                + stats(rows.stream().map(r -> r.doc.gini).toList()));
        System.out.println("TRIAGE urgent confidence, hits:   "
                + stats(rows.stream().filter(Row::urgentHit).map(r -> r.urgent.confidence).toList()));
        System.out.println("TRIAGE urgent confidence, misses: "
                + stats(rows.stream().filter(r -> !r.urgentHit()).map(r -> r.urgent.confidence).toList()));
        List<Double> latencies = new ArrayList<>();
        rows.forEach(r -> {
            latencies.add(r.doc.latencyMs);
            latencies.add(r.urgent.latencyMs);
        });
        System.out.println("TRIAGE latency per decision (ms): " + stats(latencies) + ", p95 "
                + format(percentile(latencies, 0.95)));
    }

    private static String stats(Collection<Double> values) {
        if (values.isEmpty()) {
            return "none";
        }
        List<Double> sorted = values.stream().sorted().toList();
        return "n=" + sorted.size() + " min " + format(sorted.getFirst()) + " median " + format(percentile(sorted, 0.5))
                + " max " + format(sorted.getLast());
    }

    private static double percentile(List<Double> values, double q) {
        List<Double> sorted = values.stream().sorted().toList();
        int index = (int) Math.ceil(q * sorted.size()) - 1;
        return sorted.get(Math.clamp(index, 0, sorted.size() - 1));
    }

    private static String format(double value) {
        return String.format(Locale.ROOT, "%.3f", value);
    }

    /** What one decision of one meeting got: the recorded value and confidence, the Gini, the adapter's latency. */
    record Outcome(String value, double confidence, boolean uncertain, double gini, double latencyMs) {

        static Outcome of(JsonNode record, Decision raw) {
            double gini = raw == null || raw.distribution() == null ? Double.NaN : gini(raw.distribution().values());
            double latency = raw == null || raw.usage() == null ? Double.NaN : raw.usage().latency().toNanos() / 1e6;
            return new Outcome(record.path("value").asText(), record.path("confidence").asDouble(Double.NaN),
                    record.path("uncertain").asBoolean(), gini, latency);
        }

        /** LitJev's confidence: {@code (K·Σp² − 1)/(K − 1)}. */
        static double gini(Collection<Double> p) {
            int k = p.size();
            return k <= 1 ? 1 : (k * p.stream().mapToDouble(x -> x * x).sum() - 1) / (k - 1);
        }
    }

    record Row(String file, String expectedDoc, String expectedUrgent, Outcome doc, Outcome urgent) {

        Row(String file, JsonNode label, JsonNode doc, JsonNode urgent, Decision rawDoc, Decision rawUrgent) {
            this(file, label.path("doc").asText(), label.path("urgent").asText(), Outcome.of(doc, rawDoc),
                    Outcome.of(urgent, rawUrgent));
        }

        boolean docHit() {
            return expectedDoc.equals(doc.value);
        }

        boolean urgentHit() {
            return expectedUrgent.equals(urgent.value);
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT,
                    "TRIAGE %s doc %s->%s%s (p %.3f, gini %.3f, %s, %.0f ms) urgent %s->%s%s (%.3f, %s, %.0f ms)", file,
                    expectedDoc, doc.value, docHit() ? "" : " MISS", doc.confidence, doc.gini,
                    doc.uncertain ? "uncertain" : "certain", doc.latencyMs, expectedUrgent, urgent.value,
                    urgentHit() ? "" : " MISS", urgent.confidence, urgent.uncertain ? "uncertain" : "certain",
                    urgent.latencyMs);
        }
    }

    /** Keeps each raw answer — its distribution and latency — by instance and kind, for the report only. */
    static final class Recording implements DecisionAdapter {
        private final DecisionAdapter delegate;
        private final Map<String, Decision> answers = new ConcurrentHashMap<>();

        Recording(DecisionAdapter delegate) {
            this.delegate = delegate;
        }

        @Override
        public AdapterDescriptor descriptor() {
            return delegate.descriptor();
        }

        @Override
        public CompletionStage<Decision> decide(DecisionRequest request, DecisionTask task) {
            return delegate.decide(request, task).thenApply(decision -> {
                answers.put(task.task().instanceId() + "/" + request.kind().wireName(), decision);
                return decision;
            });
        }

        Decision of(String instanceId, String kind) {
            return answers.get(instanceId + "/" + kind);
        }
    }
}
