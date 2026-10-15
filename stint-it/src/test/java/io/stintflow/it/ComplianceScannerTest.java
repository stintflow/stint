package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.it.AiFixture.Chat;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.it.AiNoContentTest.AllLogs;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;
import io.stintflow.worker.redaction.LayeredRedactor;
import io.stintflow.worker.redaction.PatternLayer;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionHandler;
import io.stintflow.worker.redaction.TermListLayer;

/**
 * SDD 3a.5, the scanner (extras): a restricted meeting goes through a local model with memory, a restricted fact and
 * the instance it starts, a redaction that lowers it, a remote model and a confidential fact — and the synthetic
 * markers it carries ({@code SYNTH-PII-*}) appear in no log record at any level, no span, no stored snapshot (nor its
 * try input), no invoke or result on the queue, no fact, no audit record and no conversation exchange; the only blobs
 * that hold them are of the {@code restricted} class. The local model did see them: they flowed, and rested only where
 * E1 puts them. Fake models (they echo what they get), synthetic data.
 */
class ComplianceScannerTest {

    static final String EMAIL_MARKER = "SYNTH-PII-0401";
    static final String PERSON_MARKER = "SYNTH-PII-0402";
    static final String TRANSCRIPT = "Speaker " + PERSON_MARKER + " asked to mail " + EMAIL_MARKER + "@example.com.";

    static final String POLICY = """
            routes:
              restricted: [redactor, summarize-local]
              confidential: [redactor, summarize-local, summarize-remote]
              internal: ['*']""";

    static final String TRIAGE = SensitivityPolicyTest.header("triage", POLICY) + """
            do:
              - local:
                  try:
                    - summarize:
                        call: remote
                        with:
                          task: summarize-local
                          input: '${ {text: $context.transcript} }'
                  catch:
                    errors:
                      with:
                        type: https://serverlessworkflow.io/spec/1.0.0/errors/runtime
                  export: { as: '${ $context + {local: .} }' }
              - announce:
                  emit:
                    event:
                      with:
                        source: https://meetings.example
                        type: io.example.meeting.transcribed.v1
                        data: '${ {text: $context.transcript} }'
              - redact:
                  call: redact
                  with:
                    task: redactor
                    input: ${ $context.transcript }
                    lowersTo: confidential
                  export: { as: '${ {transcript: .output} }' }
              - remote:
                  call: remote
                  with:
                    task: summarize-remote
                    input: '${ {text: $context.transcript} }'
                  export: { as: '${ $context + {remote: .} }' }
              - publish:
                  emit:
                    event:
                      with:
                        source: https://meetings.example
                        type: io.example.meeting.summarized.v1
                        data: '${ {summary: $context.remote} }'
            """;

    static final String FOLLOW = SensitivityPolicyTest.header("follow", POLICY) + """
            do:
              - notes:
                  call: remote
                  with:
                    task: summarize-local
                    input: '${ {text: .[0].data.text} }'
            """;

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build().get("stint-sdd3a5"));

    private static TaskHandler summarizer(LangChain4jWorker worker) {
        return worker.handler(Chat.class, (ai, task) -> Json.obj().put("summary",
                ai.reply(task.input().path("text").asText())));
    }

    @Test
    void no_marker_rests_or_travels_outside_restricted_blobs() throws Exception {
        SensitivityPolicy workerPolicy = SensitivityPolicy.fromProperties(
                Map.of("stint.policy.confidential.providers", "approved-cloud"));
        InMemoryConversationStore memory = new InMemoryConversationStore();
        RuleModel localModel = RuleModel.echoing();
        RuleModel remoteModel = RuleModel.echoing();
        PolicyFixture f = new PolicyFixture(tracer, TRIAGE, FOLLOW);
        f.handler("redactor", new RedactionHandler(new LayeredRedactor("sandbox", "1", List.of(new PatternLayer(),
                        new TermListLayer("person", List.of(PERSON_MARKER)))),
                        new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8))))
                .handler("summarize-local", summarizer(LangChain4jWorker.builder().chatModel(localModel)
                        .provider("litjev", false).conversations(memory, f.blobs).policy(workerPolicy)
                        .tracer(tracer).clock(f.now::get).build()))
                .handler("summarize-remote", summarizer(LangChain4jWorker.builder().chatModel(remoteModel)
                        .provider("approved-cloud", "rule-model", true).conversations(memory, f.blobs)
                        .policy(workerPolicy).tracer(tracer).clock(f.now::get).build()));

        List<String> ids = new ArrayList<>();
        AllLogs logs = new AllLogs();
        try (logs) {
            String triage = f.engine.start(new WorkflowRef("meetings", "triage", "1.0.0"),
                    Json.obj().put("transcript", TRANSCRIPT));
            ids.add(triage);
            f.awaitCompleted(triage);
            CloudEvent restrictedFact = f.publisher.published().getFirst();
            String follower = f.engine.startFromEvent(new WorkflowRef("meetings", "follow", "1.0.0"), restrictedFact)
                    .toCompletableFuture().get(10, TimeUnit.SECONDS);
            ids.add(follower);
            f.awaitCompleted(follower);

            assertThat(f.snapshot(triage).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
            assertThat(f.snapshot(follower).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        }

        // they flowed: the local model saw them, twice (the meeting and the instance its fact started)
        assertThat(localModel.requests).hasSize(2)
                .allSatisfy(r -> assertThat(r.messages().toString()).contains(EMAIL_MARKER, PERSON_MARKER));
        assertThat(remoteModel.requests).singleElement()
                .satisfies(r -> assertThat(r.messages().toString()).doesNotContain(EMAIL_MARKER, PERSON_MARKER));

        List<String> places = new ArrayList<>();
        places.add("logs: " + String.join("\n", logs.lines));
        places.add("spans: " + spans.getFinishedSpanItems());
        for (String id : ids) {
            places.add("snapshot " + id + ": " + f.snapshot(id));
            places.add("memory " + id + ": " + memory.exchangesOf(id).toCompletableFuture().get(5, TimeUnit.SECONDS));
            places.add("audit " + id + ": " + f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS));
        }
        places.add("dispatched: " + f.dispatched);
        places.add("queue: " + String.join("\n", f.wire));
        for (CloudEvent fact : f.publisher.published()) {
            places.add("fact: " + new String(CeWire.toJson(fact), StandardCharsets.UTF_8));
        }
        assertThat(places).hasSizeGreaterThan(8).allSatisfy(place ->
                assertThat(place).doesNotContain(EMAIL_MARKER, PERSON_MARKER));
        assertThat(f.wire).isNotEmpty();
        assertThat(logs.lines).isNotEmpty();
        assertThat(spans.getFinishedSpanItems()).isNotEmpty();

        List<String> holding = blobsHolding(f.blobDir);
        assertThat(holding).isNotEmpty().allSatisfy(name -> assertThat(name).startsWith("retention_restricted_"));
    }

    private static List<String> blobsHolding(Path dir) throws Exception {
        try (Stream<Path> files = Files.list(dir)) {
            return files.filter(p -> {
                try {
                    String text = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
                    return text.contains(EMAIL_MARKER) || text.contains(PERSON_MARKER);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }).map(p -> p.getFileName().toString()).toList();
        }
    }
}
