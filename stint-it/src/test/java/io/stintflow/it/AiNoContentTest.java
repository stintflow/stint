package io.stintflow.it;

import static io.stintflow.it.AiFixture.input;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import org.jboss.logmanager.ExtLogRecord;
import org.jboss.logmanager.LogContext;
import org.junit.jupiter.api.Test;

import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.otel.OpenTelemetryExecutionTracer;

/**
 * SDD 3a.3, CA9 (RF8): a real engine and worker, traced with OpenTelemetry — the transcript in the prompt and the text
 * the model answers never appear in any log record (message, parameters, MDC, exception) at any level, nor in any span
 * (name, attributes, events), on success and on failure. The model call is a {@code stint.llm} span, child of the
 * worker's {@code stint.task} span.
 */
class AiNoContentTest {

    /** Synthetic markers standing for a meeting transcript and for what the model says about it. */
    static final String TRANSCRIPT = "TRANSCRIPT-MARKER-c41e";
    static final String ANSWER = "ANSWER-MARKER-a9b0";

    static final String NOTES = """
            do:
              - summarize:
                  call: remote
                  with:
                    task: summarize
                    input:
                      text: ${ .text }
            """;

    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
            .addSpanProcessor(SimpleSpanProcessor.create(spans)).build().get("stint-sdd3a3"));

    @Test
    void neither_the_prompt_nor_the_answer_reaches_logs_or_spans() throws Exception {
        RuleModel model = new RuleModel(r -> "{\"topic\":\"" + ANSWER + "\",\"items\":1}");
        AiFixture f = new AiFixture(NOTES, model, b -> b, tracer);

        try (AllLogs logs = new AllLogs()) {
            f.awaitCompleted(f.start(input("text", TRANSCRIPT)));

            assertThat(model.requests.getFirst().messages().toString()).contains(TRANSCRIPT);   // the model got it
            assertThat(logs.lines).anySatisfy(line -> assertThat(line).contains("LLM usage of task /do/0/summarize"));
            assertThat(String.join("\n", logs.lines)).doesNotContain(TRANSCRIPT).doesNotContain(ANSWER);
        }
        List<SpanData> ended = spans.getFinishedSpanItems();
        SpanData llm = ended.stream().filter(s -> s.getName().equals("stint.llm")).findFirst().orElseThrow();
        SpanData task = ended.stream().filter(s -> s.getName().equals("stint.task")).findFirst().orElseThrow();
        assertThat(llm.getParentSpanId()).isEqualTo(task.getSpanId());
        assertThat(llm.getAttributes().asMap().keySet()).extracting(k -> k.getKey())
                .contains("stint.instanceId", "stint.taskId", "stint.chainId");
        assertThat(ended.toString()).doesNotContain(TRANSCRIPT).doesNotContain(ANSWER);
    }

    @Test
    void a_failure_quoting_the_prompt_does_not_reach_logs_or_spans_either() throws Exception {
        RuleModel model = new RuleModel(r -> {
            throw new IllegalStateException("provider error while reading: " + AiFixture.lastUserText(r.messages()));
        });
        AiFixture f = new AiFixture(NOTES, model, b -> b, tracer);

        try (AllLogs logs = new AllLogs()) {
            String failure = f.startFailing(input("text", TRANSCRIPT));

            assertThat(failure).contains("status=500").doesNotContain(TRANSCRIPT);
            assertThat(String.join("\n", logs.lines)).doesNotContain(TRANSCRIPT);
        }
        assertThat(spans.getFinishedSpanItems().toString()).doesNotContain(TRANSCRIPT);
        SpanData llm = spans.getFinishedSpanItems().stream().filter(s -> s.getName().equals("stint.llm")).findFirst()
                .orElseThrow();
        assertThat(llm.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);   // by class name only
        assertThat(llm.getStatus().getDescription()).isEqualTo("IllegalStateException");
    }

    /** Every record of every logger at every level, rendered with parameters, MDC and exception chain. */
    static final class AllLogs extends Handler implements AutoCloseable {
        final List<String> lines = new CopyOnWriteArrayList<>();
        private final org.jboss.logmanager.Logger root = LogContext.getLogContext().getLogger("");
        private final java.util.logging.Logger julRoot = java.util.logging.Logger.getLogger("");
        private final Level previous = root.getLevel();
        private final Level julPrevious = julRoot.getLevel();

        AllLogs() {
            setLevel(Level.ALL);
            root.setLevel(Level.ALL);
            root.addHandler(this);
            julRoot.setLevel(Level.ALL);
            julRoot.addHandler(this);
        }

        @Override
        public void publish(LogRecord record) {
            StringBuilder line = new StringBuilder(record.getLevel() + " " + record.getLoggerName() + " ");
            line.append(record instanceof ExtLogRecord ext ? ext.getFormattedMessage() : record.getMessage());
            if (record.getParameters() != null) {
                line.append(" params=").append(List.of(record.getParameters()));
            }
            if (record instanceof ExtLogRecord ext) {
                line.append(" mdc=").append(ext.getMdcCopy());
            }
            for (Throwable t = record.getThrown(); t != null; t = t.getCause()) {
                line.append(" thrown=").append(t);
            }
            lines.add(line.toString());
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            root.removeHandler(this);
            root.setLevel(previous);
            julRoot.removeHandler(this);
            julRoot.setLevel(julPrevious);
        }
    }
}
