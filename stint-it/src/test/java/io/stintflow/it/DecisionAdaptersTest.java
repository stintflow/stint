package io.stintflow.it;

import static io.stintflow.it.AiFixture.input;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TimerRequest;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.Decision;
import io.stintflow.worker.decision.DecisionAdapter;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.FakeDecisionAdapter;
import io.stintflow.worker.decision.jev.FakeJevServer;
import io.stintflow.worker.decision.jev.FakeJevServer.Reply;
import io.stintflow.worker.decision.jev.JevDecisionAdapter;
import io.stintflow.worker.langchain4j.LlmDecisionAdapter;

/**
 * SDD 3a.2, end to end: {@code call: decision} in workflows loaded from YAML, answered by the decision adapters through
 * the worker runtime — the LLM adapter over a fake LangChain4j model, the Jev adapter over a fake server, the fake
 * adapter. Synthetic data only; no provider is ever called.
 */
class DecisionAdaptersTest {

    static final String VALIDATION = "https://serverlessworkflow.io/spec/1.0.0/errors/validation";
    static final String COMMUNICATION = "https://serverlessworkflow.io/spec/1.0.0/errors/communication";

    /** The meeting classification of the POC: ata, adr or runbook, and a review when uncertain. */
    static final String CLASSIFY = """
            do:
              - classify:
                  call: decision
                  with:
                    task: classify-meeting
                    kind: choice
                    input: '${ {text: .text} }'
                    options:
                      - ata: { description: "Minutes of what was discussed", then: minutes }
                      - adr: { description: "A record of an architecture decision", then: record }
                      - runbook: { description: "Operating steps", then: steps }
                  onUncertain: review
              - minutes:
                  set: '${ {doc: "ata"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - record:
                  set: '${ {doc: "adr"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - steps:
                  set: '${ {doc: "runbook"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - review:
                  set: '${ {doc: "review"} }'
                  export: { as: '${ $context + . }' }
                  then: end
            """;

    private final FakeJevServer jev;

    DecisionAdaptersTest() throws Exception {
        jev = new FakeJevServer();
    }

    @AfterEach
    void stop() {
        jev.close();
    }

    /** The LLM answers by a rule over the input: "clear" → adr 0.95; "vague" → ata 0.5; "memo" → out of the options. */
    static RuleModel meetingModel() {
        return new RuleModel(request -> {
            String input = AiFixture.lastUserText(request.messages());
            if (input.contains("broken")) {
                return "I think this is an ADR, but I won't say it as JSON.";
            }
            if (input.contains("memo")) {
                return "{\"value\":\"memo\",\"confidence\":0.9}";
            }
            return input.contains("clear") ? "{\"value\":\"adr\",\"confidence\":0.95}"
                    : "{\"value\":\"ata\",\"confidence\":0.5}";
        });
    }

    private LlmDecisionAdapter llm(RuleModel model, ExecutionTracer tracer) {
        return LlmDecisionAdapter.builder().id("llm").chatModel(model).provider("fake-llm").tracer(tracer).build();
    }

    private JevDecisionAdapter jevAdapter() {
        return JevDecisionAdapter.builder().id("jev").baseUri(jev.baseUri()).apiKey(() -> "test").build();
    }

    private AiFixture fixture(String tasks, Map<String, String> routes, ExecutionTracer tracer,
            DecisionAdapter... adapters) throws Exception {
        Map<String, String> props = new HashMap<>(routes);
        RuleModel model = meetingModel();
        List<DecisionAdapter> all = adapters.length > 0 ? List.of(adapters) : List.of(llm(model, tracer));
        if (all.stream().anyMatch(a -> a.descriptor().id().equals("llm"))) {
            props.put("stint.decision.adapters.llm.threshold", "0.8");
        }
        DecisionHandlers.Builder handlers = DecisionHandlers.builder(DecisionConfig.fromProperties(props));
        for (DecisionAdapter adapter : all) {
            handlers.adapter(adapter);
        }
        DecisionHandlers built = handlers.build();
        return new AiFixture(tasks, model, b -> b, tracer, built::registerAll);
    }

    private AiFixture classifying(DecisionAdapter... adapters) throws Exception {
        return fixture(CLASSIFY, Map.of("stint.decision.routes.classify-meeting.adapter", "llm",
                "stint.decision.routes.classify-meeting.instructions", "Which document does this meeting produce?"),
                ExecutionTracer.NOOP, adapters);
    }

    @Test
    void the_same_input_through_two_adapters_records_two_provenances() throws Exception {
        jev.reply(Reply.ok("""
                {"model":"jev-1.13.0","answers":{"decision":{"type":"choice","choice":"adr",
                 "probabilities":{"ata":0.1,"adr":0.85,"runbook":0.05},"confidence":0.85}},
                 "usage":{"input_tokens":30,"output_tokens":2}}"""));
        String twoAdapters = """
                do:
                  - keep:
                      set: '${ . }'
                      export: { as: '${ {text: .text} }' }
                  - byLlm:
                      input: { from: '${ $context }' }
                      call: decision
                      with: { task: classify-llm, kind: choice, options: [ {ata: {}}, {adr: {}}, {runbook: {}} ] }
                  - byJev:
                      input: { from: '${ $context }' }
                      call: decision
                      with: { task: classify-jev, kind: choice, options: [ {ata: {}}, {adr: {}}, {runbook: {}} ] }
                """;
        AiFixture f = fixture(twoAdapters, Map.of("stint.decision.routes.classify-llm.adapter", "llm",
                "stint.decision.routes.classify-jev.adapter", "jev"), ExecutionTracer.NOOP,
                llm(meetingModel(), ExecutionTracer.NOOP), jevAdapter());

        String id = f.start(input("text", "a clear synthetic decision meeting"));
        f.awaitCompleted(id);

        JsonNode byLlm = f.decision(id, "/do/1/byLlm");
        JsonNode byJev = f.decision(id, "/do/2/byJev");
        assertThat(byLlm.path("value").asText()).isEqualTo("adr");
        assertThat(byLlm.path("provenance").path("adapter").asText()).isEqualTo("llm");
        assertThat(byLlm.path("provenance").path("provider").asText()).isEqualTo("fake-llm");
        assertThat(byLlm.path("provenance").path("model").asText()).isEqualTo("rule-model");
        assertThat(byLlm.path("provenance").path("version").asText()).isEqualTo(LlmDecisionAdapter.PROMPT_VERSION);
        assertThat(byJev.path("value").asText()).isEqualTo("adr");
        assertThat(byJev.path("provenance").path("adapter").asText()).isEqualTo("jev");
        assertThat(byJev.path("provenance").path("provider").asText()).isEqualTo("typesafe");
        assertThat(byJev.path("provenance").path("model").asText()).isEqualTo("jev-1.13.0");
        assertThat(jev.requests.getFirst().path("state").path("text").asText())
                .isEqualTo("a clear synthetic decision meeting");   // the same input reached both
    }

    @Test
    void below_the_threshold_goes_to_on_uncertain_and_above_it_to_the_option_s_branch() throws Exception {
        AiFixture f = classifying();

        JsonNode clear = f.awaitCompleted(f.start(input("text", "a clear synthetic meeting")));
        String vagueId = f.start(input("text", "a vague synthetic meeting"));
        JsonNode vague = f.awaitCompleted(vagueId);

        assertThat(clear.path("doc").asText()).isEqualTo("adr");
        assertThat(vague.path("doc").asText()).isEqualTo("review");
        JsonNode record = f.decision(vagueId, "/do/0/classify");
        assertThat(record.path("value").asText()).isEqualTo("ata");
        assertThat(record.path("confidence").asDouble()).isEqualTo(0.5);
        assertThat(record.path("uncertain").asBoolean()).isTrue();
    }

    @Test
    void a_malformed_llm_answer_is_a_validation_error_the_workflow_catches() throws Exception {
        String guarded = """
                do:
                  - guarded:
                      try:
                        - classify:
                            call: decision
                            with:
                              task: classify-meeting
                              kind: choice
                              input: '${ {text: .text} }'
                              options:
                                - ata: {}
                                - adr: {}
                      catch:
                        errors:
                          with:
                            type: %s
                        then: fallback
                  - fallback:
                      set: '${ {doc: "fallback"} }'
                      export: { as: '${ $context + . }' }
                      then: end
                """.formatted(VALIDATION);
        AiFixture f = fixture(guarded, Map.of("stint.decision.routes.classify-meeting.adapter", "llm"),
                ExecutionTracer.NOOP);

        assertThat(f.awaitCompleted(f.start(input("text", "a broken synthetic meeting"))).path("doc").asText())
                .isEqualTo("fallback");
    }

    @Test
    void a_value_outside_the_options_fails_with_a_validation_error_without_the_value_or_the_input() throws Exception {
        AiFixture f = classifying();

        String failure = f.startFailing(input("text", "a synthetic memo-like meeting"));

        assertThat(failure).contains("type=" + VALIDATION).contains("status=400").contains("instance=/do/0/classify")
                .contains("not one of the options").doesNotContain("memo");
    }

    @Test
    void a_jev_rate_limit_is_retried_no_sooner_than_its_retry_after() throws Exception {
        jev.reply(Reply.status(429, Map.of("Retry-After", "30")));
        jev.reply(Reply.ok("""
                {"model":"jev-1.13.0","answers":{"decision":{"type":"choice","choice":"ata",
                 "probabilities":{"ata":0.9,"adr":0.1},"confidence":0.9}}}"""));
        String retried = """
                do:
                  - guarded:
                      try:
                        - classify:
                            call: decision
                            with:
                              task: classify-meeting
                              kind: choice
                              input: '${ {text: .text} }'
                              options:
                                - ata: {}
                                - adr: {}
                      catch:
                        errors:
                          with:
                            type: %s
                        retry:
                          delay: { seconds: 1 }
                          limit:
                            attempt: { count: 3 }
                """.formatted(COMMUNICATION);
        AiFixture f = fixture(retried, Map.of("stint.decision.routes.classify-meeting.adapter", "jev"),
                ExecutionTracer.NOOP, jevAdapter());

        String id = f.start(input("text", "a synthetic meeting"));
        TimerRequest retry = f.awaitRetryTimer();

        assertThat(retry.fireAt()).isEqualTo(AiFixture.T0.plusSeconds(30));   // the Retry-After, not the 1 s delay
        f.fire(retry);
        f.awaitCompleted(id);
        JsonNode record = f.decision(id, "/do/0/guarded/do/0/classify");
        assertThat(record.path("value").asText()).isEqualTo("ata");
        assertThat(record.path("attempt").asInt()).isEqualTo(2);
        assertThat(record.path("previousError").path("status").asInt()).isEqualTo(429);
        assertThat(jev.requests).hasSize(2);
    }

    @Test
    void a_reexecution_keeps_the_first_decision_recorded() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        FakeDecisionAdapter flipping = new FakeDecisionAdapter("fake", request -> Decision.of(
                JsonNodeFactory.instance.textNode(calls.incrementAndGet() == 1 ? "approve" : "reject"), 0.9));
        WorkerRuntime worker = new WorkerRuntime(DecisionHandlers.builder(DecisionConfig.fromProperties(Map.of(
                "stint.decision.routes.decide.adapter", "fake"))).adapter(flipping).build()
                .registerAll(new TaskHandlerRegistry()), null);
        DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
        DecisionFixture f = new DecisionFixture("""
                do:
                  - classify:
                      call: decision
                      with:
                        task: decide
                        kind: choice
                        input: '${ {text: .text} }'
                        options:
                          - approve: {}
                          - reject: {}
                """).manual();

        String id = f.start("synthetic request");
        TaskInvocation invocation = f.awaitDispatch();
        TaskResult first = codec.toResult(worker.handle(codec.toEvent(invocation)).toCompletableFuture()
                .get(10, TimeUnit.SECONDS));
        TaskResult again = codec.toResult(worker.handle(codec.toEvent(invocation)).toCompletableFuture()
                .get(10, TimeUnit.SECONDS));   // the same message, delivered twice: the adapter answers differently
        f.deliver(first);
        f.deliver(again);
        f.awaitCompleted(id);

        assertThat(calls).hasValue(2);
        JsonNode record = f.decision(id, "/do/0/classify");
        assertThat(record.path("value").asText()).isEqualTo("approve");
        assertThat(record.path("correlationId").asText()).isEqualTo(invocation.correlationId());
        assertThat(record.path("count").asInt()).isEqualTo(1);
    }

    @Test
    void the_decision_input_reaches_neither_logs_nor_spans() throws Exception {
        InMemorySpanExporter spans = InMemorySpanExporter.create();
        OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build().get("stint-sdd3a2"));
        String marker = "DECISION-INPUT-MARKER-77d1";
        AiFixture f = fixture(CLASSIFY, Map.of("stint.decision.routes.classify-meeting.adapter", "llm"), tracer);

        try (AiNoContentTest.AllLogs logs = new AiNoContentTest.AllLogs()) {
            f.awaitCompleted(f.start(input("text", "a clear meeting " + marker)));
            f.startFailing(input("text", "a broken meeting " + marker));

            assertThat(logs.lines).anySatisfy(line -> assertThat(line).contains("Decision usage of task /do/0/classify"));
            assertThat(String.join("\n", logs.lines)).doesNotContain(marker);
        }
        assertThat(spans.getFinishedSpanItems()).anySatisfy(s -> assertThat(s.getName()).isEqualTo("stint.llm"));
        assertThat(spans.getFinishedSpanItems().toString()).doesNotContain(marker);
    }
}
