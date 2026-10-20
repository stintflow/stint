package io.stintflow.it;

import static io.stintflow.it.AiFixture.input;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.worker.decision.DecisionAdapter;
import io.stintflow.worker.decision.DecisionConfig;
import io.stintflow.worker.decision.DecisionHandlers;
import io.stintflow.worker.decision.DecisionPolicy;
import io.stintflow.worker.decision.jev.FakeJevServer;
import io.stintflow.worker.decision.jev.FakeJevServer.Reply;
import io.stintflow.worker.decision.jev.JevDecisionAdapter;
import io.stintflow.worker.decision.jev.JevDecisionAdapter.Server;

/**
 * SDD 3a.2b, end to end: {@code call: decision} answered by the Jev adapter's local profile — a fake server in LitJev's
 * format, never a real LitJev or model — through the worker runtime, with the provenance in the snapshot, the
 * sensitivity policy's view of a local adapter, the no-input rule under {@code catch}, temperature scaling against the
 * threshold, and no input in logs or spans.
 */
class LocalDecisionTest {

    static final String AUTHORIZATION = "https://serverlessworkflow.io/spec/1.0.0/errors/authorization";
    static final Map<String, Double> MOSTLY_ATA = ordered("ata", 0.85, "adr", 0.12, "runbook", 0.03);

    private final FakeJevServer litjev;

    LocalDecisionTest() throws Exception {
        litjev = new FakeJevServer();
    }

    @AfterEach
    void stop() {
        litjev.close();
    }

    private static Map<String, Double> ordered(Object... pairs) {
        Map<String, Double> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], (Double) pairs[i + 1]);
        }
        return map;
    }

    private JevDecisionAdapter local() {
        return JevDecisionAdapter.builder().server(Server.LITJEV).baseUri(litjev.baseUri())
                .serverVersion("2cd915a").readout("content").build();
    }

    private JevDecisionAdapter hosted() {
        return JevDecisionAdapter.builder().baseUri(litjev.baseUri()).apiKey(() -> "test").build();
    }

    /** No remote adapter for this content: the shape of the SDD 3a.5 policy, on the descriptor's {@code remote}. */
    static final DecisionPolicy LOCAL_ONLY = (task, adapter) -> adapter.remote()
            ? Optional.of("remote adapters may not decide this content") : Optional.empty();

    private AiFixture fixture(String tasks, String adapterId, Map<String, String> settings, DecisionPolicy policy,
            ExecutionTracer tracer, DecisionAdapter... adapters) throws Exception {
        Map<String, String> props = new HashMap<>(settings);
        props.put("stint.decision.routes.classify-meeting.adapter", adapterId);
        props.put("stint.decision.routes.classify-meeting.instructions", "Which document does this meeting produce?");
        DecisionHandlers.Builder handlers = DecisionHandlers.builder(DecisionConfig.fromProperties(props))
                .policy(policy);
        for (DecisionAdapter adapter : adapters) {
            handlers.adapter(adapter);
        }
        DecisionHandlers built = handlers.build();
        return new AiFixture(tasks, DecisionAdaptersTest.meetingModel(), b -> b, tracer, built::registerAll);
    }

    private AiFixture classifying(String adapterId, Map<String, String> settings, DecisionAdapter... adapters)
            throws Exception {
        return fixture(DecisionAdaptersTest.CLASSIFY, adapterId, settings, LOCAL_ONLY, ExecutionTracer.NOOP, adapters);
    }

    @Test
    void the_snapshot_shows_the_local_provenance_and_the_policy_lets_only_the_local_adapter_decide() throws Exception {
        litjev.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        JevDecisionAdapter local = local();

        AiFixture byLocal = classifying("litjev", Map.of(), local, hosted());
        String id = byLocal.start(input("text", "a synthetic planning meeting"));
        JsonNode done = byLocal.awaitCompleted(id);
        AiFixture byHosted = classifying("jev", Map.of(), local(), hosted());
        String refused = byHosted.startFailing(input("text", "a synthetic planning meeting"));

        assertThat(done.path("doc").asText()).isEqualTo("ata");
        JsonNode record = byLocal.decision(id, "/do/0/classify");
        assertThat(record.path("confidence").asDouble()).isEqualTo(0.85);   // the highest probability, not the Gini
        assertThat(record.path("provenance").path("adapter").asText()).isEqualTo("litjev");
        assertThat(record.path("provenance").path("provider").asText()).isEqualTo("litjev");
        assertThat(record.path("provenance").path("model").asText()).isEqualTo(FakeJevServer.LITJEV_MODEL);
        assertThat(record.path("provenance").path("version").asText())
                .isEqualTo("jev-adapter/1;server=litjev@2cd915a;readout=content");
        assertThat(local.descriptor().remote()).isFalse();
        assertThat(litjev.authorizations).containsExactly((String) null);   // no key, no header
        assertThat(refused).contains("type=" + AUTHORIZATION).contains("status=403")
                .contains("remote adapters may not decide this content");
        assertThat(litjev.requests).hasSize(1);   // the hosted adapter was never called
    }

    @Test
    void a_decision_without_input_is_a_validation_error_the_workflow_catches() throws Exception {
        String guarded = """
                do:
                  - guarded:
                      try:
                        - classify:
                            call: decision
                            with:
                              task: classify-meeting
                              kind: choice
                              input: '${ .text }'
                              options:
                                - ata: {}
                                - adr: {}
                      catch:
                        errors:
                          with:
                            type: %s
                        then: fallback
                  - fallback:
                      set: '${ {doc: "no input"} }'
                      export: { as: '${ $context + . }' }
                      then: end
                """.formatted(DecisionAdaptersTest.VALIDATION);
        AiFixture f = fixture(guarded, "litjev", Map.of(), LOCAL_ONLY, ExecutionTracer.NOOP, local());

        JsonNode done = f.awaitCompleted(f.start(input("text", "  ")));

        assertThat(done.path("doc").asText()).isEqualTo("no input");
        assertThat(litjev.requests).isEmpty();
    }

    @Test
    void a_configured_temperature_takes_the_same_answer_under_the_threshold() throws Exception {
        litjev.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        litjev.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        AiFixture plain = classifying("litjev", Map.of("stint.decision.adapters.litjev.threshold", "0.7"), local());
        AiFixture scaled = classifying("litjev", Map.of("stint.decision.adapters.litjev.threshold", "0.7",
                "stint.decision.adapters.litjev.calibration.temperature", "2"), local());

        String plainId = plain.start(input("text", "a synthetic meeting"));
        String scaledId = scaled.start(input("text", "a synthetic meeting"));

        assertThat(plain.awaitCompleted(plainId).path("doc").asText()).isEqualTo("ata");
        assertThat(scaled.awaitCompleted(scaledId).path("doc").asText()).isEqualTo("review");
        assertThat(scaled.decision(scaledId, "/do/0/classify").path("confidence").asDouble())
                .isBetween(0.6395, 0.6396);   // 0.85 at t = 2, under 0.7
    }

    @Test
    void the_local_profile_on_a_remote_address_warns_with_the_host_only() {
        try (AiNoContentTest.AllLogs logs = new AiNoContentTest.AllLogs()) {
            JevDecisionAdapter remote = JevDecisionAdapter.builder().server(Server.LITJEV)
                    .baseUri(URI.create("http://10.0.0.5:8000")).allowRemote(true).build();

            assertThat(remote.descriptor().remote()).isTrue();
            assertThat(logs.lines).anySatisfy(line -> assertThat(line).startsWith("WARN")
                    .contains("The local Jev profile 'litjev' sends decision inputs to 10.0.0.5: it is remote"));
        }
    }

    @Test
    void the_decision_input_reaches_neither_logs_nor_spans() throws Exception {
        InMemorySpanExporter spans = InMemorySpanExporter.create();
        OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spans)).build().get("stint-sdd3a2b"));
        String marker = "LOCAL-DECISION-MARKER-5c0e";
        litjev.reply(Reply.litjev("choice", "ata", MOSTLY_ATA));
        litjev.reply(Reply.status(500, Map.of()));
        AiFixture f = fixture(DecisionAdaptersTest.CLASSIFY, "litjev", Map.of(), LOCAL_ONLY, tracer, local());

        try (AiNoContentTest.AllLogs logs = new AiNoContentTest.AllLogs()) {
            f.awaitCompleted(f.start(input("text", "a synthetic meeting " + marker)));
            f.startFailing(input("text", "a failing synthetic meeting " + marker));

            assertThat(logs.lines).anySatisfy(line -> assertThat(line).contains("Decision usage of task /do/0/classify"));
            assertThat(String.join("\n", logs.lines)).doesNotContain(marker);
        }
        assertThat(spans.getFinishedSpanItems()).isNotEmpty();
        assertThat(spans.getFinishedSpanItems().toString()).doesNotContain(marker);
        assertThat(litjev.requests.getFirst().path("state").path("text").asText()).contains(marker);   // it was sent
    }
}
