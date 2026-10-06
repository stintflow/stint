package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.AdapterCapabilities.DeliveryGuarantee;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.wire.Json;

/**
 * SDD 2.8, with definitions written in YAML and run by the engine:
 * <ul>
 *   <li>CA1: a task timeout and a {@code listen} timeout are raised with the DSL 1.0 standard type
 *       {@code .../errors/timeout}, status 408 and the component's pointer as {@code instance}. A {@code catch}
 *       written against the spec catches them.</li>
 *   <li>CA2: a definition that still catches {@code stint://errors/timeout} keeps catching them.</li>
 *   <li>CA3: a legacy worker's {@code stint://errors/runtime} failure without status is caught by the standard
 *       type, and is seen with status 500.</li>
 * </ul>
 * Timeouts fire on a controllable clock; no sleeps.
 */
class StandardErrorTypesTest {

    private static final String TIMEOUT = "https://serverlessworkflow.io/spec/1.0.0/errors/timeout";
    private static final String RUNTIME = "https://serverlessworkflow.io/spec/1.0.0/errors/runtime";

    /** A guarded task whose catch records the error it caught. */
    private static String guardedTask(String name, String caughtType, String route) {
        return """
                document:
                  dsl: '1.0.0'
                  namespace: errors
                  name: %s
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - work:
                            call: remote
                            with:
                              task: %s
                            timeout:
                              after: { minutes: 1 }
                      catch:
                        errors:
                          with:
                            type: '%s'
                        do:
                          - record:
                              set: '${ {type: .type, status: .status, instance: .instance} }'
                  - outcome:
                      set: '${ . }'
                      export:
                        as: '${ . }'
                """.formatted(name, route, caughtType);
    }

    @Test
    void ca1_a_task_timeout_is_a_standard_408_with_its_instance_caught_by_a_spec_written_catch() throws Exception {
        Fixture f = new Fixture(guardedTask("spec-catch", TIMEOUT, "route-silent"));
        String id = f.engine.start(f.def.ref(), Json.obj());

        f.clock.set(f.clock.instant().plus(Duration.ofMinutes(2)));
        f.timer.tick();

        JsonNode caught = f.engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(caught.get("type").asText()).isEqualTo(TIMEOUT);
        assertThat(caught.get("status").asInt()).isEqualTo(408);
        assertThat(caught.get("instance").asText()).isEqualTo("/do/0/guarded");
    }

    @Test
    void ca1_a_listen_timeout_is_a_standard_408_with_its_instance() throws Exception {
        Fixture f = new Fixture("""
                document:
                  dsl: '1.0.0'
                  namespace: errors
                  name: listen-catch
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                        - wait:
                            listen:
                              to:
                                one:
                                  with: { type: io.acme.never.v1 }
                                  correlate:
                                    instance: { from: '${ .data.id }', expect: '${ $workflow.id }' }
                            timeout:
                              after: { minutes: 1 }
                      catch:
                        errors:
                          with:
                            type: 'https://serverlessworkflow.io/spec/1.0.0/errors/timeout'
                        do:
                          - record:
                              set: '${ {type: .type, status: .status, instance: .instance} }'
                  - outcome:
                      set: '${ . }'
                      export:
                        as: '${ . }'
                """);
        String id = f.engine.start(f.def.ref(), Json.obj());

        f.clock.set(f.clock.instant().plus(Duration.ofMinutes(2)));
        f.timer.tick();

        JsonNode caught = f.engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(caught.get("type").asText()).isEqualTo(TIMEOUT);
        assertThat(caught.get("status").asInt()).isEqualTo(408);
        assertThat(caught.get("instance").asText()).isEqualTo("/do/0/guarded");
    }

    @Test
    void ca2_a_definition_catching_the_legacy_stint_type_still_catches_the_timeout() throws Exception {
        Fixture f = new Fixture(guardedTask("legacy-catch", "stint://errors/timeout", "route-silent"));
        String id = f.engine.start(f.def.ref(), Json.obj());

        f.clock.set(f.clock.instant().plus(Duration.ofMinutes(2)));
        f.timer.tick();

        JsonNode caught = f.engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(caught.get("type").asText()).isEqualTo(TIMEOUT);
    }

    @Test
    void ca3_a_legacy_workers_runtime_failure_without_status_is_caught_by_the_standard_type_with_status_500()
            throws Exception {
        Fixture f = new Fixture(guardedTask("legacy-worker", RUNTIME, "route-legacy"));
        String id = f.engine.start(f.def.ref(), Json.obj());
        TaskInvocation work = f.transport.dispatched.poll(5, TimeUnit.SECONDS);

        // As an older worker would report it: the stint://errors/* type and no status.
        f.transport.handler.handle(TaskResult.failed(work.correlationId(),
                new ErrorInfo(URI.create("stint://errors/runtime"), null, "Boom", "legacy failure", null, null)))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        JsonNode caught = f.engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(caught.get("type").asText()).isEqualTo(RUNTIME);
        assertThat(caught.get("status").asInt()).isEqualTo(500);
        assertThat(caught.get("instance").asText()).isEqualTo("/do/0/guarded");
    }

    private static final class Fixture {
        final SendReportMilestoneLocalTest.MutableClock clock =
                new SendReportMilestoneLocalTest.MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
        final InMemoryTimerService timer = new InMemoryTimerService(clock);
        final ManualTransport transport = new ManualTransport();
        final WorkflowDefinition def;
        final WorkflowEngine engine;

        Fixture(String yaml) throws Exception {
            def = new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "errors.yaml",
                    LoadOptions.STRICT);
            WorkflowRegistry registry = new WorkflowRegistry();
            registry.register(def);
            engine = new WorkflowEngine(registry, transport, new InMemoryStateStore(), timer,
                    new FilesystemBlobStore(Files.createTempDirectory("stint-sdd28")),
                    new TreeInterpreter(new JqExpressionEvaluator()), clock, null, null);
        }
    }

    /** Captures dispatches; results are delivered by the test (or never: a silent worker). */
    private static final class ManualTransport implements TaskTransport {
        final LinkedBlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();
        volatile TaskResultHandler handler;

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            this.handler = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(DeliveryGuarantee.AT_LEAST_ONCE, true, Long.MAX_VALUE, null, true, true);
        }
    }
}
