package io.stintflow.it;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 3a.5: engines loaded from YAML with a compliance policy, an in-memory audit log and publisher, and workers
 * that are plain functions by routing key (a key without one is never answered). Synthetic data only; the clock moves
 * only when a test says so.
 */
final class PolicyFixture {

    static final Instant T0 = Instant.parse("2026-10-13T12:00:00Z");
    static final String AUTHORIZATION = "https://serverlessworkflow.io/spec/1.0.0/errors/authorization";
    static final DefaultCloudEventCodec CODEC = new DefaultCloudEventCodec();

    final AtomicReference<Instant> now = new AtomicReference<>(T0);
    final WorkflowRegistry registry = new WorkflowRegistry();
    final InMemoryStateStore state = new InMemoryStateStore();
    final InMemoryAuditLog audit = new InMemoryAuditLog();
    final io.stintflow.inmemory.InMemoryCorrectionLog corrections = new io.stintflow.inmemory.InMemoryCorrectionLog();
    final InMemoryTimerService timer = new InMemoryTimerService(now::get);
    /** What the engine put in the early-event inbox (SDD 3a.4 scanner). */
    final InboxSpy inbox = new InboxSpy(state);
    final InMemoryEventPublisher publisher = new InMemoryEventPublisher();
    final List<TaskInvocation> dispatched = new CopyOnWriteArrayList<>();
    final Map<String, Function<TaskInvocation, TaskResult>> workers = new ConcurrentHashMap<>();
    final java.nio.file.Path blobDir;
    final FilesystemBlobStore blobs;
    final WorkflowEngine engine;
    /** Every invoke and result as it crossed the queue: the wire JSON (SDD 3a.5 scanner). */
    final List<String> wire = new CopyOnWriteArrayList<>();
    private final ExecutionTracer tracer;
    private volatile TaskResultHandler results;

    PolicyFixture(String... yamls) throws Exception {
        this(ExecutionTracer.NOOP, yamls);
    }

    PolicyFixture(ExecutionTracer tracer, String... yamls) throws Exception {
        this.tracer = tracer;
        for (String yaml : yamls) {
            registry.register(load(yaml));
        }
        blobDir = Files.createTempDirectory("stint-sdd3a5");
        blobs = new FilesystemBlobStore(blobDir, now::get);
        engine = new WorkflowEngine(registry, new Transport(), inbox, timer, blobs,
                new TreeInterpreter(new JqExpressionEvaluator()), now::get, publisher, null, tracer).auditLog(audit)
                .correctionLog(corrections);
    }

    static WorkflowDefinition load(String yaml) {
        return new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "policy.yaml",
                LoadOptions.STRICT);
    }

    /** A worker as a function of its input, rehydrated past any claim-check pointer like a real worker's. */
    PolicyFixture worker(String routingKey, Function<JsonNode, JsonNode> work) {
        workers.put(routingKey, invocation -> TaskResult.completed(invocation.correlationId(),
                work.apply(ClaimCheck.rehydrate(invocation.input(), blobs).toCompletableFuture().join())));
        return this;
    }

    /**
     * A real worker-side handler (the reference redactor, an LLM step), run by a {@link WorkerRuntime} over the real
     * wire: the invoke carries {@code stintsensitivity}, the result {@code stintprovider}.
     */
    PolicyFixture handler(String routingKey, TaskHandler handler) {
        WorkerRuntime runtime = new WorkerRuntime(new TaskHandlerRegistry().register(routingKey, handler), blobs,
                tracer);
        workers.put(routingKey, invocation -> {
            io.cloudevents.CloudEvent invoke = CODEC.toEvent(invocation);
            wire.add(new String(CeWire.toJson(invoke), StandardCharsets.UTF_8));
            io.cloudevents.CloudEvent result = runtime.handle(invoke).toCompletableFuture().join();
            wire.add(new String(CeWire.toJson(result), StandardCharsets.UTF_8));
            return CODEC.toResult(result);
        });
        return this;
    }

    JsonNode awaitCompleted(String id) throws Exception {
        return engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
    }

    InstanceSnapshot snapshot(String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private final class Transport implements TaskTransport {

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            Function<TaskInvocation, TaskResult> work = workers.get(invocation.routingKey());
            if (work != null) {
                CompletableFuture.runAsync(() -> results.handle(work.apply(invocation)));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            results = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(AdapterCapabilities.DeliveryGuarantee.AT_LEAST_ONCE, false, 256 * 1024,
                    null, false, false);
        }
    }
}
