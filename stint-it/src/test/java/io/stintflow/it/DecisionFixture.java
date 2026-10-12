package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskResultHandler;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.Wait;
import io.stintflow.spi.wire.CloudEventCodec;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import io.stintflow.wire.decision.DecisionKind;
import io.stintflow.wire.decision.DecisionRequest;
import io.stintflow.wire.decision.DecisionResult;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 3a.1: an engine loaded from YAML, with a fake, deterministic decision adapter behind the routing key
 * {@code decide} — no LLM provider, synthetic data only. The adapter decides by rules over the request's
 * {@code input.text}:
 * <ul>
 *   <li>contains {@code aprovado} → {@code approve}; {@code rejeitado} → {@code reject} (confidence 0.9);</li>
 *   <li>contains {@code incerto} → {@code approve}, {@code uncertain: true} (confidence 0.4);</li>
 *   <li>contains {@code quebrado} → an output out of contract that echoes the input as its value;</li>
 *   <li>contains {@code quebra-uma-vez} → out of contract on attempt 1, {@code approve} afterwards;</li>
 *   <li>a boolean is {@code true} when the text contains {@code sim}; a score is the text's length.</li>
 * </ul>
 * Results travel through the real CloudEvents codec; a task routed elsewhere is never answered. With
 * {@link #manual()}, nothing answers until the test calls
 * {@link #answer}; the clock and timers move only when the test says so (never a sleep).
 */
final class DecisionFixture {

    static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");
    static final String VALIDATION = "https://serverlessworkflow.io/spec/1.0.0/errors/validation";
    static final String TIMEOUT = "https://serverlessworkflow.io/spec/1.0.0/errors/timeout";

    final AtomicReference<Instant> now = new AtomicReference<>(T0);
    final InMemoryTimerService timers = new InMemoryTimerService(now::get);
    final List<TimerRequest> armed = new CopyOnWriteArrayList<>();
    final List<JsonNode> requests = new CopyOnWriteArrayList<>();
    final BlockingQueue<TaskInvocation> dispatched = new LinkedBlockingQueue<>();
    final SavedSnapshots state;
    final WorkflowDefinition def;
    final WorkflowEngine engine;
    private final CloudEventCodec codec = new DefaultCloudEventCodec();
    private final WorkerRuntime worker;
    private volatile TaskResultHandler resultHandler;
    private volatile boolean manual;

    DecisionFixture(String tasks) throws Exception {
        this(tasks, new InMemoryStateStore());
    }

    DecisionFixture(String tasks, StateStore store) throws Exception {
        String yaml = """
                document:
                  dsl: '1.0.0'
                  namespace: decision
                  name: classify
                  version: '1.0.0'
                """ + tasks;
        def = new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "decision.yaml",
                LoadOptions.STRICT);
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(def);
        state = new SavedSnapshots(store);
        BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd3a1"));
        worker = new WorkerRuntime(new TaskHandlerRegistry().register("decide", this::decide), blobs);
        TimerService recording = new TimerService() {
            @Override
            public CompletionStage<String> schedule(TimerRequest req) {
                armed.add(req);
                return timers.schedule(req);
            }

            @Override
            public CompletionStage<Void> cancel(String timerId) {
                return timers.cancel(timerId);
            }

            @Override
            public void onFire(TimerFireHandler handler) {
                timers.onFire(handler);
            }

            @Override
            public Duration maxDelay() {
                return timers.maxDelay();
            }
        };
        engine = new WorkflowEngine(registry, new Transport(), state, recording, blobs,
                new TreeInterpreter(new JqExpressionEvaluator()), now::get, null, null);
    }

    /** Nothing answers a dispatch until {@link #answer} is called. */
    DecisionFixture manual() {
        manual = true;
        return this;
    }

    String start(String text) {
        return engine.start(def.ref(), Json.obj().put("text", text));
    }

    /** The final {@code $context} of a completed instance. */
    JsonNode awaitCompleted(String id) throws Exception {
        return engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
    }

    /**
     * Starts an instance that must fail, and returns how the engine reports the failure: its WARNING line, with the
     * error (the only place an uncaught error is reported).
     */
    String startFailing(String text) throws Exception {
        try (LogCapture logs = new LogCapture()) {
            String id = start(text);
            return logs.await(line -> line.contains("Instance " + id + " failed"));
        }
    }

    InstanceSnapshot snapshot(String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    /** The decision record of the task at {@code pointer}. */
    JsonNode decision(String id, String pointer) throws Exception {
        JsonNode decisions = snapshot(id).decisions();
        assertThat(decisions).as("decision records of %s", id).isNotNull();
        return decisions.get(pointer);
    }

    TaskInvocation awaitDispatch() throws InterruptedException {
        TaskInvocation inv = dispatched.poll(10, TimeUnit.SECONDS);
        assertThat(inv).as("a decision was dispatched").isNotNull();
        return inv;
    }

    /** Runs the fake adapter for {@code inv} through the worker runtime; the result is delivered by {@link #deliver}. */
    TaskResult answer(TaskInvocation inv) throws Exception {
        return codec.toResult(worker.handle(codec.toEvent(inv)).toCompletableFuture().get(10, TimeUnit.SECONDS));
    }

    void deliver(TaskResult result) throws Exception {
        resultHandler.handle(result).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /** Waits for the save of a snapshot that matches, then returns it. */
    InstanceSnapshot awaitSaved(Predicate<InstanceSnapshot> match) throws InterruptedException {
        while (true) {
            InstanceSnapshot saved = state.saved.poll(10, TimeUnit.SECONDS);
            assertThat(saved).as("a matching snapshot was saved").isNotNull();
            if (match.test(saved)) {
                return saved;
            }
        }
    }

    /** Moves the clock to the timer that guards {@code waitKey} and fires what is due. */
    void fireTimer(String waitKey) {
        TimerRequest timer = armed.stream().filter(t -> t.timerId().equals(waitKey)).findFirst().orElseThrow();
        now.set(timer.fireAt());
        timers.tick();
    }

    private CompletionStage<JsonNode> decide(TaskContext ctx) {
        requests.add(ctx.input());
        DecisionRequest request = DecisionRequest.fromJson(ctx.input());
        String text = request.input().path("text").asText("");
        DecisionResult.Provenance provenance = new DecisionResult.Provenance("fake-rules", null, null, "1");
        if (text.contains("quebrado") || (text.contains("quebra-uma-vez") && ctx.attempt() == 1)) {
            ObjectNode broken = Json.obj();
            broken.put("value", text);              // echoes the (synthetic) input: must never reach an error or a log
            broken.put("confidence", 2);
            broken.set("provenance", provenance.toJson());
            return CompletableFuture.completedFuture(broken);
        }
        JsonNode value;
        double confidence = 0.9;
        boolean uncertain = false;
        if (request.kind() == DecisionKind.BOOLEAN) {
            value = Json.MAPPER.getNodeFactory().booleanNode(text.contains("sim"));
        } else if (request.kind() == DecisionKind.SCORE) {
            value = Json.MAPPER.getNodeFactory().numberNode(text.length());
        } else if (text.contains("rejeitado")) {
            value = Json.MAPPER.getNodeFactory().textNode("reject");
        } else {
            value = Json.MAPPER.getNodeFactory().textNode("approve");
            if (text.contains("incerto")) {
                confidence = 0.4;
                uncertain = true;
            }
        }
        ObjectNode result = new DecisionResult(value, confidence, uncertain, provenance).toJson();
        result.put("reasoning", "rule-based");      // an extra field: allowed, never recorded
        return CompletableFuture.completedFuture(result);
    }

    /** Hands every dispatch to the fake adapter (or to the test, when manual), through the real codec. */
    private final class Transport implements TaskTransport {

        @Override
        public CompletionStage<Void> dispatch(TaskInvocation invocation) {
            dispatched.add(invocation);
            if (!manual && "decide".equals(invocation.routingKey())) { // any other task is left unanswered
                CompletableFuture.runAsync(() -> {
                    try {
                        deliver(answer(invocation));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onResult(TaskResultHandler handler) {
            resultHandler = handler;
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new AdapterCapabilities(AdapterCapabilities.DeliveryGuarantee.AT_LEAST_ONCE, false, 256 * 1024,
                    null, false, false);
        }
    }

    /** Reports every snapshot after its save succeeds. */
    static final class SavedSnapshots implements StateStore {
        final StateStore delegate;
        final BlockingQueue<InstanceSnapshot> saved = new LinkedBlockingQueue<>();

        SavedSnapshots(StateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox)
                    .thenApply(outcome -> {
                        if (outcome == SaveOutcome.OK) {
                            saved.add(snapshot);
                        }
                        return outcome;
                    });
        }

        @Override
        public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
            return delegate.load(instanceId);
        }

        @Override
        public CompletionStage<Optional<Wait>> findWait(String waitKey) {
            return delegate.findWait(waitKey);
        }

        @Override
        public CompletionStage<Void> delete(String instanceId) {
            return delegate.delete(instanceId);
        }

        @Override
        public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
            return delegate.pendingOutbox(createdAtOrBefore, limit);
        }

        @Override
        public CompletionStage<Void> removeOutbox(String eventId) {
            return delegate.removeOutbox(eventId);
        }
    }

    /** Captures every record logged by the engine and the DSL loader, with its parameters filled in. */
    static final class LogCapture extends Handler implements AutoCloseable {
        private static final List<String> LOGGERS = List.of(WorkflowEngine.class.getName(), DslLoader.class.getName());
        final List<String> lines = new CopyOnWriteArrayList<>();
        private final BlockingQueue<String> arrived = new LinkedBlockingQueue<>();

        LogCapture() {
            for (String name : LOGGERS) {
                java.util.logging.Logger.getLogger(name).addHandler(this);
                java.util.logging.Logger.getLogger(name).setLevel(java.util.logging.Level.ALL);
                org.jboss.logmanager.LogContext.getLogContext().getLogger(name).addHandler(this);
                org.jboss.logmanager.LogContext.getLogContext().getLogger(name).setLevel(java.util.logging.Level.ALL);
            }
            setLevel(java.util.logging.Level.ALL);
        }

        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            Object[] params = record.getParameters();
            if (params != null && message != null) {
                message = java.text.MessageFormat.format(message, params);
            }
            String line = record.getLevel() + " " + message
                    + (record.getThrown() == null ? "" : " " + record.getThrown());
            lines.add(line);
            arrived.add(line);
        }

        /** Waits for a line that matches. */
        String await(Predicate<String> match) throws InterruptedException {
            while (true) {
                String line = arrived.poll(10, TimeUnit.SECONDS);
                assertThat(line).as("a matching log line, among %s", lines).isNotNull();
                if (match.test(line)) {
                    return line;
                }
            }
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
            for (String name : LOGGERS) {
                java.util.logging.Logger.getLogger(name).removeHandler(this);
                org.jboss.logmanager.LogContext.getLogContext().getLogger(name).removeHandler(this);
            }
        }
    }
}
