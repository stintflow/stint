package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.DelayRoutingTimerService;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TimerFire;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerFireTooEarly;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.Wait;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 2.9, CA1 and CA4. A timer is armed before the save of the wait it guards (SDD 2.1, RF8); here a barrier holds
 * that save while the test fires the timer, which is exactly the race of the S.2 finding 8i. The fire must not be
 * lost: the engine answers {@link TimerFireTooEarly} (the connector keeps it), and the same fire, delivered again
 * after the save, resumes the instance — for each kind of timer: a task timeout, a retry delay, a listen timeout
 * and a long timer (routed to the long-timer service). CA4: a fire for a finished instance is still a no-op, logged
 * as obsolete.
 */
class EarlyTimerFireTest {

    private static final String TIMEOUT = "https://serverlessworkflow.io/spec/1.0.0/errors/timeout";
    private static final Instant T0 = Instant.parse("2026-10-11T12:00:00Z");

    final List<String> engineLog = new CopyOnWriteArrayList<>();
    final Handler capture = new Handler() {
        @Override
        public void publish(LogRecord record) {
            String message = record.getMessage();
            Object[] params = record.getParameters();
            if (params != null) {
                message = java.text.MessageFormat.format(message, params);
            }
            engineLog.add(record.getLevel() + " " + message);
        }

        @Override
        public void flush() {
        }

        @Override
        public void close() {
        }
    };

    @BeforeEach
    void captureEngineLog() {
        java.util.logging.Logger.getLogger(WorkflowEngine.class.getName()).addHandler(capture);
        org.jboss.logmanager.LogContext.getLogContext().getLogger(WorkflowEngine.class.getName()).addHandler(capture);
    }

    @AfterEach
    void releaseEngineLog() {
        java.util.logging.Logger.getLogger(WorkflowEngine.class.getName()).removeHandler(capture);
        org.jboss.logmanager.LogContext.getLogContext().getLogger(WorkflowEngine.class.getName()).removeHandler(capture);
    }

    @Test
    void task_timeout_fired_before_its_wait_is_saved_still_resumes() throws Exception {
        Fixture f = new Fixture(caughtTimeout("task-timeout", """
                        - work:
                            call: remote
                            with:
                              task: never-answers
                            timeout:
                              after: { seconds: 1 }"""), "timer:");

        String id = f.engine.start(f.def.ref(), Json.obj());
        TimerRequest timer = f.awaitHeldSave();          // armed; its wait's save is held by the barrier

        assertEarly(f.shortTimers.fire(timer));            // the race of 8i: before 2.9 this fire was acked and lost
        f.releaseSave();
        f.shortTimers.fire(timer).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.awaitCompleted(id).get("type").asText()).isEqualTo(TIMEOUT);
        assertThat(engineLog).anySatisfy(l -> assertThat(l).contains("decision=early").contains(timer.timerId()));
    }

    @Test
    void retry_delay_fired_before_its_wait_is_saved_still_resumes() throws Exception {
        Fixture f = new Fixture("""
                document:
                  dsl: '1.0.0'
                  namespace: early
                  name: retry-delay
                  version: '1.0.0'
                do:
                  - render:
                      try:
                        - work:
                            call: remote
                            with:
                              task: flaky
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                        retry:
                          delay: { seconds: 1 }
                          limit:
                            attempt: { count: 3 }
                  - outcome:
                      set: '${ . }'
                      export:
                        as: '${ . }'
                """, "retry:");

        String id = f.engine.start(f.def.ref(), Json.obj());
        TimerRequest retry = f.awaitHeldSave();

        assertEarly(f.shortTimers.fire(retry));
        f.releaseSave();
        f.shortTimers.fire(retry).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.awaitCompleted(id).get("attempt").asInt()).isEqualTo(2);
    }

    @Test
    void listen_timeout_fired_before_its_wait_is_saved_still_resumes() throws Exception {
        Fixture f = new Fixture(caughtTimeout("listen-timeout", listen("{ seconds: 2 }")), "timer:");

        String id = f.engine.start(f.def.ref(), Json.obj());
        TimerRequest timer = f.awaitHeldSave();

        assertEarly(f.shortTimers.fire(timer));
        f.releaseSave();
        f.shortTimers.fire(timer).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.awaitCompleted(id).get("type").asText()).isEqualTo(TIMEOUT);
    }

    @Test
    void long_timer_fired_before_its_wait_is_saved_still_resumes() throws Exception {
        Fixture f = new Fixture(caughtTimeout("long-timer", listen("{ hours: 24 }")), "timer:");

        String id = f.engine.start(f.def.ref(), Json.obj());
        TimerRequest timer = f.awaitHeldSave();
        assertThat(f.longTimers.armed).containsExactly(timer); // beyond 15 min: the Scheduler's side (SDD 2.4)
        assertThat(f.shortTimers.armed).isEmpty();

        assertEarly(f.longTimers.fire(timer));
        f.releaseSave();
        f.longTimers.fire(timer).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.awaitCompleted(id).get("type").asText()).isEqualTo(TIMEOUT);
    }

    @Test
    void fire_for_a_completed_instance_is_logged_obsolete_and_acked() throws Exception {
        Fixture f = new Fixture(caughtTimeout("obsolete", listen("{ seconds: 2 }")), "never-held:");
        String id = f.engine.start(f.def.ref(), Json.obj());
        TimerRequest timer = f.shortTimers.awaitArmed();
        f.shortTimers.fire(timer).toCompletableFuture().get(10, TimeUnit.SECONDS);
        f.awaitCompleted(id);

        // The same fire again (SQS redelivery, or a late Scheduler fire): acked, nothing changes.
        f.shortTimers.fire(timer).toCompletableFuture().get(10, TimeUnit.SECONDS);

        InstanceSnapshot after = f.load(id);
        assertThat(after.status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(engineLog).anySatisfy(l -> assertThat(l).startsWith("INFO")
                .contains("decision=obsolete ").contains(timer.timerId()).contains("instanceStatus=COMPLETED"));
    }

    @Test
    void a_fire_without_its_wait_past_the_window_or_without_armed_at_is_acked_with_a_warning() throws Exception {
        Fixture f = new Fixture(caughtTimeout("after-window", listen("{ seconds: 2 }")), "never-held:");
        String id = f.engine.start(f.def.ref(), Json.obj());
        f.shortTimers.awaitArmed();

        TimerFireHandler handler = f.shortTimers.handler;
        handler.handle(new TimerFire("timer:orphan-1", id, T0.minus(WorkflowEngine.DEFAULT_EARLY_FIRE_WINDOW)))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        handler.handle(new TimerFire("timer:orphan-2", id)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.load(id).status()).isEqualTo(InstanceStatus.WAITING); // untouched: still waits on its listen
        assertThat(engineLog).filteredOn(l -> l.contains("decision=obsolete-after-window"))
                .hasSize(2).allSatisfy(l -> assertThat(l).startsWith("WARN")); // WARN (jboss) or WARNING (JUL)
    }

    private static void assertEarly(CompletionStage<Void> fire) {
        assertThatThrownBy(() -> fire.toCompletableFuture().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .satisfies(e -> assertThat(TimerFireTooEarly.isCause(e)).isTrue());
    }

    private static String listen(String after) {
        return """
                        - wait:
                            listen:
                              to:
                                one:
                                  with: { type: io.acme.never.v1 }
                                  correlate:
                                    instance: { from: '${ .data.id }', expect: '${ $workflow.id }' }
                            timeout:
                              after: %s""".formatted(after);
    }

    private static String caughtTimeout(String name, String body) {
        return """
                document:
                  dsl: '1.0.0'
                  namespace: early
                  name: %s
                  version: '1.0.0'
                do:
                  - guarded:
                      try:
                %s
                      catch:
                        errors:
                          with:
                            type: '%s'
                        do:
                          - record:
                              set: '${ {type: .type} }'
                  - outcome:
                      set: '${ . }'
                      export:
                        as: '${ . }'
                """.formatted(name, body.indent(8).stripTrailing(), TIMEOUT);
    }

    /** An engine whose state store holds the first save that adds a wait with the given prefix. */
    static final class Fixture {
        final MutableClock clock = new MutableClock(T0);
        final CapturingTimers shortTimers = new CapturingTimers(clock, Duration.ofMinutes(15));
        final CapturingTimers longTimers = new CapturingTimers(clock, Duration.ofDays(365));
        final HeldSaveStore state;
        final WorkflowDefinition def;
        final WorkflowEngine engine;

        Fixture(String yaml, String heldWaitPrefix) throws Exception {
            def = new DslLoader().load(new ByteArrayInputStream(yaml.getBytes(StandardCharsets.UTF_8)), "early.yaml",
                    LoadOptions.STRICT);
            DelayRoutingTimerService timers = new DelayRoutingTimerService(shortTimers, longTimers, clock);
            WorkflowRegistry registry = new WorkflowRegistry(timers.maxDelay());
            registry.register(def);
            BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd29"));
            WorkerRuntime worker = new WorkerRuntime(new TaskHandlerRegistry()
                    .register("flaky", ctx -> ctx.attempt() == 1
                            ? CompletableFuture.failedFuture(new TaskFailure(ErrorInfo.StandardType.COMMUNICATION.uri(),
                                    503, "first attempt fails", null))
                            : CompletableFuture.completedFuture(Json.obj().put("attempt", ctx.attempt()))), blobs);
            InMemoryTaskTransport transport = new InMemoryTaskTransport();
            transport.connectWorker(invoke -> "never-answers".equals(invoke.getSubject())
                    ? new CompletableFuture<>() : worker.handle(invoke));
            state = new HeldSaveStore(new InMemoryStateStore(),
                    waits -> waits.stream().anyMatch(w -> w.waitKey().startsWith(heldWaitPrefix)));
            engine = new WorkflowEngine(registry, transport, state, timers, blobs,
                    new TreeInterpreter(new JqExpressionEvaluator()), clock, null, null);
        }

        /** Waits until the held save is reached; the timer armed just before it is the last one armed. */
        TimerRequest awaitHeldSave() throws InterruptedException {
            assertThat(state.reached.await(10, TimeUnit.SECONDS)).as("the held save was reached").isTrue();
            List<TimerRequest> all = new java.util.ArrayList<>(shortTimers.armed);
            all.addAll(longTimers.armed);
            return all.stream().filter(t -> state.heldWaitKeys.get().contains(t.timerId())).findFirst().orElseThrow();
        }

        void releaseSave() throws Exception {
            state.release.complete(null);
            assertThat(state.saved.await(10, TimeUnit.SECONDS)).as("the held save completed").isTrue();
        }

        JsonNode awaitCompleted(String id) throws Exception {
            return engine.awaitCompletion(id, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        }

        InstanceSnapshot load(String id) throws Exception {
            return state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        }
    }

    /** A timer service that only records what is armed; the test delivers each fire itself, with its armedAt. */
    static final class CapturingTimers implements TimerService {
        final List<TimerRequest> armed = new CopyOnWriteArrayList<>();
        final java.util.Map<String, Instant> armedAt = new java.util.concurrent.ConcurrentHashMap<>();
        final CountDownLatch firstArmed = new CountDownLatch(1);
        private final InstantSource clock;
        private final Duration maxDelay;
        volatile TimerFireHandler handler;

        CapturingTimers(InstantSource clock, Duration maxDelay) {
            this.clock = clock;
            this.maxDelay = maxDelay;
        }

        @Override
        public CompletionStage<String> schedule(TimerRequest req) {
            armedAt.put(req.timerId(), clock.instant());
            armed.add(req);
            firstArmed.countDown();
            return CompletableFuture.completedFuture(req.timerId());
        }

        @Override
        public CompletionStage<Void> cancel(String timerId) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void onFire(TimerFireHandler handler) {
            this.handler = handler;
        }

        @Override
        public Duration maxDelay() {
            return maxDelay;
        }

        CompletionStage<Void> fire(TimerRequest req) {
            return handler.handle(new TimerFire(req.timerId(), req.workflowInstanceId(), armedAt.get(req.timerId())));
        }

        TimerRequest awaitArmed() throws InterruptedException {
            assertThat(firstArmed.await(10, TimeUnit.SECONDS)).isTrue();
            return armed.getFirst();
        }
    }

    /** Holds the first save whose new waits match, until {@link #release}: the barrier between arming and saving. */
    static final class HeldSaveStore implements StateStore {
        final StateStore delegate;
        final Predicate<List<Wait>> held;
        final AtomicBoolean holding = new AtomicBoolean(true);
        final AtomicReference<List<String>> heldWaitKeys = new AtomicReference<>(List.of());
        final CountDownLatch reached = new CountDownLatch(1);
        final CountDownLatch saved = new CountDownLatch(1);
        final CompletableFuture<Void> release = new CompletableFuture<>();

        HeldSaveStore(StateStore delegate, Predicate<List<Wait>> held) {
            this.delegate = delegate;
            this.held = held;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, List.of());
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
            if (held.test(addWaits) && holding.compareAndSet(true, false)) {
                heldWaitKeys.set(addWaits.stream().map(Wait::waitKey).toList());
                reached.countDown();
                return release.thenCompose(v -> delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys,
                        addOutbox, removeInbox)).whenComplete((outcome, e) -> saved.countDown());
            }
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox);
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

        @Override
        public CompletionStage<Void> putInbox(InboxEntry entry) {
            return delegate.putInbox(entry);
        }

        @Override
        public CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
            return delegate.findInbox(waitKey);
        }

        @Override
        public CompletionStage<Void> removeInbox(InboxEntry.Key key) {
            return delegate.removeInbox(key);
        }
    }

    static final class MutableClock implements InstantSource {
        private final AtomicReference<Instant> now;

        MutableClock(Instant start) {
            now = new AtomicReference<>(start);
        }

        @Override
        public Instant instant() {
            return now.get();
        }
    }
}
