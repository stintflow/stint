package io.stintflow.core;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.expr.EvalScope;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.ExpressionEvaluator;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.expr.WorkflowDescriptor;
import io.stintflow.core.interpreter.DataFlowSupport;
import io.stintflow.core.interpreter.Emitted;
import io.stintflow.core.interpreter.InterpretResult;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.CallRemoteNode;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.model.OutputContract;
import io.stintflow.core.model.RetryPolicy;
import io.stintflow.core.model.TaskNode;
import io.stintflow.core.model.TryNode;
import io.stintflow.spi.BlobStore;
import io.stintflow.core.policy.Policy;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.ExecutionSpan;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.ModelProvider;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.RetryState;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.TaskTransport;
import io.stintflow.spi.TimerFireTooEarly;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.spi.TraceAttributes;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;

/**
 * The cloud-blind orchestrator.
 * <p>
 * Walks a {@link WorkflowDefinition} tree one local activation at a time (SDD 1.1), checkpointing
 * every transition as one conditional {@link StateStore#save} (SDD 1.2). SDD 1.3 adds real timer
 * firing, {@code timeout.after}, and {@link TryNode} retry/compensation: a task dispatch always
 * registers a {@code task:}+{@code timer:} wait pair; whichever event (result or timeout) wins the
 * conditional consume of both proceeds, the other finds them gone and is a no-op (sec. 8a) — the
 * same {@link StateStore#save}-conflict retry loop from SDD 1.2 handles both concurrency layers.
 * <p>
 * Crucially, this class imports <strong>no cloud SDK</strong> — only the SPI. That invariant is
 * enforced by an ArchUnit test.
 */
public final class WorkflowEngine {

    private static final Logger LOG = System.getLogger(WorkflowEngine.class.getName());

    private static final Duration DEFAULT_TIMEOUT = Duration.ofMinutes(5);

    /** SDD 1.2, sec. 8d: retry budget for a save() that lost a version/wait race (NOT the business-level TryNode retry). */
    static final int MAX_RETRY_ATTEMPTS = 5;
    private static final long BASE_BACKOFF_MILLIS = 10;
    private static final long MAX_BACKOFF_MILLIS = 200;

    /** SDD 1.5, RF4: how often {@link #awaitCompletion} rechecks the StateStore — an engine
     *  implementation detail (not business time), so a real short delay is fine even in tests. */
    private static final Duration AWAIT_POLL_INTERVAL = Duration.ofMillis(20);
    /**
     * SDD 2.9, sec. 8a: how long after its {@code armedAt} a timer fire that finds no wait is taken as early (the
     * save of its wait hasn't landed yet: the connector delivers it again) rather than obsolete.
     */
    public static final Duration DEFAULT_EARLY_FIRE_WINDOW = Duration.ofSeconds(30);
    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);

    private static final ScheduledExecutorService RETRY_SCHEDULER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "stint-engine-retry");
        t.setDaemon(true);
        return t;
    });

    private final WorkflowRegistry registry;
    private final TaskTransport transport;
    private final StateStore state;
    private final TimerService timer;
    private final BlobStore blob;
    private final TreeInterpreter interpreter;
    private final InstantSource clock;
    private volatile Duration earlyFireWindow = DEFAULT_EARLY_FIRE_WINDOW;
    /** SDD 2.2: the domain channel; {@code null} = this engine can't run {@code emit}. */
    private final EventPublisher publisher;
    /** SDD 2.2, sec. 8d: where fact payloads above the publisher's limit go ({@code dataref}); may be {@code null}. */
    private final BlobStore factBlob;
    private final OutboxRelay relay;
    /** SDD 2.5: short spans per activation through the SPI port; {@link ExecutionTracer#NOOP} by default. */
    private final ExecutionTracer tracer;
    /** SDD 3a.5, RF8. */
    private volatile AuditLog audit = AuditLog.NONE;

    /** SDD 2.2, sec. 8c: the outbox shares the activation's transaction (100 items max in DynamoDB). */
    static final int MAX_FACTS_PER_ACTIVATION = 25;

    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob) {
        this(registry, transport, state, timer, blob, new TreeInterpreter(new JqExpressionEvaluator()));
    }

    /** @param interpreter override point for tests (e.g. a lower local node limit for CA5). */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter) {
        this(registry, transport, state, timer, blob, interpreter, InstantSource.system());
    }

    /** @param clock override point for tests (SDD 1.3, sec. 8f) — no sleeps, advance it and call the timer's tick(). */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter, InstantSource clock) {
        this(registry, transport, state, timer, blob, interpreter, clock, null, null);
    }

    /**
     * SDD 2.2: an engine that can run {@code emit}. Facts go to {@code publisher} (the domain channel,
     * never the {@code transport}) through the transactional outbox; {@code factBlob} holds fact payloads
     * too large for the publisher (sec. 8d) — a store of its own, never the internal claim-check one.
     * Call {@code outboxRelay().start(...)} in production so facts left behind by a crash get swept.
     */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter, InstantSource clock,
                          EventPublisher publisher, BlobStore factBlob) {
        this(registry, transport, state, timer, blob, interpreter, clock, publisher, factBlob, ExecutionTracer.NOOP);
    }

    /**
     * SDD 2.5: with a tracer ({@code stint-otel} provides an OpenTelemetry one). Every activation is one short
     * span that ends before the instance suspends; see {@link #resumeWithRetry} for when it is a child of the
     * event that caused it and when it only links to it.
     */
    public WorkflowEngine(WorkflowRegistry registry, TaskTransport transport, StateStore state,
                          TimerService timer, BlobStore blob, TreeInterpreter interpreter, InstantSource clock,
                          EventPublisher publisher, BlobStore factBlob, ExecutionTracer tracer) {
        this.registry = registry;
        this.transport = transport;
        this.state = state;
        this.timer = timer;
        this.blob = blob;
        this.interpreter = interpreter;
        this.clock = clock;
        this.publisher = publisher;
        this.factBlob = factBlob;
        this.relay = publisher == null ? null : new OutboxRelay(state, publisher, clock);
        this.tracer = tracer;
        this.transport.onResult(this::onResult);
        this.timer.onFire(this::onTimerFire);
    }

    /** Start an instance and return its id. The instance runs asynchronously, event by event. */
    public String start(WorkflowRef ref, JsonNode input) {
        return start(ref, input, null);
    }

    /**
     * SDD 3a.5, RF1: starts an instance whose data is at {@code sensitivity}. For a definition with a compliance
     * policy, no level is {@link Sensitivity#RESTRICTED}; without one, the level is only tracked and propagated.
     */
    public String start(WorkflowRef ref, JsonNode input, Sensitivity sensitivity) {
        WorkflowDefinition def = registry.find(ref)
                .orElseThrow(() -> new IllegalArgumentException("Unknown workflow: " + ref.canonical()));
        Sensitivity level = def.policy() != null && sensitivity == null ? Sensitivity.RESTRICTED : sensitivity;
        String instanceId = UUID.randomUUID().toString();
        // SDD 2.5, sec. 8a rule 4: a programmatic start begins a new chain; nothing caused it.
        String chainId = newChainId(instanceId);
        ExecutionSpan span = startActivation(instanceId, chainId, null, ref, null, List.of());
        Instance inst = new Instance(instanceId, input, clock.instant(), chainId,
                new Lineage(chainId, null, null, null).withTrace(span.context()), null, List.of(), null, level);
        CompletionStage<SaveOutcome> committed;
        try (LogContext log = logContext(inst, ref)) {
            // $context starts equal to the workflow's raw input (RF5/SDD 1.1 sec. 8c): there is no prior
            // export.as yet, so the instance's accumulated context and its initial data coincide.
            InterpretResult result = interpreter.run(def, inst.descriptor(ref), WorkflowDefinition.ROOT_POINTER, input, input);
            // A freshly minted UUID has no concurrent writer, so a CONFLICT here is a genuine bug, not a
            // race to retry — no retry loop needed for the very first save.
            committed = commit(inst, def, result, 0, List.of());
        } catch (RuntimeException e) {
            endSpan(span, e);
            throw e;
        }
        committed.whenComplete((outcome, ex) -> endSpan(span, ex))
                .thenAccept(outcome -> {
                    if (outcome != SaveOutcome.OK) {
                        completeExceptionally(instanceId,
                                new IllegalStateException("Unexpected CONFLICT creating new instance " + instanceId));
                    }
                })
                .exceptionally(ex -> {
                    completeExceptionally(instanceId, ex);
                    return null;
                });
        return instanceId;
    }

    /**
     * SDD 2.1, RF2/RF6: starts {@code target} from an inbound domain event, <em>exactly once</em> per
     * (event, definition).
     * <p>
     * The instance id is derived from the definition and the event's {@code source} + {@code id}
     * ({@link #triggeredInstanceId}), so the idempotency key <em>is</em> the instance: creation is the
     * existing conditional {@code expectedVersion=0} save (SDD 1.2) — one write, no window between "key
     * recorded" and "instance recorded" (sec. 8a). A redelivery or a concurrent duplicate loses that
     * save with {@code CONFLICT}, finds the instance and completes normally with the same id (no-op).
     * <p>
     * Unlike {@link #start}, the returned stage covers every effect (save, timer, dispatch), so a
     * {@link io.stintflow.spi.DomainEventSource} can acknowledge the message only after it completes.
     * The workflow input is DSL 1.0's "array containing the events that trigger the execution"
     * ({@code dsl.md}): {@code [<the event in CloudEvents structured JSON>]}. A failure while running
     * the local part of the workflow (e.g. a bad {@code input.from}) persists the instance as
     * {@code FAILED} and still completes normally (sec. 8d) — redelivering it would fail the same way.
     */
    public CompletionStage<String> startFromEvent(WorkflowRef target, CloudEvent trigger) {
        WorkflowDefinition def = registry.find(target).orElse(null);
        if (def == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unknown workflow: " + target.canonical()));
        }
        String instanceId = triggeredInstanceId(target, trigger);
        JsonNode input = Json.MAPPER.createArrayNode().add(Json.read(CeWire.toJson(trigger)));
        // SDD 2.5, sec. 8a: inherit the trigger's chain, or start a deterministic new one (legacy/external/cron);
        // the trigger is the cause. Sec. 8b: another workflow's event starts a new trace, linked to it.
        Lineage triggerLineage = DefaultCloudEventCodec.lineage(trigger);
        String chainId = triggerLineage.chainId() != null ? triggerLineage.chainId() : chainIdOf(trigger);
        ExecutionSpan span = startActivation(instanceId, chainId, trigger.getId(), target, null,
                triggerLineage.hasTrace() ? List.of(triggerLineage) : List.of());
        Instance inst = new Instance(instanceId, input, clock.instant(), chainId,
                new Lineage(chainId, trigger.getId(), null, null).withTrace(span.context()), null, List.of(), null,
                triggeredSensitivity(def, trigger, input, instanceId));
        CompletionStage<SaveOutcome> committed;
        try (LogContext log = logContext(inst, target)) {
            InterpretResult result;
            try {
                result = interpreter.run(def, inst.descriptor(target), WorkflowDefinition.ROOT_POINTER, input, input);
            } catch (RuntimeException e) {
                result = InterpretResult.failed(WorkflowDefinition.ROOT_POINTER, input,
                        "Failed to start from event " + trigger.getId() + ": " + e.getMessage());
            }
            committed = commit(inst, def, result, 0, List.of());
        } catch (RuntimeException e) {
            committed = CompletableFuture.failedFuture(e);
        }
        return committed.whenComplete((outcome, ex) -> endSpan(span, ex)).thenCompose(outcome -> {
            if (outcome == SaveOutcome.OK) {
                return CompletableFuture.completedFuture(instanceId);
            }
            return state.load(instanceId).thenApply(existing -> {
                if (existing.isEmpty()) {
                    throw new IllegalStateException("CONFLICT creating " + instanceId + " but no such instance exists");
                }
                LOG.log(Level.DEBUG, "Duplicate delivery of event {0} from {1}: instance {2} already exists",
                        trigger.getId(), trigger.getSource(), instanceId);
                return instanceId;
            });
        });
    }

    /**
     * SDD 3a.5, RF1: the level of an instance started by {@code trigger} — the event's {@code stintsensitivity}; else,
     * with a policy, its {@code sensitivity.from} over the input; else, with a policy, {@code restricted}. Without a
     * policy, only the event's level (or none) is tracked.
     */
    private Sensitivity triggeredSensitivity(WorkflowDefinition def, CloudEvent trigger, JsonNode input,
            String instanceId) {
        Sensitivity carried = DefaultCloudEventCodec.sensitivity(trigger);
        Policy policy = def.policy();
        if (carried != null || policy == null) {
            return carried;
        }
        if (policy.sensitivityFrom() != null) {
            try {
                JsonNode label = interpreter.evaluator().eval(policy.sensitivityFrom(), input,
                        new EvalScope(input, new WorkflowDescriptor(instanceId, input, clock.instant(), def.ref())));
                if (label != null && label.isTextual()) {
                    return Sensitivity.parseOrRestricted(label.asText());
                }
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "The sensitivity of instance {0} could not be read from its trigger ({1}): "
                        + "restricted", instanceId, e.getClass().getName());
            }
        }
        return Sensitivity.RESTRICTED;
    }

    /**
     * SDD 2.1, sec. 8a: {@code "evt-" + UUIDv3(definition, source, id)}. Name-based only — a stable
     * name, not a security property. The definition (including its version) is part of the key
     * because one event may start several definitions (sec. 8b), each an independent execution.
     */
    public static String triggeredInstanceId(WorkflowRef target, CloudEvent trigger) {
        String name = target.canonical() + '\n' + trigger.getSource() + '\n' + trigger.getId();
        return "evt-" + UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    /** SDD 2.5, sec. 8a rule 2: an event without a chain starts one, the same for every redelivery/fan-out target. */
    static String chainIdOf(CloudEvent trigger) {
        String name = "chain|" + trigger.getSource() + '|' + trigger.getId();
        return "chn-" + UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    /** SDD 2.5, sec. 8a rule 4 (and the fallback for snapshots written before SDD 2.5). */
    static String newChainId(String instanceId) {
        String name = "chain|" + instanceId;
        return "chn-" + UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8));
    }

    private ExecutionSpan startActivation(String instanceId, String chainId, String causationId, WorkflowRef ref,
            Lineage parent, List<Lineage> links) {
        return tracer.start("stint.activation", identifiers(instanceId, chainId, causationId, ref), parent, links);
    }

    /** SDD 2.5, decision 7: identifiers only — never input, output or payloads. */
    private static Map<String, String> identifiers(String instanceId, String chainId, String causationId, WorkflowRef ref) {
        Map<String, String> ids = new HashMap<>();
        ids.put(TraceAttributes.INSTANCE_ID, instanceId);
        ids.put(TraceAttributes.CHAIN_ID, chainId);
        if (causationId != null) {
            ids.put(TraceAttributes.CAUSATION_ID, causationId);
        }
        ids.put(TraceAttributes.WORKFLOW, ref.canonical());
        return ids;
    }

    private static LogContext logContext(Instance inst, WorkflowRef ref) {
        return LogContext.open(identifiers(inst.id(), inst.chainId(), inst.activation().causationId(), ref), inst.trace());
    }

    private static void endSpan(ExecutionSpan span, Throwable failure) {
        if (failure != null) {
            span.recordFailure(failure);
        }
        span.close();
    }

    /** Convenience for tests/sync callers: start and await completion with the default timeout. */
    public CompletionStage<JsonNode> startAndWait(WorkflowRef ref, JsonNode input) {
        return awaitCompletion(start(ref, input), DEFAULT_AWAIT_TIMEOUT);
    }

    /**
     * SDD 1.5, RF4: waits for {@code instanceId} to reach a terminal {@link InstanceStatus} by
     * polling the {@link StateStore} — works across JVMs/engine instances sharing the same store,
     * unlike the old in-memory-only shortcut this replaces. Completes with the instance's final
     * {@code $context} on {@code COMPLETED}; completes exceptionally on {@code FAILED} or timeout
     * (never with an empty result — a silent empty result would hide a real failure).
     * <p>
     * Known limitation (sec. 8d): {@link InstanceSnapshot} does not persist <em>why</em> an instance
     * failed, only that it did — the exceptional completion on {@code FAILED} can report the
     * instance id and its last position, not the original {@link ErrorInfo}. Not intended for
     * high-volume production use (poll cost is linear in the number of concurrent waiters); fine for
     * tests and one-off synchronous calls.
     */
    public CompletionStage<JsonNode> awaitCompletion(String instanceId, Duration timeout) {
        CompletableFuture<JsonNode> result = new CompletableFuture<>();
        pollForCompletion(instanceId, clock.instant().plus(timeout), result);
        return result;
    }

    private void pollForCompletion(String instanceId, Instant deadline, CompletableFuture<JsonNode> result) {
        state.load(instanceId).whenComplete((optSnap, ex) -> {
            if (ex != null) {
                result.completeExceptionally(ex);
                return;
            }
            if (optSnap.isPresent()) {
                InstanceSnapshot snap = optSnap.get();
                if (snap.status() == InstanceStatus.COMPLETED) {
                    result.complete(snap.context());
                    return;
                }
                if (snap.status() == InstanceStatus.FAILED) {
                    result.completeExceptionally(new RuntimeException(
                            "Instance " + instanceId + " failed (last position: " + snap.position() + ")"));
                    return;
                }
            }
            if (clock.instant().isAfter(deadline)) {
                result.completeExceptionally(
                        new TimeoutException("Instance " + instanceId + " did not complete within the timeout"));
                return;
            }
            RETRY_SCHEDULER.schedule(() -> pollForCompletion(instanceId, deadline, result),
                    AWAIT_POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        });
    }

    // --- events that can resume a suspended instance (SDD 1.3, sec. 8a: handled uniformly) --------

    private sealed interface ResumeEvent {
        record TaskCompleted(TaskResult result) implements ResumeEvent {
        }

        record TimerElapsed(String timerId) implements ResumeEvent {
        }

        /** SDD 2.3: a domain event for a {@code listen}; {@code fromInbox} = it had been kept as an early event. */
        record EventArrived(CloudEvent event, boolean fromInbox) implements ResumeEvent {
        }
    }

    private CompletionStage<Void> onResult(TaskResult result) {
        String taskKey = WaitKeys.task(result.correlationId());
        String timerKey = WaitKeys.timer(result.correlationId());
        return timer.cancel(timerKey).thenCompose(v ->
                resumeWithRetry(taskKey, new ResumeEvent.TaskCompleted(result), 1));
    }

    /**
     * SDD 2.9, sec. 8a: a timer is armed before the save of the wait it guards (SDD 2.1, RF8), so a short one can
     * fire before that wait exists. A fire whose wait is there resumes as always. One that finds no wait is:
     * <ul>
     *   <li><b>obsolete</b> when its instance is terminal: nothing can wait on it any more — acked (INFO);</li>
     *   <li><b>early</b> when it was armed less than {@link #earlyFireWindow} ago: failed with
     *       {@link TimerFireTooEarly}, so the connector keeps it and delivers it again (INFO);</li>
     *   <li><b>obsolete after the window</b> otherwise (or without {@code armedAt}, the pre-2.9 format): an orphan of
     *       a save that lost, a wait already consumed, or a loss the window didn't cover — acked (WARN).</li>
     * </ul>
     * A missing instance is not proof: the save that creates it may be the one still on its way.
     */
    private CompletionStage<Void> onTimerFire(io.stintflow.spi.TimerFire fire) {
        return state.findWait(fire.timerId()).thenCompose(wait -> wait.isPresent()
                ? resumeWithRetry(fire.timerId(), new ResumeEvent.TimerElapsed(fire.timerId()), 1)
                : unmatchedTimerFire(fire));
    }

    private CompletionStage<Void> unmatchedTimerFire(io.stintflow.spi.TimerFire fire) {
        return state.load(fire.workflowInstanceId()).thenCompose(snap -> {
            String status = snap.map(s -> s.status().name()).orElse("ABSENT");
            Duration age = fire.armedAt() == null ? null : Duration.between(fire.armedAt(), clock.instant());
            if (snap.isPresent() && isTerminal(snap.get().status())) {
                logTimerFire(Level.INFO, "obsolete", fire, age, status);
                return done();
            }
            if (age != null && age.compareTo(earlyFireWindow) < 0) {
                logTimerFire(Level.INFO, "early", fire, age, status);
                return CompletableFuture.failedFuture(new TimerFireTooEarly(fire.timerId(), "Timer " + fire.timerId()
                        + " fired " + age + " after it was armed, before the save of its wait; deliver it again"));
            }
            logTimerFire(Level.WARNING, "obsolete-after-window", fire, age, status);
            return done();
        });
    }

    private static boolean isTerminal(InstanceStatus status) {
        return status == InstanceStatus.COMPLETED || status == InstanceStatus.FAILED || status == InstanceStatus.ABORTED;
    }

    /** SDD 2.9, sec. 8d: one line per timer fire that finds no wait, with the decision and what it was based on. */
    private static void logTimerFire(Level level, String decision, io.stintflow.spi.TimerFire fire, Duration age,
            String instanceStatus) {
        LOG.log(level, "Timer fire without its wait: decision={0} timerId={1} instanceId={2} armedAt={3} age={4} "
                + "instanceStatus={5}", decision, fire.timerId(), fire.workflowInstanceId(), fire.armedAt(), age,
                instanceStatus);
    }

    /**
     * SDD 2.9, sec. 8a: sets how long after arming a fire without its wait is taken as early. Must cover the time
     * between arming a timer and saving its wait; the SQS connector's redeliveries (2+4+8+16 s) fit the default.
     */
    public WorkflowEngine earlyFireWindow(Duration window) {
        if (window == null || window.isNegative()) {
            throw new IllegalArgumentException("early-fire window must be zero or positive: " + window);
        }
        this.earlyFireWindow = window;
        return this;
    }

    // --- resume path: findWait -> load -> apply -> save, retrying on CONFLICT ----------------------

    private CompletionStage<Void> resumeWithRetry(String waitKey, ResumeEvent event, int attempt) {
        return state.findWait(waitKey).thenCompose(optWait -> {
            if (optWait.isEmpty()) {
                return done(); // unknown, already consumed by a winner, or a stale/obsolete duplicate (sec. 8a)
            }
            String instanceId = optWait.get().instanceId();
            return state.load(instanceId).thenCompose(optSnap -> {
                if (optSnap.isEmpty()) {
                    return done(); // stale: instance is gone
                }
                // Note: we do NOT also require waitKey == snap.waitingKey() here. A dispatch inside a
                // TryNode registers BOTH task:<id> and timer:<id> together (sec. 8a), but a snapshot
                // only records one "primary" waitingKey — so the sibling's own firing would otherwise
                // look "stale" by that check alone. state.findWait(waitKey) already proved this exact
                // wait is still unconsumed, and the save() below is still version+wait conditional, so
                // atomicity doesn't depend on this extra check.
                InstanceSnapshot snap = optSnap.get();
                // SDD 2.5: the activation's lineage — the instance's own chain, the event as its cause, and
                // (sec. 8b) a child span of the worker's span for a prompt result, or a link to the span that
                // suspended when resuming after a wait (timer, or a result that carries no trace).
                String chainId = snap.chainId() != null ? snap.chainId() : newChainId(instanceId);
                Lineage suspended = snap.suspendedAt();
                Lineage parent = null;
                List<Lineage> links = suspended != null && suspended.hasTrace() ? List.of(suspended) : List.of();
                String causationId;
                if (event instanceof ResumeEvent.TaskCompleted tc) {
                    causationId = tc.result().correlationId();
                    if (tc.result().lineage().hasTrace()) {
                        parent = tc.result().lineage();
                        links = List.of();
                    }
                } else if (event instanceof ResumeEvent.EventArrived arrived) {
                    // SDD 2.3, RF6: the event is the cause; like any event from elsewhere it starts a new trace
                    // linked to it (and to the activation that suspended), never a child of it.
                    causationId = arrived.event().getId();
                    Lineage eventLineage = DefaultCloudEventCodec.lineage(arrived.event());
                    if (eventLineage.hasTrace()) {
                        List<Lineage> withEvent = new ArrayList<>(links);
                        withEvent.add(eventLineage);
                        links = List.copyOf(withEvent);
                    }
                } else {
                    causationId = ((ResumeEvent.TimerElapsed) event).timerId(); // decision 6: a timer fire, not a wire event
                }
                ExecutionSpan span = startActivation(instanceId, chainId, causationId, snap.definition(), parent, links);
                Lineage activation = new Lineage(chainId, causationId, null, null).withTrace(span.context());
                CompletionStage<SaveOutcome> advanced;
                try (LogContext log = LogContext.open(identifiers(instanceId, chainId, causationId, snap.definition()),
                        activation)) {
                    advanced = advance(snap, event, waitKey, activation);
                } catch (RuntimeException e) {
                    advanced = CompletableFuture.failedFuture(e);
                }
                return advanced
                        .whenComplete((outcome, ex) -> endSpan(span, ex))
                        .thenCompose(outcome -> afterAttempt(waitKey, event, attempt, outcome))
                        .exceptionally(ex -> {
                            failExistingInstance(instanceId, ex);
                            return null;
                        });
            });
        });
    }

    private CompletionStage<Void> afterAttempt(String waitKey, ResumeEvent event, int attempt, SaveOutcome outcome) {
        if (outcome == SaveOutcome.OK) {
            return done();
        }
        if (attempt >= MAX_RETRY_ATTEMPTS) {
            return handleExhaustedRetries(waitKey);
        }
        return delay(backoffMillis(attempt)).thenCompose(v -> resumeWithRetry(waitKey, event, attempt + 1));
    }

    /** Sec. 8d: never ambiguous — either someone else legitimately won, or this is a definitive FAILED. */
    private CompletionStage<Void> handleExhaustedRetries(String waitKey) {
        return state.findWait(waitKey).thenCompose(recheck -> {
            if (recheck.isEmpty()) {
                return done(); // another attempt already claimed it — not an error
            }
            return state.load(recheck.get().instanceId()).thenCompose(optSnap -> {
                if (optSnap.isEmpty()) {
                    return done();
                }
                InstanceSnapshot snap = optSnap.get();
                InstanceSnapshot failedSnap = new InstanceSnapshot(snap.instanceId(), snap.definition(),
                        snap.position(), null, snap.context(), InstanceStatus.FAILED, snap.version() + 1, null,
                        clock.instant(), snap.input(), snap.startedAt(), snap.chainId(), snap.suspendedAt(),
                        null, snap.decisions(), snap.sensitivity());
                return state.save(failedSnap, snap.version(), List.of(), List.of(waitKey))
                        .thenAccept(outcome -> completeExceptionally(snap.instanceId(), new RuntimeException(
                                "State store write conflict persisted after " + MAX_RETRY_ATTEMPTS
                                        + " attempts on wait key " + waitKey)));
            });
        });
    }

    /** Applies a task result or timer fire to the snapshot it was found waiting on; returns the save outcome. */
    private CompletionStage<SaveOutcome> advance(InstanceSnapshot snap, ResumeEvent event, String waitKey,
            Lineage activation) {
        WorkflowDefinition def = registry.find(snap.definition()).orElseThrow();
        List<InboxEntry.Key> fromInbox = event instanceof ResumeEvent.EventArrived arrived && arrived.fromInbox()
                ? List.of(new InboxEntry.Key(waitKey, arrived.event().getId())) : List.of();
        Instance loaded = Instance.of(snap, activation, fromInbox);
        // SDD 3a.5, RF2: an event's level can only raise the instance's, never lower it
        Instance inst = event instanceof ResumeEvent.EventArrived arrived
                ? loaded.withSensitivity(Sensitivity.max(loaded.sensitivity(),
                        DefaultCloudEventCodec.sensitivity(arrived.event())))
                : loaded;
        String instanceId = inst.id();
        WorkflowDescriptor wf = inst.descriptor(snap.definition());
        TaskNode node = def.at(snap.position());

        ObjectNode activeListen = ListenState.active(snap.listenState());
        if (activeListen != null) {
            return advanceListen(inst, def, snap, node, activeListen, event, waitKey);
        }
        if (event instanceof ResumeEvent.EventArrived) {
            return CompletableFuture.completedFuture(SaveOutcome.OK); // no listen active: stale, nothing to do
        }

        if (WaitKeys.isRetry(waitKey)) {
            if (!(node instanceof TryNode tryNode)) {
                return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), List.of(waitKey),
                        () -> completeExceptionally(instanceId, new IllegalStateException(
                                "Instance " + instanceId + " has a retry wait but is not at a TryNode: " + snap.position())));
            }
            return redispatchRetry(inst, def, tryNode, snap, waitKey);
        }

        String correlationId = WaitKeys.rawId(waitKey);
        List<String> consumeKeys = List.of(WaitKeys.task(correlationId), WaitKeys.timer(correlationId));
        boolean succeeded = event instanceof ResumeEvent.TaskCompleted tc
                && tc.result().status() == TaskResult.Status.COMPLETED;

        if (succeeded) {
            TaskResult result = ((ResumeEvent.TaskCompleted) event).result();
            if (!(node instanceof CallRemoteNode) && !(node instanceof TryNode)) {
                return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), consumeKeys,
                        () -> completeExceptionally(instanceId, new IllegalStateException(
                                "Instance " + instanceId + " was waiting at an unexpected node: " + snap.position())));
            }
            CallRemoteNode call = node instanceof TryNode tryNode ? tryNode.body() : (CallRemoteNode) node;
            return ClaimCheck.rehydrate(result.output(), blob).thenCompose(rawOutput -> {
                OutputContract contract = call.outputContract();
                Instance applied = inst;
                if (contract == null) {
                    auditAiCall(inst, call, null, result.provider(), "answered");
                } else {
                    // SDD 3a.1, RF4: an output out of contract is a failure of the task, raised at it.
                    List<String> violations = contract.violations(rawOutput);
                    if (!violations.isEmpty()) {
                        ErrorInfo invalid = new ErrorInfo(ErrorInfo.TYPE_VALIDATION,
                                ErrorInfo.StandardType.VALIDATION.status(), contract.title(),
                                String.join("; ", violations), call.pointer(), null);
                        return failTask(inst, def, node, snap, invalid, consumeKeys);
                    }
                    applied = inst.withDecisions(recordOutput(inst.decisions(), call, contract, rawOutput, snap,
                            correlationId));
                    auditAiCall(inst, call, contract.auditKind() == null ? contract.record(rawOutput) : null,
                            result.provider(), "answered");
                    // SDD 3a.5, RF5: a verified redaction lowers the level — never above where it is
                    Optional<Sensitivity> after = contract.sensitivityAfter(rawOutput);
                    if (after.isPresent() && applied.sensitivity() != null
                            && applied.sensitivity().compareTo(after.get()) > 0) {
                        applied = applied.withSensitivity(after.get());
                    }
                    if (contract.auditKind() != null) {
                        auditOutput(inst, applied, call, contract, rawOutput);
                    }
                }
                InterpretResult next = node instanceof TryNode tryNode
                        ? interpreter.resumeTry(def, wf, tryNode, rawOutput, snap.context())
                        : interpreter.resume(def, wf, snap.position(), call, rawOutput, snap.context());
                return commit(applied, def, next, snap.version(), consumeKeys);
            });
        }

        if (event instanceof ResumeEvent.TaskCompleted tc) {
            auditFailure(inst, node instanceof TryNode tryNode ? tryNode.body()
                    : node instanceof CallRemoteNode call ? call : null, tc.result());
        }
        // SDD 2.8: the standard type, its status, and the failing component's pointer as instance.
        ErrorInfo error = (event instanceof ResumeEvent.TaskCompleted tc && tc.result().error() != null
                ? tc.result().error()
                : ErrorInfo.timeout("Task at " + snap.position() + " did not respond within its timeout"))
                .raisedAt(snap.position());
        return failTask(inst, def, node, snap, error, consumeKeys);
    }

    /** A failed task: caught by its try, or the instance fails. */
    private CompletionStage<SaveOutcome> failTask(Instance inst, WorkflowDefinition def, TaskNode node,
            InstanceSnapshot snap, ErrorInfo error, List<String> consumeKeys) {
        if (node instanceof TryNode tryNode) {
            return handleTryFailure(inst, def, tryNode, snap, error, consumeKeys);
        }
        return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                InstanceStatus.FAILED, snap.version(), consumeKeys,
                () -> completeExceptionally(inst.id(), new RuntimeException("Task failed: " + error)));
    }

    /**
     * SDD 3a.1, RF5/RF6: {@code records} with the record of {@code call}'s output, by its pointer — the attempt that
     * produced it, its correlation id, when it was applied, how many times this task has decided in this instance,
     * and the type and status of the previous attempt's error, if any. Only the first result of an attempt is ever
     * applied (a duplicate or late one finds its wait consumed), so this is written once per attempt that succeeds.
     * Logged with the record only, which never carries the task's input (RF7).
     */
    private JsonNode recordOutput(JsonNode records, CallRemoteNode call, OutputContract contract, JsonNode output,
            InstanceSnapshot snap, String correlationId) {
        JsonNode record = contract.record(output);
        if (record == null) {
            return records;
        }
        ObjectNode entry = record.isObject() ? ((ObjectNode) record).deepCopy() : Json.obj().set("record", record);
        RetryState retry = snap.retryState();
        int attempt = retry != null ? retry.attempt() : 1;
        entry.put("attempt", attempt);
        entry.put("correlationId", correlationId);
        entry.put("decidedAt", clock.instant().toString());
        ObjectNode updated = records != null && records.isObject() ? ((ObjectNode) records).deepCopy() : Json.obj();
        entry.put("count", updated.path(call.pointer()).path("count").asInt(0) + 1);
        if (retry != null && retry.lastError() != null) {
            ObjectNode previous = entry.putObject("previousError");
            previous.put("type", retry.lastError().type() == null ? null : retry.lastError().type().toString());
            if (retry.lastError().status() != null) {
                previous.put("status", retry.lastError().status());
            }
        }
        updated.set(call.pointer(), entry);
        LOG.log(Level.INFO, "Recorded the output of {0} (attempt {1}): {2}", call.pointer(), attempt, record);
        return updated;
    }

    // --- TryNode: catch / retry / compensation (SDD 1.3) -------------------------------------------

    private CompletionStage<SaveOutcome> handleTryFailure(Instance inst, WorkflowDefinition def, TryNode tryNode,
            InstanceSnapshot snap, ErrorInfo error, List<String> consumeKeys) {
        String instanceId = inst.id();
        WorkflowDescriptor wf = inst.descriptor(def.ref());
        TryNode.Catch catchClause = tryNode.catchClause();
        ExpressionEvaluator evaluator = interpreter.evaluator();
        JsonNode errorJson = errorToJsonForEval(error);

        if (!matchesCatch(catchClause, errorJson, snap.context(), evaluator, wf)) {
            return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), consumeKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException("Uncaught task failure: " + error)));
        }

        RetryState prior = snap.retryState();
        int attempt = prior != null ? prior.attempt() : 1;
        Instant firstAttemptAt = prior != null ? prior.firstAttemptAt() : clock.instant();

        if (catchClause.retry() != null) {
            RetryPolicy retry = catchClause.retry();
            boolean underAttempts = attempt < retry.maxAttempts();
            boolean underDuration = retry.maxDuration() == null
                    || Duration.between(firstAttemptAt, clock.instant()).compareTo(retry.maxDuration()) < 0;

            if (underAttempts && underDuration) {
                Duration delay = withMinimumDelay(applyJitter(retry.delayFor(attempt + 1), retry.jitterRatio()),
                        error.retryAfter());
                if (delay.compareTo(timer.maxDelay()) <= 0) {
                    return scheduleRetryDelay(inst, snap, tryNode, attempt, firstAttemptAt, error, delay, consumeKeys);
                }
                return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                        InstanceStatus.FAILED, snap.version(), consumeKeys,
                        () -> completeExceptionally(instanceId, new RuntimeException(
                                "Retry delay " + delay + " exceeds the timer's max delay " + timer.maxDelay())));
            }
            return commitTerminal(inst, snap.definition(), snap.position(), snap.context(),
                    InstanceStatus.FAILED, snap.version(), consumeKeys,
                    () -> completeExceptionally(instanceId, new RuntimeException(
                            "Retry limit exhausted after " + attempt + " attempt(s): " + error)));
        }

        // caught, no retry policy: optional local compensation, then continue via catch.then
        JsonNode resultData = catchClause.compensation() == null ? errorJson
                : evaluator.eval(catchClause.compensation(), errorJson, new EvalScope(snap.context(), wf));
        InterpretResult next = interpreter.resumeFromCatch(def, wf, tryNode, resultData, snap.context());
        return commit(inst, def, next, snap.version(), consumeKeys);
    }

    private CompletionStage<SaveOutcome> scheduleRetryDelay(Instance inst, InstanceSnapshot snap, TryNode tryNode,
            int attempt, Instant firstAttemptAt, ErrorInfo error, Duration delay, List<String> consumeKeys) {
        String instanceId = inst.id();
        String retryId = UUID.randomUUID().toString();
        String retryKey = WaitKeys.retry(retryId);
        RetryState newRetryState = new RetryState(tryNode.pointer(), attempt, firstAttemptAt, error, null,
                snap.retryState() != null ? snap.retryState().dispatchInput() : null);
        InstanceSnapshot next = new InstanceSnapshot(instanceId, snap.definition(), snap.position(), retryKey,
                snap.context(), InstanceStatus.WAITING, snap.version() + 1, newRetryState, clock.instant(),
                inst.input(), inst.startedAt(), inst.chainId(), inst.trace(), inst.listenState(), inst.decisions(), inst.sensitivity());

        return armThenSave(new TimerRequest(retryKey, instanceId, null, clock.instant().plus(delay)),
                () -> state.save(next, snap.version(),
                        List.of(new Wait(retryKey, instanceId, snap.position(), clock.instant())), consumeKeys),
                () -> done());
    }

    private CompletionStage<SaveOutcome> redispatchRetry(Instance inst, WorkflowDefinition def, TryNode tryNode,
            InstanceSnapshot snap, String retryWaitKey) {
        String instanceId = inst.id();
        RetryState prior = snap.retryState();
        int nextAttempt = (prior != null ? prior.attempt() : 1) + 1;
        Instant firstAttemptAt = prior != null ? prior.firstAttemptAt() : clock.instant();
        String correlationId = UUID.randomUUID().toString();
        String taskKey = WaitKeys.task(correlationId);
        String timerKey = WaitKeys.timer(correlationId);
        CallRemoteNode body = tryNode.body();
        Optional<String> refused = policyRefusal(inst, def, body);
        if (refused.isPresent()) {
            return refuseDispatch(inst, def, body, snap.position(), snap.context(), snap.version(),
                    List.of(retryWaitKey), new RetryState(tryNode.pointer(), nextAttempt, firstAttemptAt,
                            prior != null ? prior.lastError() : null, null,
                            prior != null ? prior.dispatchInput() : null), List.of(), refused.get());
        }

        // SDD 2.9, RF4: a retry dispatches again exactly what attempt 1 dispatched (recorded in its wire form, so a
        // claim-checked payload is reused as is). Snapshots from before SDD 2.9 have no record: recompute it, from
        // $context as before, but through the body's input.from and with.input.
        JsonNode recorded = prior != null ? prior.dispatchInput() : null;
        CompletionStage<JsonNode> wire;
        if (recorded != null) {
            wire = CompletableFuture.completedFuture(recorded);
        } else {
            WorkflowDescriptor wf = inst.descriptor(def.ref());
            JsonNode tryInput = DataFlowSupport.applyExpr(interpreter.evaluator(), tryNode.dataFlow().inputFrom(),
                    snap.context(), snap.context(), wf);
            // Keyed per attempt: with deterministic event-started ids (SDD 2.1, 8a) two racing duplicates must
            // never overwrite each other's claim-checked payload.
            wire = ClaimCheck.offload(interpreter.tryBodyArgument(tryNode, tryInput, snap.context(), wf),
                    transport.capabilities(), blob, instanceId + "/" + body.name() + "/" + correlationId);
        }
        return wire
                .thenCompose(wireInput -> {
                    RetryState newRetryState = new RetryState(tryNode.pointer(), nextAttempt, firstAttemptAt,
                            prior != null ? prior.lastError() : null, correlationId, wireInput);
                    InstanceSnapshot next = new InstanceSnapshot(instanceId, snap.definition(), snap.position(),
                            taskKey, snap.context(), InstanceStatus.WAITING, snap.version() + 1, newRetryState,
                            clock.instant(), inst.input(), inst.startedAt(), inst.chainId(), inst.trace(),
                            inst.listenState(), inst.decisions(), inst.sensitivity());
                    Duration timeout = effectiveTimeout(body);
                    // SDD 3a.3c: the attempt's deadline is the instant its timeout timer fires
                    Instant deadline = clock.instant().plus(timeout);
                    TaskInvocation inv = new TaskInvocation(instanceId, snap.position(), correlationId,
                            snap.definition(), body.routingKey(), wireInput, nextAttempt, inst.activation(), deadline,
                            inst.sensitivity());

                    return armThenSave(new TimerRequest(timerKey, instanceId, correlationId, deadline),
                            () -> state.save(next, snap.version(),
                                    List.of(new Wait(taskKey, instanceId, snap.position(), clock.instant()),
                                            new Wait(timerKey, instanceId, snap.position(), clock.instant())),
                                    List.of(retryWaitKey)),
                            () -> transport.dispatch(inv));
                });
    }

    private boolean matchesCatch(TryNode.Catch catchClause, JsonNode errorJson, JsonNode context,
            ExpressionEvaluator evaluator, WorkflowDescriptor wf) {
        if (catchClause.errorFilter() != null && !isTruthy(evaluator.eval(catchClause.errorFilter(), errorJson,
                new EvalScope(context, wf)))) {
            return false;
        }
        if (catchClause.when() != null && !isTruthy(evaluator.eval(catchClause.when(), errorJson,
                new EvalScope(context, wf)))) {
            return false;
        }
        return catchClause.exceptWhen() == null
                || !isTruthy(evaluator.eval(catchClause.exceptWhen(), errorJson, new EvalScope(context, wf)));
    }

    private static boolean isTruthy(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return false;
        }
        return !node.isBoolean() || node.booleanValue();
    }

    private static JsonNode errorToJsonForEval(ErrorInfo error) {
        var node = Json.obj();
        // SDD 2.8: normalized, so a catch on a legacy stint://errors/* type and one on the standard type both match.
        node.put("type", error.type() == null ? null : ErrorInfo.normalize(error.type()).toString());
        if (error.status() != null) {
            node.put("status", error.status());
        }
        if (error.title() != null) {
            node.put("title", error.title());
        }
        if (error.detail() != null) {
            node.put("detail", error.detail());
        }
        if (error.instance() != null) {
            node.put("instance", error.instance());
        }
        if (error.retryAfter() != null) {
            node.put("retryAfter", error.retryAfter().toString());
        }
        return node;
    }

    private static Duration applyJitter(Duration base, double jitterRatio) {
        if (jitterRatio <= 0) {
            return base;
        }
        double factor = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterRatio;
        long millis = Math.max(0, Math.round(base.toMillis() * factor));
        return Duration.ofMillis(millis);
    }

    /** RNF2: {@code retryAfter} from the error is a floor on the computed delay, not a suggestion. */
    private static Duration withMinimumDelay(Duration computed, Duration retryAfter) {
        return retryAfter != null && computed.compareTo(retryAfter) < 0 ? retryAfter : computed;
    }

    private static Duration effectiveTimeout(CallRemoteNode node) {
        return node.timeout() != null ? node.timeout() : DEFAULT_TIMEOUT;
    }

    // --- persisting an InterpretResult: Suspend / SuspendInTry / Complete / Failed ------------------

    private CompletionStage<SaveOutcome> commit(Instance inst, WorkflowDefinition def, InterpretResult interpreted,
            long expectedVersion, List<String> consumeWaitKeys) {
        String instanceId = inst.id();
        InterpretResult result = checkEmits(interpreted);
        return prepareFacts(inst, def, result.emitted(), expectedVersion).thenCompose(outbox -> {
            CompletionStage<SaveOutcome> saved = switch (result) {
                case InterpretResult.Suspend s -> commitSuspend(inst, def, s.node(), s.pointer(), s.dispatchInput(),
                        s.context(), expectedVersion, consumeWaitKeys, null, outbox);
                case InterpretResult.SuspendInTry s -> commitSuspend(inst, def, s.tryNode().body(),
                        s.tryNode().pointer(), s.dispatchInput(), s.context(), expectedVersion, consumeWaitKeys,
                        new RetryState(s.tryNode().pointer(), 1, clock.instant(), null, null), outbox);
                case InterpretResult.SuspendOnListen s -> commitListen(inst, def, s, expectedVersion, consumeWaitKeys,
                        outbox);
                case InterpretResult.Complete c -> commitTerminal(inst, def.ref(), c.pointer(), c.context(),
                        InstanceStatus.COMPLETED, expectedVersion, consumeWaitKeys,
                        () -> complete(instanceId, c.context()), outbox);
                case InterpretResult.Failed f -> commitTerminal(inst, def.ref(), f.pointer(), f.context(),
                        InstanceStatus.FAILED, expectedVersion, consumeWaitKeys,
                        () -> completeExceptionally(instanceId, new RuntimeException(f.message())), outbox);
            };
            // SDD 2.2, sec. 8a: publish right after the save that recorded the facts. Never fails the
            // transition — the facts are durable; whatever doesn't go out now is swept later.
            return outbox.isEmpty() ? saved : saved.thenCompose(outcome -> outcome != SaveOutcome.OK
                    ? CompletableFuture.completedFuture(outcome)
                    : relay.publishNow(outbox).thenApply(v -> outcome));
        });
    }

    /** SDD 2.2: an activation that emitted but can't record its facts fails instead of silently dropping them. */
    private InterpretResult checkEmits(InterpretResult result) {
        List<Emitted> emitted = result.emitted();
        if (emitted.isEmpty()) {
            return result;
        }
        if (publisher == null) {
            return failedLike(result, "emit requires an EventPublisher, but no EventPublisher configured on this engine");
        }
        if (emitted.size() > MAX_FACTS_PER_ACTIVATION) {
            return failedLike(result, emitted.size() + " facts emitted in one activation exceed the limit of "
                    + MAX_FACTS_PER_ACTIVATION);
        }
        return result;
    }

    private static InterpretResult failedLike(InterpretResult result, String message) {
        return switch (result) {
            case InterpretResult.Suspend s -> InterpretResult.failed(s.pointer(), s.context(), message);
            case InterpretResult.SuspendInTry s -> InterpretResult.failed(s.tryNode().pointer(), s.context(), message);
            case InterpretResult.SuspendOnListen s -> InterpretResult.failed(s.pointer(), s.context(), message);
            case InterpretResult.Complete c -> InterpretResult.failed(c.pointer(), c.context(), message);
            case InterpretResult.Failed f -> InterpretResult.failed(f.pointer(), f.context(), f.message() + "; " + message);
        };
    }

    /**
     * SDD 2.2: turns what the activation emitted into outbox entries — stable id (sec. 8b), CloudEvent
     * built by {@link FactFactory} (sec. 8f) and, when it's too large for the publisher, the data put in
     * the facts store <em>before</em> the save under a deterministic key and replaced by a {@code dataref}
     * (sec. 8d). A re-run overwrites the same key; a blob of a run that lost the save is simply orphaned.
     */
    private CompletionStage<List<OutboxEntry>> prepareFacts(Instance inst, WorkflowDefinition def,
            List<Emitted> emitted, long baseVersion) {
        CompletionStage<List<OutboxEntry>> chain = CompletableFuture.completedFuture(new ArrayList<>());
        Instant now = clock.instant();
        for (int i = 0; i < emitted.size(); i++) {
            Emitted e = emitted.get(i);
            int seq = i;
            chain = chain.thenCompose(entries -> {
                String id = FactFactory.factId(e.with(), inst.id(), baseVersion, e.pointer(), e.occurrence());
                CloudEvent inline = FactFactory.build(id, e.with(), now, inst.activation());
                byte[] json = CeWire.toJson(inline);
                CompletionStage<CloudEvent> fact;
                if (json.length <= publisher.capabilities().maxPayloadBytes() || !FactFactory.hasData(e.with())) {
                    fact = CompletableFuture.completedFuture(inline);
                } else if (factBlob == null) {
                    fact = CompletableFuture.failedFuture(new IllegalStateException("Fact " + id + " is "
                            + json.length + " bytes, above the publisher's limit, and no facts store is configured"));
                } else {
                    fact = factBlob.put(FactFactory.dataBytes(e.with()), FactFactory.factKey(def.ref(), id))
                            .thenApply(ref -> FactFactory.buildWithDataref(id, e.with(), now, ref, inst.activation()));
                }
                return fact.thenApply(built -> {
                    // SDD 3a.5, RF2: a fact carries its instance's level to whoever it starts or resumes
                    CloudEvent ce = inst.sensitivity() == null ? built : CloudEventBuilder.v1(built)
                            .withExtension(StintEvents.EXT_SENSITIVITY, inst.sensitivity().wireName()).build();
                    entries.add(new OutboxEntry(id, inst.id(), seq,
                            new String(CeWire.toJson(ce), StandardCharsets.UTF_8), now));
                    return entries;
                });
            });
        }
        return chain;
    }

    /**
     * SDD 3a.5, RF8: where the engine records what its policy allowed or refused, what a redaction found and which
     * provider answered a decision. Default {@link AuditLog#NONE}.
     */
    public WorkflowEngine auditLog(AuditLog audit) {
        this.audit = Objects.requireNonNull(audit, "audit");
        return this;
    }

    /** SDD 3a.5, RF4 (ADR 3a.5-A, engine side): why the policy refuses dispatching {@code node} now, or empty. */
    private Optional<String> policyRefusal(Instance inst, WorkflowDefinition def, CallRemoteNode node) {
        Policy policy = def.policy();
        if (policy == null) {
            return Optional.empty();
        }
        Optional<String> refusal = policy.refusal(node.pointer(), node.routingKey(), inst.sensitivity());
        if (refusal.isEmpty()) {
            audit(inst, AuditLog.AuditRecord.POLICY, node.pointer(), node.routingKey(), "allowed", Map.of());
        }
        return refusal;
    }

    /**
     * SDD 3a.5, RF4: a dispatch the policy refuses is not sent; the task fails with {@code authorization} (403) at its
     * pointer — caught by its try like any failure, or failing the instance.
     */
    private CompletionStage<SaveOutcome> refuseDispatch(Instance inst, WorkflowDefinition def, CallRemoteNode node,
            String position, JsonNode context, long expectedVersion, List<String> consumeWaitKeys,
            RetryState retryState, List<OutboxEntry> outbox, String reason) {
        ErrorInfo error = new ErrorInfo(ErrorInfo.StandardType.AUTHORIZATION.uri(),
                ErrorInfo.StandardType.AUTHORIZATION.status(), "Dispatch refused by the compliance policy", reason,
                node.pointer(), null);
        LOG.log(Level.WARNING, "Instance {0}: the compliance policy refused dispatching {1} to {2}: {3}", inst.id(),
                node.pointer(), node.routingKey(), reason);
        audit(inst, AuditLog.AuditRecord.POLICY, node.pointer(), node.routingKey(), "refused",
                Map.of("reason", reason));
        if (def.at(position) instanceof TryNode tryNode) {
            InstanceSnapshot attempt = new InstanceSnapshot(inst.id(), def.ref(), position, null, context,
                    InstanceStatus.WAITING, expectedVersion, retryState, clock.instant(), inst.input(),
                    inst.startedAt(), inst.chainId(), inst.trace(), inst.listenState(), inst.decisions(),
                    inst.sensitivity());
            return handleTryFailure(inst, def, tryNode, attempt, error, consumeWaitKeys);
        }
        return commitTerminal(inst, def.ref(), position, context, InstanceStatus.FAILED, expectedVersion,
                consumeWaitKeys, () -> completeExceptionally(inst.id(), new RuntimeException("Task failed: " + error)),
                outbox);
    }

    /**
     * SDD 3a.5, RF8: an AI call — the provider, model and adapter of a checked output's record (a decision) and what the
     * worker reported on the result ({@code stintprovider}): names and flags only.
     */
    /**
     * SDD 3a.5, RF8: a failed task that reported a provider is a failed AI call; one that reported none and failed with
     * {@code authorization} was refused by the worker's policy before any provider saw the data. Type and status
     * only: an error's detail is not audited.
     */
    private void auditFailure(Instance inst, CallRemoteNode call, TaskResult result) {
        if (call == null || result.error() == null) {
            return;
        }
        if (result.provider() != null) {
            auditAiCall(inst, call, null, result.provider(), "failed");
        } else if (ErrorInfo.StandardType.AUTHORIZATION.uri().equals(result.error().type())) {
            audit(inst, AuditLog.AuditRecord.POLICY, call.pointer(), call.routingKey(), "refused",
                    Map.of("by", "worker", "status", String.valueOf(result.error().status())));
        }
    }

    private void auditAiCall(Instance inst, CallRemoteNode call, JsonNode record, ModelProvider provider,
            String outcome) {
        JsonNode provenance = record == null ? null : record.get("provenance");
        boolean hasProvenance = provenance != null && provenance.isObject();
        if (!hasProvenance && provider == null) {
            return;
        }
        Map<String, String> detail = new HashMap<>();
        if (hasProvenance) {
            provenance.properties().forEach(e -> {
                if (e.getValue().isTextual()) {
                    detail.put(e.getKey(), e.getValue().asText());
                }
            });
        }
        if (provider != null) {
            // SDD 3a.5, RF8: what the worker reported on the result (stintprovider); the decision's provenance, when
            // there is one, keeps its own provider and model
            detail.putIfAbsent("provider", provider.name());
            if (provider.model() != null) {
                detail.putIfAbsent("model", provider.model());
            }
            detail.put("remote", String.valueOf(provider.remote()));
        }
        audit(inst, AuditLog.AuditRecord.AI_CALL, call.pointer(), call.routingKey(), outcome, detail);
    }

    /**
     * SDD 3a.5, RF8: an audited output (a redaction): its record's numbers and flags — never the output itself — and
     * whether the level went down.
     */
    private void auditOutput(Instance before, Instance after, CallRemoteNode call, OutputContract contract,
            JsonNode output) {
        JsonNode record = contract.record(output);
        Map<String, String> detail = new HashMap<>();
        if (record != null) {
            record.properties().forEach(e -> {
                if (e.getValue().isObject()) {
                    e.getValue().properties().forEach(inner -> {
                        if (inner.getValue().isValueNode()) {
                            detail.put(e.getKey() + "." + inner.getKey(), inner.getValue().asText());
                        }
                    });
                } else if (e.getValue().isValueNode()) {
                    detail.put(e.getKey(), e.getValue().asText());
                }
            });
        }
        boolean lowered = before.sensitivity() != null && after.sensitivity() != before.sensitivity();
        if (lowered) {
            detail.put("from", before.sensitivity().wireName());
        }
        audit(after, contract.auditKind(), call.pointer(), call.routingKey(), lowered ? "lowered" : "kept", detail);
    }

    /** Best effort, never failing a transition; ids, levels and counts only. */
    private void audit(Instance inst, String kind, String pointer, String route, String outcome,
            Map<String, String> detail) {
        AuditLog log = audit;
        if (log == AuditLog.NONE) {
            return;
        }
        try {
            log.append(new AuditLog.AuditRecord(inst.id(), inst.chainId(), clock.instant(), kind, pointer,
                    inst.sensitivity(), route, outcome, detail)).exceptionally(t -> {
                        LOG.log(Level.WARNING, "Audit record of instance {0} not written ({1})", inst.id(),
                                t.getClass().getName());
                        return null;
                    });
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Audit record of instance {0} not written ({1})", inst.id(),
                    e.getClass().getName());
        }
    }

    /** SDD 2.2: the relay that publishes this engine's facts, or {@code null} if it has no publisher. */
    public OutboxRelay outboxRelay() {
        return relay;
    }

    private CompletionStage<SaveOutcome> commitSuspend(Instance inst, WorkflowDefinition def, CallRemoteNode node,
            String position, JsonNode dispatchInput, JsonNode context, long expectedVersion,
            List<String> consumeWaitKeys, RetryState initialRetryState, List<OutboxEntry> outbox) {
        String instanceId = inst.id();
        Optional<String> refused = policyRefusal(inst, def, node);
        if (refused.isPresent()) {
            return refuseDispatch(inst, def, node, position, context, expectedVersion, consumeWaitKeys,
                    initialRetryState, outbox, refused.get());
        }
        String correlationId = UUID.randomUUID().toString();
        String taskKey = WaitKeys.task(correlationId);
        String timerKey = WaitKeys.timer(correlationId);
        // Keyed per attempt (see redispatchRetry): racing duplicate creates never share a blob key.
        return ClaimCheck.offload(dispatchInput, transport.capabilities(), blob,
                        instanceId + "/" + node.name() + "/" + correlationId)
                .thenCompose(wireInput -> {
                    // SDD 2.9, RF4: inside a try, attempt 1 records what it dispatched; retries send it again.
                    RetryState retryState = initialRetryState == null ? null
                            : new RetryState(initialRetryState.tryNodePointer(), initialRetryState.attempt(),
                                    initialRetryState.firstAttemptAt(), initialRetryState.lastError(), correlationId,
                                    initialRetryState.dispatchInput() != null ? initialRetryState.dispatchInput()
                                            : wireInput);
                    InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), position, taskKey, context,
                            InstanceStatus.WAITING, expectedVersion + 1, retryState, clock.instant(),
                            inst.input(), inst.startedAt(), inst.chainId(), inst.trace(), inst.listenState(),
                            inst.decisions(), inst.sensitivity());
                    Duration timeout = effectiveTimeout(node);
                    int attempt = retryState != null ? retryState.attempt() : 1;
                    // SDD 3a.3c: the attempt's deadline is the instant its timeout timer fires
                    Instant deadline = clock.instant().plus(timeout);
                    TaskInvocation inv = new TaskInvocation(instanceId, position, correlationId, def.ref(),
                            node.routingKey(), wireInput, attempt, inst.activation(), deadline,
                            inst.sensitivity());

                    return armThenSave(new TimerRequest(timerKey, instanceId, correlationId, deadline),
                            () -> state.save(snap, expectedVersion,
                                    List.of(new Wait(taskKey, instanceId, position, clock.instant()),
                                            new Wait(timerKey, instanceId, position, clock.instant())),
                                    consumeWaitKeys, outbox, inst.inboxConsumed()),
                            () -> transport.dispatch(inv));
                });
    }

    // --- listen (SDD 2.3) ---------------------------------------------------------------------------

    /**
     * SDD 2.3, sec. 4.2: suspends at a {@code listen}. Each filter's {@code expect}s are evaluated now, against
     * the task input, into one {@code event:} key per filter (sec. 4.1); the keys, the timeout timer (armed
     * before the save, RF8) and the facts emitted before the listen go in one save — so an answer to one of
     * those facts can never arrive before its wait exists (sec. 8c-1). Then the inbox is checked for events
     * that arrived earlier (sec. 8c-2).
     */
    private CompletionStage<SaveOutcome> commitListen(Instance inst, WorkflowDefinition def,
            InterpretResult.SuspendOnListen s, long expectedVersion, List<String> consumeWaitKeys,
            List<OutboxEntry> outbox) {
        String instanceId = inst.id();
        ListenNode listen = s.listen();
        List<String> keys;
        try {
            keys = listenKeys(listen, s.input(), s.context(), inst.descriptor(def.ref()));
        } catch (RuntimeException e) {
            return commitTerminal(inst, def.ref(), s.pointer(), s.context(), InstanceStatus.FAILED, expectedVersion,
                    consumeWaitKeys, () -> completeExceptionally(instanceId, e), outbox);
        }
        String listenId = UUID.randomUUID().toString();
        String timerKey = listen.timeout() == null ? null : WaitKeys.timer(listenId);
        Instant now = clock.instant();
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, def.ref(), s.pointer(), keys.get(0), s.context(),
                InstanceStatus.WAITING, expectedVersion + 1, null, now, inst.input(), inst.startedAt(),
                inst.chainId(), inst.trace(),
                ListenState.withActive(inst.listenState(), listenId, listen.strategy().name(), keys, timerKey),
                inst.decisions(), inst.sensitivity());
        List<Wait> waits = new ArrayList<>();
        for (String key : keys) {
            waits.add(new Wait(key, instanceId, s.pointer(), now));
        }
        if (timerKey != null) {
            waits.add(new Wait(timerKey, instanceId, s.pointer(), now));
        }
        Supplier<CompletionStage<SaveOutcome>> save = () -> state.save(snap, expectedVersion, waits, consumeWaitKeys,
                outbox, inst.inboxConsumed());
        CompletionStage<SaveOutcome> saved = timerKey == null
                ? save.get().thenCompose(outcome -> outcome != SaveOutcome.OK
                        ? CompletableFuture.completedFuture(outcome)
                        : deliverKeptEvents(keys).thenApply(v -> outcome))
                : armThenSave(new TimerRequest(timerKey, instanceId, listenId, now.plus(listen.timeout())), save,
                        () -> deliverKeptEvents(keys));
        return saved.thenCompose(outcome -> outcome != SaveOutcome.CONFLICT
                ? CompletableFuture.completedFuture(outcome)
                : failIfCorrelationTaken(inst, def, s, keys, expectedVersion, consumeWaitKeys, outbox));
    }

    /** One key per filter, in filter order. */
    private List<String> listenKeys(ListenNode listen, JsonNode input, JsonNode context, WorkflowDescriptor wf) {
        List<String> keys = new ArrayList<>();
        for (ListenNode.Filter filter : listen.filters()) {
            SortedMap<String, JsonNode> values = new TreeMap<>();
            for (Map.Entry<String, ListenNode.Correlation> c : filter.correlate().entrySet()) {
                JsonNode value = interpreter.evaluator().eval(c.getValue().expect(), input, new EvalScope(context, wf));
                if (value == null || value.isNull() || value.isMissingNode()) {
                    throw new IllegalStateException("listen at " + listen.pointer() + ": correlate." + c.getKey()
                            + ".expect evaluated to null");
                }
                values.put(c.getKey(), value);
            }
            String key = EventCorrelation.waitKey(filter.type(), EventCorrelation.shapeOf(filter), values);
            if (keys.contains(key)) {
                throw new IllegalStateException("listen at " + listen.pointer()
                        + ": two filters wait on the same type with the same correlation values");
            }
            keys.add(key);
        }
        return keys;
    }

    /**
     * Sec. 8a: a wait key belongs to one instance. If the save lost because another instance already waits
     * on one of these keys, retrying can't help — this instance fails with {@code correlation-conflict}.
     * Any other conflict is returned as is, for the usual retry.
     */
    private CompletionStage<SaveOutcome> failIfCorrelationTaken(Instance inst, WorkflowDefinition def,
            InterpretResult.SuspendOnListen s, List<String> keys, long expectedVersion, List<String> consumeWaitKeys,
            List<OutboxEntry> outbox) {
        CompletionStage<String> taken = CompletableFuture.completedFuture(null);
        for (String key : keys) {
            taken = taken.thenCompose(found -> found != null ? CompletableFuture.completedFuture(found)
                    : state.findWait(key).thenApply(w -> w.filter(wait -> !wait.instanceId().equals(inst.id()))
                            .map(wait -> key).orElse(null)));
        }
        return taken.thenCompose(key -> {
            if (key == null) {
                return CompletableFuture.completedFuture(SaveOutcome.CONFLICT);
            }
            return commitTerminal(inst, def.ref(), s.pointer(), s.context(), InstanceStatus.FAILED, expectedVersion,
                    consumeWaitKeys, () -> completeExceptionally(inst.id(), new IllegalStateException(
                            "correlation-conflict: another instance already waits on " + key)), outbox);
        });
    }

    /**
     * Sec. 8c-2, the suspending side of "write, then check": the wait is saved; now any early event kept under
     * one of its keys (and not expired) is delivered as if it had just arrived. The arriving side does the
     * mirror image, so at least one of them sees the other; if both do, the conditional consume of the key
     * lets one win. Never fails the activation: the delivery is a separate one.
     */
    private CompletionStage<Void> deliverKeptEvents(List<String> keys) {
        CompletionStage<Void> chain = done();
        for (String key : keys) {
            chain = chain.thenCompose(v -> findInbox(key)).thenCompose(entries -> {
                CompletionStage<Void> deliveries = done();
                Instant now = clock.instant();
                for (InboxEntry entry : entries) {
                    if (entry.expiresAt().isAfter(now)) {
                        CloudEvent kept = CeWire.fromJson(entry.event().getBytes(StandardCharsets.UTF_8));
                        deliveries = deliveries.thenCompose(v -> resumeWithRetry(key,
                                new ResumeEvent.EventArrived(kept, true), 1));
                    }
                }
                return deliveries;
            });
        }
        return chain.exceptionally(ex -> {
            LOG.log(Level.WARNING, "Could not check the early-event inbox for " + keys, ex);
            return null;
        });
    }

    private CompletionStage<List<InboxEntry>> findInbox(String key) {
        try {
            return state.findInbox(key);
        } catch (UnsupportedOperationException e) {
            return CompletableFuture.completedFuture(List.of()); // a store without an inbox keeps no early events
        }
    }

    /**
     * SDD 2.3, sec. 4.2 step 4 / 8b / 8d: an event or the timeout reached an instance waiting in a
     * {@code listen}. The timeout consumes every pending key and fails the task ({@code catch}able). An event
     * for {@code one}/{@code any} consumes every pending key and the timer; for {@code all} it records a partial
     * and consumes only its own key, until the last one completes the task. Concurrent arrivals are serialized
     * by the versioned save (SDD 1.2): the loser reloads and sees the winner's partial.
     */
    private CompletionStage<SaveOutcome> advanceListen(Instance inst, WorkflowDefinition def, InstanceSnapshot snap,
            TaskNode node, ObjectNode active, ResumeEvent event, String waitKey) {
        TryNode tryNode = node instanceof TryNode t ? t : null;
        ListenNode listen = tryNode != null ? tryNode.listen() : node instanceof ListenNode l ? l : null;
        List<String> pending = ListenState.strings(active.get("pending"));
        String timerKey = active.hasNonNull("timerKey") ? active.get("timerKey").asText() : null;
        List<String> consumeAll = new ArrayList<>(pending);
        if (timerKey != null) {
            consumeAll.add(timerKey);
        }
        if (listen == null) {
            return commitTerminal(inst, snap.definition(), snap.position(), snap.context(), InstanceStatus.FAILED,
                    snap.version(), consumeAll, () -> completeExceptionally(inst.id(), new IllegalStateException(
                            "Instance " + inst.id() + " has an active listen but is not at one: " + snap.position())));
        }

        if (!(event instanceof ResumeEvent.EventArrived arrived)) {
            ErrorInfo error = ErrorInfo.timeout("listen at " + snap.position()
                    + " did not receive the events it waits for within its timeout").raisedAt(snap.position());
            if (tryNode != null) {
                return handleTryFailure(inst, def, tryNode, snap, error, consumeAll);
            }
            return commitTerminal(inst, snap.definition(), snap.position(), snap.context(), InstanceStatus.FAILED,
                    snap.version(), consumeAll,
                    () -> completeExceptionally(inst.id(), new RuntimeException("Uncaught listen timeout: " + error)));
        }

        CloudEvent ce = arrived.event();
        int filterIndex = ListenState.strings(active.get("keys")).indexOf(waitKey);
        if (filterIndex < 0 || !matchesWith(listen.filters().get(filterIndex), ce)
                || ListenState.alreadyConsumed(snap.listenState(), ce)) {
            // Not this wait's event after all (an attribute of 'with' differs), or a redelivery of an event this
            // instance already consumed (sec. 8c): nothing to record.
            return CompletableFuture.completedFuture(SaveOutcome.OK);
        }
        ObjectNode after = ListenState.withPartial(snap.listenState(), waitKey, filterIndex,
                Json.read(CeWire.toJson(ce)), ce);
        List<String> stillPending = ListenState.strings(after.get("active").get("pending"));

        if (listen.strategy() == ListenNode.Strategy.ALL && !stillPending.isEmpty()) {
            InstanceSnapshot partial = new InstanceSnapshot(inst.id(), snap.definition(), snap.position(),
                    stillPending.get(0), snap.context(), InstanceStatus.WAITING, snap.version() + 1, null,
                    clock.instant(), inst.input(), inst.startedAt(), inst.chainId(), inst.trace(), after,
                    inst.decisions(), inst.sensitivity());
            return state.save(partial, snap.version(), List.of(), List.of(waitKey), List.of(), inst.inboxConsumed());
        }

        ArrayNode consumed = (ArrayNode) after.get("active").get("events");
        Instance next = inst.withListenState(ListenState.carried(after));
        WorkflowDescriptor wf = next.descriptor(snap.definition());
        return readEvents(listen.read(), consumed).thenCompose(output -> {
            InterpretResult result = tryNode != null
                    ? interpreter.resumeTry(def, wf, tryNode, output, snap.context())
                    : interpreter.resume(def, wf, snap.position(), listen, output, snap.context());
            return commit(next, def, result, snap.version(), consumeAll);
        }).thenCompose(outcome -> outcome != SaveOutcome.OK || timerKey == null
                ? CompletableFuture.completedFuture(outcome)
                : timer.cancel(timerKey).handle((v, ex) -> outcome)); // best-effort: a late fire finds no wait
    }

    /** The exact-match subset of {@code with} (sec. 8a): {@code type} is already implied by the key. */
    private static boolean matchesWith(ListenNode.Filter filter, CloudEvent event) {
        for (Map.Entry<String, String> required : filter.with().entrySet()) {
            Object value = switch (required.getKey()) {
                case "specversion", "id", "source", "type", "datacontenttype", "dataschema", "subject", "time" ->
                        event.getAttribute(required.getKey());
                default -> event.getExtension(required.getKey());
            };
            if (value == null || !required.getValue().equals(value.toString())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Sec. 8e, DSL 1.0 Listen {@code read}: {@code data} (default) — the event's data, a {@code dataref}
     * (SDD 2.2) resolved from the facts store; {@code envelope} — the whole event, structured JSON;
     * {@code raw} — the data as received (JSON as is, anything else base64). One element per consumed event,
     * in consumption order.
     */
    private CompletionStage<JsonNode> readEvents(ListenNode.Read read, ArrayNode consumed) {
        CompletionStage<ArrayNode> out = CompletableFuture.completedFuture(Json.MAPPER.createArrayNode());
        for (JsonNode entry : consumed) {
            JsonNode envelope = entry.get("event");
            out = out.thenCompose(array -> readOne(read, envelope).thenApply(array::add));
        }
        return out.thenApply(array -> array);
    }

    private CompletionStage<JsonNode> readOne(ListenNode.Read read, JsonNode envelope) {
        if (read == ListenNode.Read.ENVELOPE) {
            return CompletableFuture.completedFuture(envelope);
        }
        if (envelope.has("data")) {
            return CompletableFuture.completedFuture(envelope.get("data"));
        }
        if (envelope.has("data_base64")) {
            if (read == ListenNode.Read.RAW) {
                return CompletableFuture.completedFuture(envelope.get("data_base64"));
            }
            return CompletableFuture.completedFuture(jsonOrBase64(Base64.getDecoder().decode(
                    envelope.get("data_base64").asText())));
        }
        if (read == ListenNode.Read.DATA && envelope.hasNonNull(FactFactory.DATAREF)) {
            BlobStore store = factBlob != null ? factBlob : blob;
            return store.get(java.net.URI.create(envelope.get(FactFactory.DATAREF).asText()))
                    .thenApply(WorkflowEngine::jsonOrBase64);
        }
        return CompletableFuture.completedFuture(Json.MAPPER.nullNode());
    }

    private static JsonNode jsonOrBase64(byte[] bytes) {
        try {
            return Json.read(bytes);
        } catch (RuntimeException notJson) {
            return Json.MAPPER.getNodeFactory().textNode(Base64.getEncoder().encodeToString(bytes));
        }
    }

    /**
     * SDD 2.3, RF4/sec. 4.2 step 3: what the domain consumer does with an inbound event for {@code listen}s.
     * For every correlation shape the registry knows for its type (sec. 8a), the {@code from}s are applied to
     * the event to build the key; a key with a wait resumes that instance, a key without one keeps the event
     * in the inbox for {@code earlyEventWindow} and checks again (sec. 8c-2 — the arriving side of "write,
     * then check"). The stage completes once every effect is recorded, so the source may then ack.
     *
     * @return whether the event concerned any {@code listen} (a key could be built for it)
     */
    public CompletionStage<Boolean> onDomainEvent(CloudEvent event, Duration earlyEventWindow) {
        List<EventCorrelation.Shape> shapes = registry.correlationShapes(event.getType());
        if (shapes.isEmpty()) {
            return CompletableFuture.completedFuture(false);
        }
        byte[] json = CeWire.toJson(event);
        JsonNode envelope = Json.read(json);
        List<String> keys = new ArrayList<>();
        for (EventCorrelation.Shape shape : shapes) {
            SortedMap<String, JsonNode> values = correlationValues(shape, envelope);
            if (values != null) {
                keys.add(EventCorrelation.waitKey(event.getType(), shape, values));
            }
        }
        CompletionStage<Void> chain = done();
        for (String key : keys) {
            chain = chain.thenCompose(v -> deliverOrKeep(key, event, json, earlyEventWindow));
        }
        return chain.thenApply(v -> !keys.isEmpty());
    }

    /** The event's value for each correlation of {@code shape}, or {@code null} if one is absent. */
    private SortedMap<String, JsonNode> correlationValues(EventCorrelation.Shape shape, JsonNode envelope) {
        SortedMap<String, JsonNode> values = new TreeMap<>();
        for (Map.Entry<String, Expr> from : shape.from().entrySet()) {
            JsonNode value;
            try {
                value = interpreter.evaluator().eval(from.getValue(), envelope,
                        new EvalScope(Json.obj(), (WorkflowDescriptor) null));
            } catch (RuntimeException e) {
                return null; // not an event of this shape
            }
            if (value == null || value.isNull() || value.isMissingNode()) {
                return null;
            }
            values.put(from.getKey(), value);
        }
        return values;
    }

    private CompletionStage<Void> deliverOrKeep(String key, CloudEvent event, byte[] json, Duration window) {
        return state.findWait(key).thenCompose(wait -> {
            if (wait.isPresent()) {
                return resumeWithRetry(key, new ResumeEvent.EventArrived(event, false), 1);
            }
            CompletionStage<Void> kept;
            try {
                kept = state.putInbox(new InboxEntry(key, event.getId(), new String(json, StandardCharsets.UTF_8),
                        clock.instant().plus(window)));
            } catch (UnsupportedOperationException e) {
                LOG.log(Level.INFO, "Event {0} arrived before any wait on {1} and the state store has no inbox; "
                        + "dropped", event.getId(), key);
                return done();
            }
            return kept.thenCompose(v -> state.findWait(key)).thenCompose(recheck -> recheck.isEmpty() ? done()
                    : resumeWithRetry(key, new ResumeEvent.EventArrived(event, true), 1));
        });
    }

    /**
     * SDD 2.1, RF8: arms the timer <em>before</em> the save that registers the wait it guards, then runs
     * {@code afterSaved} (the dispatch) only once the save is OK. A crash after the save can no longer
     * leave a WAITING instance with no timer and no task in flight: the timer is already armed, fires,
     * and the instance follows its timeout/retry policy (SDD 1.3). If the save loses (CONFLICT) or
     * fails, the armed timer is an orphan: it's cancelled best-effort, and if it fires anyway
     * {@code findWait} finds nothing and the fire is a no-op (SDD 1.3, sec. 8a).
     */
    private CompletionStage<SaveOutcome> armThenSave(TimerRequest timerRequest,
            Supplier<CompletionStage<SaveOutcome>> save,
            Supplier<CompletionStage<Void>> afterSaved) {
        return timer.schedule(timerRequest)
                .thenCompose(timerId -> save.get())
                .thenCompose(outcome -> {
                    if (outcome != SaveOutcome.OK) {
                        return timer.cancel(timerRequest.timerId()).thenApply(v -> outcome);
                    }
                    return afterSaved.get().thenApply(v -> outcome);
                });
    }

    private CompletionStage<SaveOutcome> commitTerminal(Instance inst, WorkflowRef ref, String position,
            JsonNode context, InstanceStatus status, long expectedVersion, List<String> consumeWaitKeys,
            Runnable onSuccess) {
        return commitTerminal(inst, ref, position, context, status, expectedVersion, consumeWaitKeys, onSuccess,
                List.of());
    }

    private CompletionStage<SaveOutcome> commitTerminal(Instance inst, WorkflowRef ref, String position,
            JsonNode context, InstanceStatus status, long expectedVersion, List<String> consumeWaitKeys,
            Runnable onSuccess, List<OutboxEntry> outbox) {
        String instanceId = inst.id();
        InstanceSnapshot snap = new InstanceSnapshot(instanceId, ref, position, null, context, status,
                expectedVersion + 1, null, clock.instant(), inst.input(), inst.startedAt(), inst.chainId(), inst.trace(),
                inst.listenState(), inst.decisions(), inst.sensitivity());
        return state.save(snap, expectedVersion, List.of(), consumeWaitKeys, outbox, inst.inboxConsumed())
                .thenApply(outcome -> {
                    if (outcome == SaveOutcome.OK) {
                        onSuccess.run();
                    }
                    return outcome;
                });
    }

    /**
     * SDD 1.5, RF4: the StateStore save that reaches {@code COMPLETED} has already happened by the
     * time this runs — {@link #awaitCompletion} discovers it independently by polling the store, so
     * this is just an observability hook now (the in-memory-only completions map it used to feed is
     * gone: it never worked across JVMs and its entries were never evicted).
     */
    private void complete(String instanceId, JsonNode context) {
        LOG.log(Level.DEBUG, "Instance {0} completed", instanceId);
    }

    private void completeExceptionally(String instanceId, Throwable t) {
        LOG.log(Level.WARNING, "Instance " + instanceId + " failed", t);
    }

    /** Best-effort: an I/O failure (not a version/wait conflict) happened mid-transition. */
    private void failExistingInstance(String instanceId, Throwable t) {
        state.load(instanceId).thenAccept(opt -> opt.ifPresent(snap -> {
            InstanceSnapshot failedSnap = new InstanceSnapshot(instanceId, snap.definition(), snap.position(),
                    null, snap.context(), InstanceStatus.FAILED, snap.version() + 1, null, clock.instant(),
                    snap.input(), snap.startedAt(), snap.chainId(), snap.suspendedAt(), null,
                    snap.decisions(), snap.sensitivity());
            List<String> consume = snap.waitingKey() == null ? List.of() : List.of(snap.waitingKey());
            state.save(failedSnap, snap.version(), List.of(), consume);
        }));
        completeExceptionally(instanceId, t);
    }

    /**
     * What identifies a running instance across every transition (SDD 2.1, RF6): its id plus the raw
     * input and start time the DSL 1.0 {@code $workflow} descriptor exposes. Carried unchanged into
     * every snapshot so a resume on another engine sees the same {@code $workflow}.
     */
    private record Instance(String id, JsonNode input, Instant startedAt, String chainId, Lineage activation,
                            JsonNode listenState, List<InboxEntry.Key> inboxConsumed, JsonNode decisions,
                            Sensitivity sensitivity) {

        /**
         * SDD 2.5: {@code activation} = chain, cause and trace context of the activation now running.
         * SDD 2.3: {@code listenState} carries forward only what outlives a {@code listen} (the ids of the
         * events already consumed); {@code inboxConsumed} = the early events this activation consumes, removed
         * from the inbox by the same save. SDD 3a.1: {@code decisions} = the records carried into every snapshot.
         */
        static Instance of(InstanceSnapshot snap, Lineage activation, List<InboxEntry.Key> inboxConsumed) {
            return new Instance(snap.instanceId(), snap.input(), snap.startedAt(), activation.chainId(), activation,
                    ListenState.carried(snap.listenState()), inboxConsumed, snap.decisions(), snap.sensitivity());
        }

        Instance withListenState(JsonNode state) {
            return new Instance(id, input, startedAt, chainId, activation, state, inboxConsumed, decisions,
                    sensitivity);
        }

        Instance withSensitivity(Sensitivity level) {
            return new Instance(id, input, startedAt, chainId, activation, listenState, inboxConsumed, decisions,
                    level);
        }

        Instance withDecisions(JsonNode records) {
            return new Instance(id, input, startedAt, chainId, activation, listenState, inboxConsumed, records,
                    sensitivity);
        }

        /** What the snapshot keeps for the next activation to link to (SDD 2.5, 8b). */
        Lineage trace() {
            return Lineage.trace(activation.traceparent(), activation.tracestate());
        }

        WorkflowDescriptor descriptor(WorkflowRef ref) {
            return new WorkflowDescriptor(id, input, startedAt, ref);
        }
    }

    private static long backoffMillis(int attempt) {
        long value = BASE_BACKOFF_MILLIS * (1L << attempt);
        return Math.min(value, MAX_BACKOFF_MILLIS);
    }

    private static CompletionStage<Void> delay(long millis) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        RETRY_SCHEDULER.schedule(() -> future.complete(null), millis, TimeUnit.MILLISECONDS);
        return future;
    }

    private static CompletionStage<Void> done() {
        return CompletableFuture.completedFuture(null);
    }
}
