package io.stintflow.inmemory;

import static io.stintflow.inmemory.EmitPublishTest.order;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;

/**
 * SDD 2.2, CA2 (sec. 8a/8b): a fact is never lost and always keeps its id —
 * <ul>
 *   <li>the publish right after the save fails: the fact stays in the outbox and the sweep (clock past
 *       the grace period, no sleep) publishes it with the same id;</li>
 *   <li>the process dies <em>before</em> the save: nothing was recorded, and the re-run (redelivered
 *       result, another engine) emits the same fact with the same id.</li>
 * </ul>
 */
class OutboxRelayTest {

    private static final WorkflowRef REF = new WorkflowRef("billing", "invoice-order", "1.0.0");
    private static final String INVOICED = "io.acme.order.invoiced.v1";
    private static final Expr FACT = Expr.jq(
            "{source: \"https://acme.example/billing\", type: \"" + INVOICED + "\", data: {orderId: .orderId}}");

    @Test
    void ca2_publish_failure_after_the_save_is_swept_later_with_the_same_id() throws Exception {
        EmitFixture f = new EmitFixture(WorkflowBuilder.create().emit("announce", INVOICED, FACT, DataFlow.NONE).build(REF));
        f.publisher.failNext(1);

        String instanceId = f.engine.start(REF, order("A-1", 1));

        assertThat(f.load(instanceId).status()).isEqualTo(InstanceStatus.COMPLETED); // the transition did not fail
        assertThat(f.publisher.published()).isEmpty();
        List<OutboxEntry> pending = f.outbox();
        assertThat(pending).hasSize(1);
        String factId = pending.get(0).eventId();

        // Within the grace period the sweep leaves it alone (it would race the immediate publish).
        assertThat(f.engine.outboxRelay().sweep().toCompletableFuture().get(5, TimeUnit.SECONDS)).isZero();
        assertThat(f.outbox()).hasSize(1);

        f.clock.advance(Duration.ofSeconds(31));
        assertThat(f.engine.outboxRelay().sweep().toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo(1);

        assertThat(f.publisher.attempts()).isEqualTo(2);
        assertThat(f.publisher.published()).singleElement().satisfies(e -> assertThat(e.getId()).isEqualTo(factId));
        assertThat(f.outbox()).isEmpty(); // CA6
    }

    @Test
    void ca2_a_crash_before_the_save_reruns_the_emit_with_the_same_id() throws Exception {
        WorkflowDefinition def = WorkflowBuilder.create()
                .callRemote("work", "route-work", DataFlow.NONE)
                .emit("announce", INVOICED, FACT, DataFlow.NONE)
                .build(REF);
        DiesBeforeSavingFacts state = new DiesBeforeSavingFacts(new InMemoryStateStore());
        EmitFixture engineA = new EmitFixture(state, new InMemoryEventPublisher(), def);

        String instanceId = engineA.engine.start(REF, order("A-1", 1));
        TaskInvocation work = engineA.transport.awaitDispatch();
        TaskResult result = TaskResult.completed(work.correlationId(), Json.obj());

        // Engine A computes the fact, then "dies" inside the save that would record it: nothing is saved.
        engineA.transport.deliverResult(result); // never completes — the process is gone
        OutboxEntry lost = state.died.get();
        assertThat(lost).isNotNull();
        assertThat(engineA.publisher.published()).isEmpty();
        assertThat(state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow().status())
                .isEqualTo(InstanceStatus.WAITING); // still waiting on "work": the result will be redelivered

        // Engine B (same StateStore) gets the redelivered result and re-runs the same activation.
        InMemoryEventPublisher publisherB = new InMemoryEventPublisher();
        DomainEventFixture.CapturingTransport transportB = new DomainEventFixture.CapturingTransport();
        new WorkflowEngine(engineA.registry, transportB, state, new InMemoryTimerService(engineA.clock),
                new FilesystemBlobStore(Files.createTempDirectory("stint-ca2-b")),
                new TreeInterpreter(new JqExpressionEvaluator()), engineA.clock, publisherB, engineA.facts);
        transportB.deliverResult(result).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(publisherB.published()).singleElement().satisfies(e -> assertThat(e.getId()).isEqualTo(lost.eventId()));
        // SDD 2.5, CA6 (RNF2, decision 5): the re-run yields the same chain and cause too. The trace context may
        // legitimately differ between the two attempts, so it is deliberately not compared.
        CloudEvent lostFact = CeWire.fromJson(lost.event().getBytes(StandardCharsets.UTF_8));
        CloudEvent publishedFact = publisherB.published().get(0);
        assertThat(publishedFact.getExtension(StintEvents.EXT_CHAIN_ID)).isNotNull()
                .isEqualTo(lostFact.getExtension(StintEvents.EXT_CHAIN_ID));
        assertThat(publishedFact.getExtension(StintEvents.EXT_CAUSATION_ID)).isNotNull()
                .isEqualTo(lostFact.getExtension(StintEvents.EXT_CAUSATION_ID));
        assertThat(state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow().status())
                .isEqualTo(InstanceStatus.COMPLETED);
        assertThat(state.pendingOutbox(Instant.MAX.minusSeconds(1), 10).toCompletableFuture().get()).isEmpty(); // CA6
    }

    /** The first save that carries facts never happens — the process died right before it. */
    private static final class DiesBeforeSavingFacts implements StateStore {
        private final StateStore delegate;
        private final AtomicBoolean dead = new AtomicBoolean();
        final AtomicReference<OutboxEntry> died = new AtomicReference<>();

        DiesBeforeSavingFacts(StateStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                List<Wait> addWaits, List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            if (!addOutbox.isEmpty() && dead.compareAndSet(false, true)) {
                died.set(addOutbox.get(0));
                return new CompletableFuture<>(); // never completes: nothing after this runs
            }
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox);
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
}
