package io.stintflow.inmemory;

import static io.stintflow.inmemory.ScheduleReconcilerTest.DAILY;
import static io.stintflow.inmemory.ScheduleReconcilerTest.scheduled;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.builder.WorkflowBuilder;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.DataFlow;
import io.stintflow.core.model.Schedule;
import io.stintflow.core.schedule.ScheduleReconciler;
import io.stintflow.core.schedule.ScheduledStart;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.EventFilter;
import io.stintflow.core.trigger.StartReaction;
import io.stintflow.core.trigger.TriggerBindings;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.ScheduleFire;
import io.stintflow.spi.ScheduleSpec;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * SDD 2.4:
 * <ul>
 *   <li>a cron occurrence, reached by advancing the controllable clock, starts the definition;</li>
 *   <li>CA3: the same occurrence delivered twice, even at the same time, starts one instance;</li>
 *   <li>each occurrence starts a new chain (SDD 2.5);</li>
 *   <li>the floci fallback derives the occurrence from the delivery time;</li>
 *   <li>{@code schedule.on} binds its events and starts the definition.</li>
 * </ul>
 */
class ScheduledStartTest {

    /** A cron start whose instance records the synthetic event it was started with. */
    private static final class Fixture {
        final EmitFixture.MutableClock clock = new EmitFixture.MutableClock(); // 2026-01-01T00:00Z, a Thursday
        final WorkflowRegistry registry = new WorkflowRegistry();
        final CountingCreates state = new CountingCreates();
        final InMemoryScheduleService schedules = new InMemoryScheduleService(clock);
        final WorkflowEngine engine;
        final ScheduleSpec spec;

        Fixture(WorkflowDefinition def) throws Exception {
            registry.register(def);
            engine = new WorkflowEngine(registry, new DomainEventFixture.CapturingTransport(), state,
                    new InMemoryTimerService(clock), new FilesystemBlobStore(Files.createTempDirectory("stint-sdd24")),
                    new TreeInterpreter(new JqExpressionEvaluator()), clock, null, null);
            schedules.onFire(new ScheduledStart(registry, engine));
            new ScheduleReconciler(registry, schedules).reconcile().toCompletableFuture().get();
            spec = registry.scheduleSpecs().get(0);
        }

        String instanceFor(Instant occurrence) {
            return WorkflowEngine.triggeredInstanceId(spec.definition(), ScheduledStart.syntheticEvent(spec, occurrence));
        }

        InstanceSnapshot load(String id) throws Exception {
            return state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        }
    }

    private static WorkflowDefinition recordsItsTrigger() {
        return WorkflowBuilder.create()
                .set("trigger", Expr.jq("{id: $workflow.input[0].id, type: $workflow.input[0].type}"),
                        new DataFlow(null, null, Expr.jq(".")))
                .schedule(Schedule.cron(io.stintflow.spi.CronExpression.parse("0 7 * * *"), java.time.ZoneId.of("UTC")))
                .build(DAILY);
    }

    @Test
    void an_occurrence_reached_by_advancing_the_clock_starts_the_definition() throws Exception {
        Fixture f = new Fixture(recordsItsTrigger());

        f.clock.advance(Duration.ofHours(6)); // 06:00: nothing due yet
        f.schedules.tick().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(f.state.creates.get()).isZero();

        f.clock.advance(Duration.ofHours(1)); // 07:00
        f.schedules.tick().toCompletableFuture().get(10, TimeUnit.SECONDS);

        String id = f.instanceFor(Instant.parse("2026-01-01T07:00:00Z"));
        InstanceSnapshot started = f.load(id);
        assertThat(started.status()).isEqualTo(InstanceStatus.COMPLETED);
        assertThat(started.context().get("type").asText()).isEqualTo(StintEvents.TYPE_SCHEDULE_FIRED);
        assertThat(started.context().get("id").asText()).isEqualTo(f.spec.name() + "@2026-01-01T07:00:00Z");
    }

    @Test
    void ca3_the_same_occurrence_delivered_twice_at_once_starts_one_instance() throws Exception {
        Fixture f = new Fixture(recordsItsTrigger());
        Instant occurrence = Instant.parse("2026-01-01T07:00:00Z");
        CyclicBarrier together = new CyclicBarrier(2);

        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> deliverTogether(f, occurrence, together));
        CompletableFuture<Void> second = CompletableFuture.runAsync(() -> deliverTogether(f, occurrence, together));
        CompletableFuture.allOf(first, second).get(10, TimeUnit.SECONDS);
        f.schedules.deliver(f.spec.name(), occurrence).toCompletableFuture().get(10, TimeUnit.SECONDS); // and later

        assertThat(f.state.creates.get()).isEqualTo(1);
        assertThat(f.load(f.instanceFor(occurrence)).status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    @Test
    void each_occurrence_starts_a_new_chain() throws Exception {
        Fixture f = new Fixture(recordsItsTrigger());
        Instant monday = Instant.parse("2026-01-05T07:00:00Z");
        Instant tuesday = Instant.parse("2026-01-06T07:00:00Z");

        f.schedules.deliver(f.spec.name(), monday).toCompletableFuture().get(10, TimeUnit.SECONDS);
        f.schedules.deliver(f.spec.name(), tuesday).toCompletableFuture().get(10, TimeUnit.SECONDS);

        String mondayChain = f.load(f.instanceFor(monday)).chainId();
        String tuesdayChain = f.load(f.instanceFor(tuesday)).chainId();
        assertThat(mondayChain).isNotEqualTo(tuesdayChain);
        CloudEvent mondayEvent = ScheduledStart.syntheticEvent(f.spec, monday);
        assertThat(mondayChain).isEqualTo("chn-" + UUID.nameUUIDFromBytes(("chain|" + mondayEvent.getSource() + "|"
                + mondayEvent.getId()).getBytes(StandardCharsets.UTF_8))); // SDD 2.5: deterministic per occurrence
    }

    @Test
    void without_a_scheduled_time_the_occurrence_is_derived_from_the_delivery_time() throws Exception {
        Fixture f = new Fixture(recordsItsTrigger());
        Instant occurrence = Instant.parse("2026-01-01T07:00:00Z");
        ScheduledStart handler = new ScheduledStart(f.registry, f.engine);

        // floci: the placeholder came back unsubstituted; the message was sent 20 s after the occurrence.
        handler.handle(new ScheduleFire(f.spec.name(), DAILY, null, occurrence.plusSeconds(20)))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        // AWS: the same occurrence delivered with its substituted time is a duplicate of it.
        handler.handle(new ScheduleFire(f.spec.name(), DAILY, occurrence, occurrence.plusSeconds(50)))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(f.state.creates.get()).isEqualTo(1);
        assertThat(f.load(f.instanceFor(occurrence)).status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    @Test
    void ca4_sdd27_a_delivery_half_a_period_or_more_after_the_occurrence_is_rejected_not_guessed() throws Exception {
        Fixture f = new Fixture(recordsItsTrigger()); // daily at 07:00: half a period is 12 h
        Instant occurrence = Instant.parse("2026-01-01T07:00:00Z");
        ScheduledStart handler = new ScheduledStart(f.registry, f.engine);

        assertThatThrownBy(() -> handler.handle(new ScheduleFire(f.spec.name(), DAILY, null,
                        occurrence.plus(Duration.ofHours(13)))).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("too far from the last occurrence");
        assertThat(f.state.creates.get()).isZero();
    }

    @Test
    void an_occurrence_of_a_schedule_no_definition_wants_here_is_not_acknowledged() throws Exception {
        Fixture f = new Fixture(recordsItsTrigger());
        ScheduledStart handler = new ScheduledStart(f.registry, f.engine);

        assertThatThrownBy(() -> handler.handle(new ScheduleFire("stint-unknown", DAILY, Instant.EPOCH, Instant.EPOCH))
                .toCompletableFuture().get(5, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("not wanted by any definition");
    }

    @Test
    void schedule_on_binds_its_events_and_starts_the_definition() throws Exception {
        WorkflowRef archive = new WorkflowRef("reports", "archive-report", "1.0.0");
        WorkflowRegistry registry = new WorkflowRegistry();
        registry.register(WorkflowBuilder.create()
                .set("archived", Expr.jq("{report: .[0].data.reportId}"), new DataFlow(null, null, Expr.jq(".")))
                .schedule(Schedule.on(List.of(EventFilter.ofType("io.acme.report.sent.v1"))))
                .build(archive));
        registry.register(scheduled(DAILY, "0 7 * * *")); // a cron schedule binds nothing
        InMemoryStateStore state = new InMemoryStateStore();
        WorkflowEngine engine = new WorkflowEngine(registry, new DomainEventFixture.CapturingTransport(), state,
                new InMemoryTimerService(InstantSource.system()),
                new FilesystemBlobStore(Files.createTempDirectory("stint-sdd24-on")));
        TriggerBindings bindings = new TriggerBindings(registry);
        InMemoryDomainEventBus bus = new InMemoryDomainEventBus();
        new DomainEventRouter(List.of(new StartReaction(bindings, engine))).subscribeTo(bus);

        assertThat(bindings.bindDeclaredSchedules()).isEqualTo(1);
        assertThat(bindings.bindDeclaredSchedules()).isZero(); // idempotent

        var data = Json.obj().put("reportId", "R-7");
        CloudEvent sent = CloudEventBuilder.v1().withId("sent-1").withSource(java.net.URI.create("https://acme.example/r"))
                .withType("io.acme.report.sent.v1").withDataContentType("application/json").withData(Json.bytes(data))
                .build();
        bus.publish(sent).toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(engine.awaitCompletion(WorkflowEngine.triggeredInstanceId(archive, sent), Duration.ofSeconds(10))
                .toCompletableFuture().get(15, TimeUnit.SECONDS).get("report").asText()).isEqualTo("R-7");
    }

    private static void deliverTogether(Fixture f, Instant occurrence, CyclicBarrier together) {
        try {
            together.await(10, TimeUnit.SECONDS);
            f.schedules.deliver(f.spec.name(), occurrence).toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Counts instance creations (an OK save at version 0) — the proof of "one instance". */
    private static final class CountingCreates implements StateStore {
        private final InMemoryStateStore delegate = new InMemoryStateStore();
        final AtomicInteger creates = new AtomicInteger();

        @Override
        public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
                List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
            return delegate.save(snapshot, expectedVersion, addWaits, consumeWaitKeys, addOutbox).thenApply(outcome -> {
                if (expectedVersion == 0 && outcome == SaveOutcome.OK) {
                    creates.incrementAndGet();
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
}
