package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.CancelOutcome;
import io.stintflow.core.InstanceCancelledException;
import io.stintflow.core.trigger.CancelReaction;
import io.stintflow.core.trigger.DomainEventReaction.Reacted;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.TaskResult;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * SDD 3a.11: an instance is cancelled from outside — {@code ABORTED}, every wait consumed in one versioned save — whatever
 * it is suspended on; what arrives afterwards is dropped; a cancel and a result racing give exactly one winner; the
 * cancel event acts only for the sources it was configured with. Barriers and the controllable clock — no sleep.
 */
class CancelInstanceTest {

    static final WorkflowRef REF = new WorkflowRef("meetings", "cancel", "1.0.0");
    static final String SOURCE = "https://coordinator.sandbox.example";
    static final String POLICY = """
            routes:
              restricted: ['*']
              confidential: ['*']
              internal: ['*']""";

    static final String REMOTE = SensitivityPolicyTest.header("cancel", POLICY) + """
            do:
              - work:
                  call: remote
                  with:
                    task: slow
                    input: ${ . }
              - after:
                  set: '${ {done: true} }'
                  export: { as: '${ $context + . }' }
            """;

    static final String RETRYING = SensitivityPolicyTest.header("cancel", POLICY) + """
            do:
              - guarded:
                  try:
                    - flaky:
                        call: remote
                        with:
                          task: flaky
                          input: ${ . }
                  catch:
                    errors:
                      with:
                        type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                    retry:
                      delay: { seconds: 30 }
                      limit:
                        attempt: { count: 3 }
            """;

    static String listening(boolean withTimeout) {
        String timeout = withTimeout ? """
                      timeout:
                        after: PT1H
                """ : "";
        return SensitivityPolicyTest.header("cancel", POLICY) + """
                do:
                  - wait:
                      listen:
                        to:
                          one:
                            with: { type: io.acme.never.v1 }
                            correlate:
                              instance: { from: '${ .data.id }', expect: '${ $workflow.id }' }
                """ + timeout;
    }

    private static String start(PolicyFixture f) {
        return f.engine.start(REF, Json.obj(), Sensitivity.INTERNAL);
    }

    private static void awaitWaiting(PolicyFixture f, String id) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until && f.state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS)
                .map(InstanceSnapshot::waitingKey).isEmpty()) {
            Thread.onSpinWait();
        }
        assertThat(f.snapshot(id).waitingKey()).isNotNull();
    }

    private static CancelOutcome cancel(PolicyFixture f, String id, String reason) throws Exception {
        return f.engine.cancel(id, reason).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    /** Of the keys named, the ones that still have a wait. */
    private static List<String> waitsLeft(PolicyFixture f, String... keys) throws Exception {
        List<String> left = new ArrayList<>();
        for (String key : keys) {
            if (f.state.findWait(key).toCompletableFuture().get(5, TimeUnit.SECONDS).isPresent()) {
                left.add(key);
            }
        }
        return left;
    }

    @Test
    void ca1_a_task_in_flight_is_cancelled_with_its_task_and_timer_waits_and_a_late_result_is_dropped() throws Exception {
        PolicyFixture f = new PolicyFixture(REMOTE);
        String id = start(f);
        awaitWaiting(f, id);
        String taskKey = f.snapshot(id).waitingKey();
        String timerKey = "timer:" + taskKey.substring("task:".length());
        assertThat(waitsLeft(f, taskKey, timerKey)).containsExactly(taskKey, timerKey);
        TaskInvocation inv = f.dispatched.get(0);

        assertThat(cancel(f, id, "the operation was revoked")).isEqualTo(CancelOutcome.CANCELLED);

        InstanceSnapshot after = f.snapshot(id);
        assertThat(after.status()).isEqualTo(InstanceStatus.ABORTED);
        assertThat(after.waitingKey()).isNull();
        assertThat(waitsLeft(f, taskKey, timerKey)).isEmpty();
        // CA2: the result of the task already sent, and the timer, arrive after the cancel
        f.answer(TaskResult.completed(inv.correlationId(), Json.obj().put("late", true)));
        f.timer.fire(timerKey);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
        assertThat(f.snapshot(id).context().has("done")).isFalse();
        // CA8: the reason is in the audit log, not in the snapshot
        assertThat(f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .filteredOn(r -> AuditRecord.CANCEL.equals(r.kind())).singleElement().satisfies(r -> {
                    assertThat(r.outcome()).isEqualTo("cancelled");
                    assertThat(r.detail()).containsEntry("reason", "the operation was revoked")
                            .containsEntry("origin", "api");
                });
        assertThat(f.snapshot(id).toString()).doesNotContain("revoked");
    }

    @Test
    void ca1_a_retry_delay_is_cancelled_and_its_timer_cannot_bring_the_instance_back() throws Exception {
        PolicyFixture f = new PolicyFixture(RETRYING);
        f.workers.put("flaky", inv -> TaskResult.failed(inv.correlationId(),
                new ErrorInfo(ErrorInfo.TYPE_COMMUNICATION, 503, "down", null, null, null)));
        String id = start(f);
        String retryKey = awaitRetryWait(f, id);

        assertThat(cancel(f, id, "stop")).isEqualTo(CancelOutcome.CANCELLED);

        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
        assertThat(waitsLeft(f, retryKey)).isEmpty();
        int dispatchedBefore = f.dispatched.size();
        f.now.set(f.now.get().plus(Duration.ofMinutes(5)));
        f.timer.tick();
        assertThat(f.dispatched).hasSize(dispatchedBefore);   // no second attempt
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
    }

    private static String awaitRetryWait(PolicyFixture f, String id) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until) {
            String key = f.snapshot(id).waitingKey();
            if (key != null && key.startsWith("retry:")) {
                return key;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("the instance never waited on a retry delay");
    }

    @Test
    void ca1_a_listen_with_and_without_a_timeout_is_cancelled_and_a_later_event_does_not_wake_it() throws Exception {
        for (boolean withTimeout : new boolean[] {false, true}) {
            PolicyFixture f = new PolicyFixture(listening(withTimeout));
            String id = start(f);
            awaitWaiting(f, id);
            String eventKey = f.snapshot(id).waitingKey();
            assertThat(eventKey).startsWith("event:");
            List<String> keys = new ArrayList<>(List.of(eventKey));
            if (withTimeout) {
                String timerKey = f.snapshot(id).listenState().path("active").path("timerKey").asText();
                assertThat(timerKey).startsWith("timer:");
                keys.add(timerKey);
            }
            assertThat(waitsLeft(f, keys.toArray(String[]::new))).containsExactlyElementsOf(keys);

            assertThat(cancel(f, id, "listen " + withTimeout)).isEqualTo(CancelOutcome.CANCELLED);

            assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
            assertThat(waitsLeft(f, keys.toArray(String[]::new))).isEmpty();
            CloudEvent event = CloudEventBuilder.v1().withId("late-1").withSource(URI.create("https://acme.example"))
                    .withType("io.acme.never.v1").withDataContentType("application/json")
                    .withData(Json.bytes(Json.obj().put("id", id))).build();
            f.engine.onDomainEvent(event, Duration.ofMinutes(1)).toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
        }
    }

    @Test
    void ca3_a_cancel_and_a_result_racing_give_exactly_one_winner_and_leave_no_wait() throws Exception {
        for (int round = 0; round < 25; round++) {
            PolicyFixture f = new PolicyFixture(REMOTE);
            String id = start(f);
            awaitWaiting(f, id);
            String taskKey = f.snapshot(id).waitingKey();
            String timerKey = "timer:" + taskKey.substring("task:".length());
            TaskInvocation inv = f.dispatched.get(0);
            CyclicBarrier go = new CyclicBarrier(2);

            CompletableFuture<CancelOutcome> cancelled = CompletableFuture.supplyAsync(() -> {
                await(go);
                return f.engine.cancel(id, "race").toCompletableFuture().join();
            });
            CompletableFuture<Void> answered = CompletableFuture.runAsync(() -> {
                await(go);
                f.answer(TaskResult.completed(inv.correlationId(), Json.obj().put("ok", true)));
            });
            CancelOutcome outcome = cancelled.get(20, TimeUnit.SECONDS);
            answered.get(20, TimeUnit.SECONDS);

            InstanceSnapshot end = awaitTerminal(f, id);
            assertThat(waitsLeft(f, taskKey, timerKey)).isEmpty();
            if (outcome == CancelOutcome.CANCELLED) {
                assertThat(end.status()).isEqualTo(InstanceStatus.ABORTED);
            } else {
                assertThat(outcome).isEqualTo(CancelOutcome.ALREADY_TERMINAL);
                assertThat(end.status()).isEqualTo(InstanceStatus.COMPLETED);
            }
        }
    }

    private static InstanceSnapshot awaitTerminal(PolicyFixture f, String id) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        InstanceSnapshot snap = f.snapshot(id);
        while (System.nanoTime() < until && snap.status() != InstanceStatus.ABORTED
                && snap.status() != InstanceStatus.COMPLETED) {
            Thread.onSpinWait();
            snap = f.snapshot(id);
        }
        return snap;
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Test
    void ca4_cancel_is_idempotent_and_knows_a_terminal_and_an_unknown_instance() throws Exception {
        PolicyFixture f = new PolicyFixture(REMOTE);
        String id = start(f);
        awaitWaiting(f, id);

        assertThat(cancel(f, id, "first")).isEqualTo(CancelOutcome.CANCELLED);
        assertThat(cancel(f, id, "second")).isEqualTo(CancelOutcome.ALREADY_TERMINAL);
        assertThat(cancel(f, "no-such-instance", "x")).isEqualTo(CancelOutcome.NOT_FOUND);

        PolicyFixture g = new PolicyFixture(REMOTE).worker("slow", in -> Json.obj().put("ok", true));
        String done = start(g);
        g.awaitCompleted(done);
        assertThat(cancel(g, done, "too late")).isEqualTo(CancelOutcome.ALREADY_TERMINAL);
        assertThat(g.snapshot(done).status()).isEqualTo(InstanceStatus.COMPLETED);
    }

    @Test
    void ca5_awaiting_a_cancelled_instance_fails_at_once_and_does_not_wait_for_the_timeout() throws Exception {
        PolicyFixture f = new PolicyFixture(REMOTE);
        String id = start(f);
        awaitWaiting(f, id);
        cancel(f, id, "stop");

        assertThatThrownBy(() -> f.engine.awaitCompletion(id, Duration.ofMinutes(30)).toCompletableFuture()
                .get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(InstanceCancelledException.class);
    }

    @Test
    void ca6_the_cancel_event_acts_only_for_a_listed_source_and_a_refusal_is_audited() throws Exception {
        PolicyFixture f = new PolicyFixture(REMOTE);
        String id = start(f);
        awaitWaiting(f, id);
        CancelReaction trusting = new CancelReaction(f.engine, Set.of(SOURCE));
        CancelReaction nobody = new CancelReaction(f.engine, Set.of());

        assertThat(react(nobody, cancelEvent("c-1", id, SOURCE, "no list"))).isEqualTo(Reacted.MATCHED);
        assertThat(react(trusting, cancelEvent("c-2", id, "https://elsewhere.example", "wrong source")))
                .isEqualTo(Reacted.MATCHED);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.WAITING);   // neither cancelled it
        assertThat(f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .filteredOn(r -> AuditRecord.CANCEL.equals(r.kind())).hasSize(2)
                .allSatisfy(r -> assertThat(r.outcome()).isEqualTo("refused"));

        assertThat(react(trusting, cancelEvent("c-3", id, SOURCE, "coordinator gave up"))).isEqualTo(Reacted.MATCHED);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.ABORTED);
        assertThat(react(trusting, cancelEvent("c-3", id, SOURCE, "redelivered"))).isEqualTo(Reacted.MATCHED);
        assertThat(f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS))
                .filteredOn(r -> "cancelled".equals(r.outcome())).singleElement()
                .satisfies(r -> assertThat(r.detail()).containsEntry("origin", "event:" + SOURCE)
                        .containsEntry("reason", "coordinator gave up"));
        assertThat(react(trusting, CloudEventBuilder.v1().withId("o").withSource(URI.create(SOURCE))
                .withType("io.acme.other.v1").build())).isEqualTo(Reacted.IGNORED);
        assertThatThrownBy(() -> new CancelReaction(f.engine, null)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ca7_the_cancel_type_is_reserved_so_no_emit_and_no_listen_can_use_it() {
        assertThat(StintEvents.isReservedType(StintEvents.TYPE_INSTANCE_CANCEL)).isTrue();
        String emitting = SensitivityPolicyTest.header("cancel", POLICY) + """
                do:
                  - say:
                      emit:
                        event:
                          with:
                            type: io.stintflow.instance.cancel.v1
                            source: https://acme.example
                """;
        assertThatThrownBy(() -> PolicyFixture.load(emitting)).hasMessageContaining("io.stintflow.instance.");
        String listeningOnIt = SensitivityPolicyTest.header("cancel", POLICY) + """
                do:
                  - wait:
                      listen:
                        to:
                          one:
                            with: { type: io.stintflow.instance.cancel.v1 }
                            correlate:
                              instance: { from: '${ .subject }', expect: '${ $workflow.id }' }
                """;
        assertThatThrownBy(() -> new PolicyFixture(listeningOnIt)).hasMessageContaining("io.stintflow.instance.");
        assertThatThrownBy(() -> new io.stintflow.core.trigger.TriggerBinding("b", io.stintflow.core.trigger.EventFilter
                .ofType(StintEvents.TYPE_INSTANCE_CANCEL), REF)).hasMessageContaining("instance-control");
    }

    private static Reacted react(CancelReaction reaction, CloudEvent event) throws Exception {
        return reaction.react(event).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static CloudEvent cancelEvent(String eventId, String instanceId, String source, String reason) {
        return CloudEventBuilder.v1().withId(eventId).withSource(URI.create(source))
                .withType(StintEvents.TYPE_INSTANCE_CANCEL).withSubject(instanceId)
                .withDataContentType("application/json").withData(Json.bytes(Json.obj().put("reason", reason))).build();
    }
}
