package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.schedule.ScheduleReconciler;
import io.stintflow.core.schedule.ScheduledStart;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.StartReaction;
import io.stintflow.core.trigger.TriggerBindings;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.example.MailGateway;
import io.stintflow.example.SendEmailHandler;
import io.stintflow.example.SendReport;
import io.stintflow.example.archive.ArchiveRecordHandler;
import io.stintflow.example.archive.ArchiveReport;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryDomainEventBus;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryScheduleService;
import io.stintflow.inmemory.InMemoryStateStore;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.ScheduleSpec;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;

/**
 * SDD 2.6, the milestone in memory (RF4): send-report on its cron, archive-report reacting to its fact on a
 * <em>separate</em> engine whose registry does not know send-report (sec. 8c-3) — the two meet only on the
 * domain bus.
 * <ul>
 *   <li>CA1: an accelerated cron occurrence (controllable clock + {@code tick()}) runs send-report — the mail
 *       goes to the defaults of its {@code prepare} task (sec. 8f) and {@code report-sent} is published;</li>
 *   <li>CA2: archive-report starts from that fact, on the same chain, caused by the fact;</li>
 *   <li>duplicated simultaneous cron deliveries start one send-report;</li>
 *   <li>two reports in parallel keep their own mails and chains;</li>
 *   <li>sec. 8e: the engine dies between the checkpoint and the publication — another engine's outbox sweep
 *       still gets the fact to archive-report, with the same id and chain.</li>
 * </ul>
 */
class SendReportMilestoneLocalTest {

    /** 2026-01-01 is a Thursday; send-report runs at 09:00 UTC on Mondays. */
    private static final Instant THURSDAY = Instant.parse("2026-01-01T00:00:00Z");
    private static final Instant MONDAY_NINE = Instant.parse("2026-01-05T09:00:00Z");

    @Test
    void ca1_an_accelerated_cron_occurrence_runs_send_report_and_ca2_archive_report_reacts_on_the_same_chain()
            throws Exception {
        Milestone m = new Milestone();

        m.clock.set(MONDAY_NINE.minusSeconds(60));
        m.schedules.tick().toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(m.mail.sent).isEmpty(); // 08:59 on Monday: not yet

        m.clock.set(MONDAY_NINE);
        m.schedules.tick().toCompletableFuture().get(10, TimeUnit.SECONDS);

        String sendId = m.sendInstanceFor(MONDAY_NINE);
        m.send.awaitCompletion(sendId, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(m.mail.sent).singleElement().satisfies(mail -> {
            assertThat(mail.to()).isEqualTo("reports@stintflow.io"); // prepare's defaults: a cron run has no input
            assertThat(mail.subject()).isEqualTo("Weekly report");
        });
        CloudEvent fact = m.facts.get(0);
        assertThat(fact.getType()).isEqualTo(ArchiveReport.REPORT_SENT_TYPE);

        // CA2: archive-report, on its own engine, started from the fact — same chain, caused by the fact.
        String archiveId = WorkflowEngine.triggeredInstanceId(ArchiveReport.REF, fact);
        JsonNode archived = m.archive.awaitCompletion(archiveId, Duration.ofSeconds(10)).toCompletableFuture()
                .get(15, TimeUnit.SECONDS);
        String sendChain = load(m.sendState, sendId).chainId();
        assertThat(load(m.archiveState, archiveId).chainId()).isEqualTo(sendChain);
        assertThat(fact.getExtension(StintEvents.EXT_CHAIN_ID)).isEqualTo(sendChain);
        assertThat(archived.get("chainId").asText()).isEqualTo(sendChain); // in the archived record too
        assertThat(archived.get("factId").asText()).isEqualTo(fact.getId());
        assertThat(m.archiveInvokes).singleElement().satisfies(invoke ->
                assertThat(invoke.getExtension(StintEvents.EXT_CAUSATION_ID)).isEqualTo(fact.getId()));
        assertThat(m.archiveRegistry.find(SendReport.REF)).isEmpty(); // archive's engine never knew send-report
    }

    @Test
    void two_simultaneous_deliveries_of_the_same_cron_occurrence_start_one_send_report() throws Exception {
        Milestone m = new Milestone();
        CyclicBarrier together = new CyclicBarrier(2);

        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> deliverTogether(m, together));
        CompletableFuture<Void> second = CompletableFuture.runAsync(() -> deliverTogether(m, together));
        CompletableFuture.allOf(first, second).get(15, TimeUnit.SECONDS);
        m.send.awaitCompletion(m.sendInstanceFor(MONDAY_NINE), Duration.ofSeconds(10)).toCompletableFuture()
                .get(15, TimeUnit.SECONDS);

        assertThat(m.sendCreates.get()).isEqualTo(1);
        assertThat(m.mail.sent).hasSize(1);
    }

    @Test
    void two_reports_in_parallel_keep_their_own_mails_and_chains() throws Exception {
        Milestone m = new Milestone();
        CyclicBarrier together = new CyclicBarrier(2);

        CompletableFuture<String> a = CompletableFuture.supplyAsync(() -> startTogether(m, together, "a@acme.example", "Report A"));
        CompletableFuture<String> b = CompletableFuture.supplyAsync(() -> startTogether(m, together, "b@acme.example", "Report B"));
        String sendA = a.get(15, TimeUnit.SECONDS);
        String sendB = b.get(15, TimeUnit.SECONDS);
        m.send.awaitCompletion(sendA, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        m.send.awaitCompletion(sendB, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);

        Map<String, String> subjectByRecipient = new ConcurrentHashMap<>();
        m.mail.sent.forEach(mail -> subjectByRecipient.put(mail.to(), mail.subject()));
        assertThat(subjectByRecipient).containsExactlyInAnyOrderEntriesOf(
                Map.of("a@acme.example", "Report A", "b@acme.example", "Report B"));

        assertThat(m.facts).hasSize(2);
        for (CloudEvent fact : m.facts) {
            String recipient = Json.read(fact.getData().toBytes()).get("recipient").asText();
            String sendId = recipient.startsWith("a@") ? sendA : sendB;
            String archiveId = WorkflowEngine.triggeredInstanceId(ArchiveReport.REF, fact);
            m.archive.awaitCompletion(archiveId, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertThat(load(m.archiveState, archiveId).chainId()).isEqualTo(load(m.sendState, sendId).chainId());
        }
        assertThat(load(m.sendState, sendA).chainId()).isNotEqualTo(load(m.sendState, sendB).chainId());
    }

    @Test
    void the_fact_reaches_archive_report_even_if_the_engine_dies_between_checkpoint_and_publication() throws Exception {
        Milestone m = new Milestone();
        // Engine "A" shares send-report's state store, but its publisher never completes: the process is gone
        // right after the checkpoint that recorded the fact in the outbox.
        WorkflowEngine dying = m.sendEngine(new NeverPublishes(), m.clock);
        ObjectNode input = Json.obj().put("recipient", "chaos@acme.example").put("subject", "Chaos").put("body", "!");
        String sendId = dying.start(SendReport.REF, input);
        dying.awaitCompletion(sendId, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(m.facts).isEmpty();
        assertThat(m.sendState.pendingOutbox(Instant.MAX.minusSeconds(1), 10).toCompletableFuture().get()).hasSize(1);

        // Engine "B" (healthy publisher, clock past the 30 s grace) sweeps it.
        AtomicReference<Instant> later = new AtomicReference<>(m.clock.instant().plusSeconds(31));
        WorkflowEngine survivor = m.sendEngine(m.publisher, later::get);
        assertThat(survivor.outboxRelay().sweep().toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo(1);

        CloudEvent fact = m.facts.get(0);
        String archiveId = WorkflowEngine.triggeredInstanceId(ArchiveReport.REF, fact);
        m.archive.awaitCompletion(archiveId, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
        assertThat(load(m.archiveState, archiveId).chainId()).isEqualTo(load(m.sendState, sendId).chainId());
        assertThat(m.sendState.pendingOutbox(Instant.MAX.minusSeconds(1), 10).toCompletableFuture().get()).isEmpty();
    }

    private static void deliverTogether(Milestone m, CyclicBarrier together) {
        try {
            together.await(10, TimeUnit.SECONDS);
            m.schedules.deliver(m.spec.name(), MONDAY_NINE).toCompletableFuture().get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String startTogether(Milestone m, CyclicBarrier together, String to, String subject) {
        try {
            together.await(10, TimeUnit.SECONDS);
            return m.send.start(SendReport.REF, Json.obj().put("recipient", to).put("subject", subject).put("body", "-"));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static InstanceSnapshot load(StateStore state, String id) throws Exception {
        return state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
    }

    static WorkflowDefinition loadYaml(String resource) {
        try (InputStream in = SendReportMilestoneLocalTest.class.getClassLoader().getResourceAsStream(resource)) {
            return new DslLoader().load(in, resource, LoadOptions.STRICT);
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** Two engines that meet only on {@link #bus}: send-report's and archive-report's. */
    private static final class Milestone {
        final MutableClock clock = new MutableClock(THURSDAY);
        final InMemoryDomainEventBus bus = new InMemoryDomainEventBus();
        final List<CloudEvent> facts = new CopyOnWriteArrayList<>();
        final InMemoryEventPublisher publisher = new InMemoryEventPublisher();
        final CapturingMail mail = new CapturingMail();
        final AtomicInteger sendCreates = new AtomicInteger();
        final InMemoryStateStore sendState = new InMemoryStateStore();
        final WorkflowRegistry sendRegistry = new WorkflowRegistry();
        final InMemoryScheduleService schedules = new InMemoryScheduleService(clock);
        final WorkflowEngine send;
        final ScheduleSpec spec;

        final InMemoryStateStore archiveState = new InMemoryStateStore();
        final WorkflowRegistry archiveRegistry = new WorkflowRegistry();
        final List<CloudEvent> archiveInvokes = new CopyOnWriteArrayList<>();
        final WorkflowEngine archive;

        Milestone() throws Exception {
            publisher.subscribe(fact -> {
                facts.add(fact);
                return bus.publish(fact);
            });

            sendRegistry.register(loadYaml("stint/workflows/send-report.yaml"));
            send = sendEngine(publisher, clock);
            schedules.onFire(new ScheduledStart(sendRegistry, send));
            new ScheduleReconciler(sendRegistry, schedules).reconcile().toCompletableFuture().get();
            spec = sendRegistry.scheduleSpecs().get(0);

            archiveRegistry.register(loadYaml("stint/workflows/archive-report.yaml"));
            BlobStore archiveBlobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd26-archive"));
            WorkerRuntime archiveWorker = new WorkerRuntime(new TaskHandlerRegistry()
                    .register(ArchiveReport.ROUTE_ARCHIVE_RECORD, new ArchiveRecordHandler(archiveBlobs)), archiveBlobs);
            InMemoryTaskTransport archiveTransport = new InMemoryTaskTransport();
            archiveTransport.connectWorker(invoke -> {
                archiveInvokes.add(invoke);
                return archiveWorker.handle(invoke);
            });
            archive = new WorkflowEngine(archiveRegistry, archiveTransport, archiveState,
                    new InMemoryTimerService(clock), archiveBlobs);
            TriggerBindings bindings = new TriggerBindings(archiveRegistry);
            bindings.bindDeclaredSchedules();
            new DomainEventRouter(List.of(new StartReaction(bindings, archive))).subscribeTo(bus);
        }

        /** A send-report engine on the shared send-report state store, with its own worker. */
        WorkflowEngine sendEngine(EventPublisher facts, InstantSource engineClock) throws Exception {
            BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd26-send"));
            WorkerRuntime worker = new WorkerRuntime(new TaskHandlerRegistry()
                    .register(SendReport.ROUTE_SEND_EMAIL, new SendEmailHandler(mail)), blobs);
            InMemoryTaskTransport transport = new InMemoryTaskTransport();
            transport.connectWorker(worker::handle);
            return new WorkflowEngine(sendRegistry, transport, new CountingCreates(sendState, sendCreates),
                    new InMemoryTimerService(engineClock), blobs, new TreeInterpreter(new JqExpressionEvaluator()),
                    engineClock, facts, null);
        }

        String sendInstanceFor(Instant occurrence) {
            return WorkflowEngine.triggeredInstanceId(SendReport.REF, ScheduledStart.syntheticEvent(spec, occurrence));
        }
    }

    record Mail(String to, String subject, String body) {
    }

    /** Captures mails (local stand-in for SMTP; the floci IT uses Mailpit). */
    static final class CapturingMail implements MailGateway {
        final List<Mail> sent = new CopyOnWriteArrayList<>();
        private final AtomicInteger ids = new AtomicInteger();

        @Override
        public CompletionStage<String> send(String to, String subject, String body) {
            sent.add(new Mail(to, subject, body));
            return CompletableFuture.completedFuture("msg-" + ids.incrementAndGet());
        }
    }

    /** The publisher of a process that died right after its checkpoint: nothing ever completes. */
    private static final class NeverPublishes implements EventPublisher {
        @Override
        public CompletionStage<Void> publish(CloudEvent event) {
            return new CompletableFuture<>();
        }

        @Override
        public AdapterCapabilities capabilities() {
            return new InMemoryEventPublisher().capabilities();
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

        void set(Instant instant) {
            now.set(instant);
        }
    }
}
