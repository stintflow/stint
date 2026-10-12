package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeEventPublisher;
import io.stintflow.aws.EventBridgeSchedulerService;
import io.stintflow.aws.EventBridgeSchedulerTimerService;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.aws.SqsDomainEventSource;
import io.stintflow.aws.SqsDelayTimerService;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.core.DelayRoutingTimerService;
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
import io.stintflow.example.SendEmailHandler;
import io.stintflow.example.SendReport;
import io.stintflow.example.archive.ArchiveRecordHandler;
import io.stintflow.example.archive.ArchiveReport;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryTaskTransport;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.StateStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.Target;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.6, CA3: the milestone on floci, with mail captured by Mailpit. What runs real and what doesn't (sec. 8a):
 * <table>
 *   <caption>Services in this IT</caption>
 *   <tr><th>Service</th><th>Here</th></tr>
 *   <tr><td>SQS</td><td>real — task invoke/result of both workflows, archive-report's inbound domain queue, the
 *       schedule queue</td></tr>
 *   <tr><td>EventBridge</td><td>real — domain bus {@code stint-domain}, rule on the report-sent type, SQS target</td></tr>
 *   <tr><td>EventBridge Scheduler</td><td>real — the schedule is reconciled and fires for real. Its scheduled
 *       instant comes from the SDD 2.4 {@code SentTimestamp} fallback, enabled explicitly here (SDD 2.7, off by
 *       default): floci doesn't substitute {@code <aws.scheduler.scheduled-time>}</td></tr>
 *   <tr><td>DynamoDB</td><td>real — state, waits and outbox of both workflows (separate tables per workflow)</td></tr>
 *   <tr><td>S3</td><td>real — archive records ({@link S3BlobStore}) and claim-check store</td></tr>
 *   <tr><td>SNS</td><td>not used (the alternative publisher is proven in {@code FlociEmitIT})</td></tr>
 *   <tr><td>e-mail</td><td>real SMTP to a Mailpit container, through the test-only {@link SmtpMailGateway}</td></tr>
 *   <tr><td>TimerService</td><td>real (SDD 2.7) — {@code DelayRoutingTimerService}: SQS {@code DelaySeconds} up to
 *       15 min, EventBridge Scheduler one-time schedules beyond, one timer queue per engine</td></tr>
 *   <tr><td>outbox sweep clock</td><td>controllable, only in the crash scenario, to pass the 30 s grace without
 *       sleeping</td></tr>
 * </table>
 * <ol>
 *   <li>CA1: send-report's own YAML, with only its cron line changed to {@code * * * * *}, is reconciled into the real
 *       Scheduler. Its occurrence runs it: the mail lands in Mailpit and report-sent goes out on the domain bus.</li>
 *   <li>CA2: archive-report runs on its own engine, whose registry doesn't know send-report. It starts from the
 *       fact (EventBridge → SQS), on the same chain, and its S3 record carries that chain.</li>
 *   <li>Two reports started in parallel keep their own mails (Mailpit, per recipient) and chains.</li>
 *   <li>Sec. 8e: an engine dies between the checkpoint and the publication. Another engine sweeps the DynamoDB
 *       outbox, and archive-report still starts on the same chain.</li>
 * </ol>
 * No sleeps: every wait is on an observed creation, a completion or a single read after a completion.
 */
class FlociSendReportMilestoneIT {

    private static final String ROLE = "arn:aws:iam::000000000000:role/stint-scheduler";
    private static final String DEFAULT_RECIPIENT = "reports@stintflow.io";

    @Test
    void send_report_on_its_cron_and_archive_report_on_its_fact_with_mail_captured() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566);
             GenericContainer<?> mailpit = new GenericContainer<>(DockerImageName.parse("axllent/mailpit:latest"))
                     .withExposedPorts(1025, 8025)) {
            floci.start();
            mailpit.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            SmtpMailGateway smtp = new SmtpMailGateway(mailpit.getHost(), mailpit.getMappedPort(1025));
            Mailpit inbox = new Mailpit(URI.create("http://" + mailpit.getHost() + ":" + mailpit.getMappedPort(8025)));

            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 EventBridgeClient events = EventBridgeClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1).forcePathStyle(true)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 SchedulerClient scheduler = SchedulerClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                // --- infrastructure ---------------------------------------------------------------------------
                String sendInvoke = queue(sqs, "send-invoke");
                String sendResult = queue(sqs, "send-result");
                String chaosInvoke = queue(sqs, "chaos-invoke");
                String chaosResult = queue(sqs, "chaos-result");
                String archiveInvoke = queue(sqs, "archive-invoke");
                String archiveResult = queue(sqs, "archive-result");
                String archiveInbound = queue(sqs, "archive-domain-events");
                String scheduleQueue = queue(sqs, "stint-schedules");
                events.createEventBus(b -> b.name("stint-domain"));
                events.putRule(r -> r.name("report-sent-to-archive").eventBusName("stint-domain")
                        .eventPattern("{\"detail-type\":[\"" + ArchiveReport.REPORT_SENT_TYPE + "\"]}"));
                events.putTargets(t -> t.rule("report-sent-to-archive").eventBusName("stint-domain").targets(
                        Target.builder().id("archive").arn(queueArn(sqs, archiveInbound)).inputPath("$.detail").build()));
                DynamoDbTestTables.create(ddb, "send-instances", "send-waits");
                DynamoDbTestTables.createOutbox(ddb, "send-outbox");
                DynamoDbTestTables.create(ddb, "archive-instances", "archive-waits");
                s3.createBucket(b -> b.bucket("stint-blobs"));
                s3.createBucket(b -> b.bucket("stint-archive"));
                BlobStore blobs = new S3BlobStore(s3, "stint-blobs");
                BlobStore archiveRecords = new S3BlobStore(s3, "stint-archive");
                EventPublisher domainBus = new EventBridgeEventPublisher(events, "stint-domain", "default");

                // --- archive-report: its own engine, registry, tables and worker --------------------------------
                Creations archiveCreated = new Creations();
                StateStore archiveState = new CountingCreates(
                        new DynamoDbStateStore(ddb, "archive-instances", "archive-waits"), new AtomicInteger(), archiveCreated);
                archiveCreated.store = archiveState;
                WorkflowRegistry archiveRegistry = new WorkflowRegistry();
                archiveRegistry.register(loadYaml("stint/workflows/archive-report.yaml", text -> text));
                WorkflowEngine archive = new WorkflowEngine(archiveRegistry,
                        new SqsTaskTransport(sqs, archiveInvoke, archiveResult), archiveState,
                        realTimers(sqs, scheduler, queue(sqs, "archive-timers"), InstantSource.system()), blobs);
                TriggerBindings bindings = new TriggerBindings(archiveRegistry);
                bindings.bindDeclaredSchedules();
                new DomainEventRouter(List.of(new StartReaction(bindings, archive)))
                        .subscribeTo(new SqsDomainEventSource(sqs, archiveInbound));
                SqsWorkerRunner archiveWorker = new SqsWorkerRunner(sqs, archiveInvoke, archiveResult,
                        new WorkerRuntime(new TaskHandlerRegistry().register(ArchiveReport.ROUTE_ARCHIVE_RECORD,
                                new ArchiveRecordHandler(archiveRecords)), blobs));

                // --- send-report: the real YAML with only its cron accelerated (sec. 8b) ------------------------
                Creations sendCreated = new Creations();
                StateStore sendState = new CountingCreates(
                        new DynamoDbStateStore(ddb, "send-instances", "send-waits", "send-outbox", 4),
                        new AtomicInteger(), sendCreated);
                sendCreated.store = sendState;
                WorkflowRegistry sendRegistry = new WorkflowRegistry();
                sendRegistry.register(loadYaml("stint/workflows/send-report.yaml", text -> {
                    String accelerated = text.replace("cron: '0 9 * * MON'", "cron: '* * * * *'");
                    assertThat(accelerated).isNotEqualTo(text); // only the schedule line differs
                    return accelerated;
                }));
                WorkflowEngine send = new WorkflowEngine(sendRegistry, new SqsTaskTransport(sqs, sendInvoke, sendResult),
                        sendState, realTimers(sqs, scheduler, queue(sqs, "send-timers"), InstantSource.system()), blobs,
                        new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), domainBus, null);
                SqsWorkerRunner sendWorker = new SqsWorkerRunner(sqs, sendInvoke, sendResult, mailWorker(smtp, blobs));
                EventBridgeSchedulerService schedules = new EventBridgeSchedulerService(scheduler, sqs, "stint-schedules",
                        ROLE, queueArn(sqs, scheduleQueue), scheduleQueue, true); // floci: fallback enabled explicitly
                new ScheduleReconciler(sendRegistry, schedules).reconcile().toCompletableFuture().get(30, TimeUnit.SECONDS);
                String scheduleName = sendRegistry.scheduleSpecs().get(0).name();

                try (archiveWorker; sendWorker) {
                    // 1) CA1: the real cron occurrence runs send-report.
                    CompletableFuture<InstanceSnapshot> cronRun = sendCreated.await(s -> true);
                    schedules.onFire(new ScheduledStart(sendRegistry, send));
                    InstanceSnapshot cronStarted = cronRun.get(3, TimeUnit.MINUTES);
                    schedules.remove(scheduleName).toCompletableFuture().get(30, TimeUnit.SECONDS); // one run is enough
                    assertThat(cronStarted.input().get(0).get("type").asText()).isEqualTo("io.stintflow.schedule.fired.v1");
                    send.awaitCompletion(cronStarted.instanceId(), Duration.ofSeconds(60)).toCompletableFuture()
                            .get(90, TimeUnit.SECONDS);
                    assertThat(inbox.subjectsTo(DEFAULT_RECIPIENT)).containsExactly("Weekly report");

                    // 2) CA2: archive-report reacts to the fact, on the same chain, recorded in S3.
                    String cronChain = cronStarted.chainId();
                    InstanceSnapshot archived = awaitArchived(archive, archiveCreated, cronChain);
                    JsonNode fact = archived.input().get(0);
                    assertThat(fact.get("type").asText()).isEqualTo(ArchiveReport.REPORT_SENT_TYPE);
                    JsonNode record = Json.read(archiveRecords.get(URI.create(archived.context().get("archivedAt").asText()))
                            .toCompletableFuture().get(10, TimeUnit.SECONDS));
                    assertThat(record.get("chainId").asText()).isEqualTo(cronChain);
                    assertThat(record.get("factId").asText()).isEqualTo(fact.get("id").asText());
                    assertThat(record.get("report").get("recipient").asText()).isEqualTo(DEFAULT_RECIPIENT);
                    assertThat(archiveRegistry.find(SendReport.REF)).isEmpty();

                    // 3) Two reports in parallel: their own mails and chains.
                    CyclicBarrier together = new CyclicBarrier(2);
                    CompletableFuture<String> a = CompletableFuture.supplyAsync(() -> startTogether(send, together, "a@acme.example", "Report A"));
                    CompletableFuture<String> b = CompletableFuture.supplyAsync(() -> startTogether(send, together, "b@acme.example", "Report B"));
                    String sendA = a.get(30, TimeUnit.SECONDS);
                    String sendB = b.get(30, TimeUnit.SECONDS);
                    send.awaitCompletion(sendA, Duration.ofSeconds(60)).toCompletableFuture().get(90, TimeUnit.SECONDS);
                    send.awaitCompletion(sendB, Duration.ofSeconds(60)).toCompletableFuture().get(90, TimeUnit.SECONDS);
                    assertThat(inbox.subjectsTo("a@acme.example")).containsExactly("Report A");
                    assertThat(inbox.subjectsTo("b@acme.example")).containsExactly("Report B");
                    String chainA = load(sendState, sendA).chainId();
                    String chainB = load(sendState, sendB).chainId();
                    assertThat(chainA).isNotEqualTo(chainB);
                    assertThat(awaitArchived(archive, archiveCreated, chainA).input().get(0).get("data").get("recipient")
                            .asText()).isEqualTo("a@acme.example");
                    assertThat(awaitArchived(archive, archiveCreated, chainB).input().get(0).get("data").get("recipient")
                            .asText()).isEqualTo("b@acme.example");

                    // 4) Sec. 8e: an engine dies between the checkpoint and the publication.
                    WorkflowEngine dying = new WorkflowEngine(sendRegistry, new SqsTaskTransport(sqs, chaosInvoke, chaosResult),
                            sendState, realTimers(sqs, scheduler, queue(sqs, "chaos-timers"), InstantSource.system()),
                            blobs, new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(),
                            new NeverPublishes(), null);
                    try (SqsWorkerRunner chaosWorker = new SqsWorkerRunner(sqs, chaosInvoke, chaosResult, mailWorker(smtp, blobs))) {
                        String chaosId = dying.start(SendReport.REF,
                                Json.obj().put("recipient", "chaos@acme.example").put("subject", "Chaos").put("body", "!"));
                        dying.awaitCompletion(chaosId, Duration.ofSeconds(60)).toCompletableFuture().get(90, TimeUnit.SECONDS);
                        List<OutboxEntry> stranded = sendState.pendingOutbox(Instant.now().plusSeconds(60), 100)
                                .toCompletableFuture().get(10, TimeUnit.SECONDS).stream()
                                .filter(e -> e.instanceId().equals(chaosId)).toList();
                        assertThat(stranded).hasSize(1);

                        InstantSource pastGrace = () -> Instant.now().plusSeconds(31);
                        WorkflowEngine survivor = new WorkflowEngine(sendRegistry, new InMemoryTaskTransport(), sendState,
                                realTimers(sqs, scheduler, queue(sqs, "survivor-timers"), pastGrace), blobs,
                                new TreeInterpreter(new JqExpressionEvaluator()),
                                pastGrace, domainBus, null);
                        assertThat(survivor.outboxRelay().sweep().toCompletableFuture().get(30, TimeUnit.SECONDS))
                                .isGreaterThanOrEqualTo(1);

                        String chaosChain = load(sendState, chaosId).chainId();
                        InstanceSnapshot rescued = awaitArchived(archive, archiveCreated, chaosChain);
                        assertThat(rescued.input().get(0).get("id").asText()).isEqualTo(stranded.get(0).eventId()); // same id
                        assertThat(sendState.pendingOutbox(Instant.now().plusSeconds(60), 100).toCompletableFuture()
                                .get(10, TimeUnit.SECONDS)).noneMatch(e -> e.instanceId().equals(chaosId));
                    }
                }
            }
        }
    }

    /** SDD 2.7: real timers — SQS up to 15 min, EventBridge Scheduler beyond — on the engine's own timer queue. */
    static DelayRoutingTimerService realTimers(SqsClient sqs, SchedulerClient scheduler, String timerQueue,
            InstantSource clock) {
        return new DelayRoutingTimerService(new SqsDelayTimerService(sqs, timerQueue),
                new EventBridgeSchedulerTimerService(scheduler, "stint-timers", ROLE, queueArn(sqs, timerQueue),
                        Duration.ofDays(365)), clock);
    }

    private static WorkerRuntime mailWorker(SmtpMailGateway smtp, BlobStore blobs) {
        return new WorkerRuntime(new TaskHandlerRegistry().register(SendReport.ROUTE_SEND_EMAIL, new SendEmailHandler(smtp)),
                blobs);
    }

    /** Waits for archive-report's instance on {@code chain} to be created, then completed; returns its final state. */
    private static InstanceSnapshot awaitArchived(WorkflowEngine archive, Creations created, String chain) throws Exception {
        InstanceSnapshot started = created.await(s -> chain.equals(s.chainId())).get(2, TimeUnit.MINUTES);
        archive.awaitCompletion(started.instanceId(), Duration.ofSeconds(60)).toCompletableFuture().get(90, TimeUnit.SECONDS);
        InstanceSnapshot done = created.store.load(started.instanceId()).toCompletableFuture().get(10, TimeUnit.SECONDS)
                .orElseThrow();
        assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
        return done;
    }

    private static String startTogether(WorkflowEngine send, CyclicBarrier together, String to, String subject) {
        try {
            together.await(10, TimeUnit.SECONDS);
            return send.start(SendReport.REF, Json.obj().put("recipient", to).put("subject", subject).put("body", "-"));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static InstanceSnapshot load(StateStore state, String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private static WorkflowDefinition loadYaml(String resource, java.util.function.UnaryOperator<String> edit) {
        try (InputStream in = FlociSendReportMilestoneIT.class.getClassLoader().getResourceAsStream(resource)) {
            String text = edit.apply(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            return new DslLoader().load(new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8)), resource,
                    LoadOptions.STRICT);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static String queue(SqsClient sqs, String name) {
        return sqs.createQueue(q -> q.queueName(name)).queueUrl();
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    /** Instance creations seen by a {@link CountingCreates}, and futures for the ones a test waits for. */
    private static final class Creations implements java.util.function.Consumer<InstanceSnapshot> {
        private final List<InstanceSnapshot> seen = new CopyOnWriteArrayList<>();
        private final List<Map.Entry<Predicate<InstanceSnapshot>, CompletableFuture<InstanceSnapshot>>> waiting =
                new CopyOnWriteArrayList<>();
        StateStore store;

        @Override
        public synchronized void accept(InstanceSnapshot snapshot) {
            seen.add(snapshot);
            waiting.stream().filter(w -> w.getKey().test(snapshot)).forEach(w -> w.getValue().complete(snapshot));
        }

        synchronized CompletableFuture<InstanceSnapshot> await(Predicate<InstanceSnapshot> which) {
            CompletableFuture<InstanceSnapshot> future = new CompletableFuture<>();
            seen.stream().filter(which).findFirst().ifPresentOrElse(future::complete,
                    () -> waiting.add(Map.entry(which, future)));
            return future;
        }
    }

    /** Mailpit's HTTP API: the subjects of the messages addressed to someone. */
    private record Mailpit(URI base) {
        List<String> subjectsTo(String address) throws IOException, InterruptedException {
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(base.resolve("/api/v1/messages?limit=500")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            List<String> subjects = new ArrayList<>();
            for (JsonNode message : Json.read(response.body().getBytes(StandardCharsets.UTF_8)).path("messages")) {
                for (JsonNode to : message.path("To")) {
                    if (address.equalsIgnoreCase(to.path("Address").asText())) {
                        subjects.add(message.path("Subject").asText());
                    }
                }
            }
            return subjects;
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
}
