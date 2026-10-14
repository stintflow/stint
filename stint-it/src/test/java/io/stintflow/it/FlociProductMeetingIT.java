package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.EventBridgeSchedulerTimerService;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.aws.SqsDelayTimerService;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.core.DelayRoutingTimerService;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.example.meeting.Corpus;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.example.meeting.ProductMeetingPipeline;
import io.stintflow.example.meeting.Report;
import io.stintflow.example.meeting.ReviewChannel;
import io.stintflow.example.meeting.RunMetrics;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.publishing.FakeConfluenceServer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 3a.6, CA1 on floci (dummy {@code test}/{@code test} credentials): the milestone with the engine on SQS,
 * DynamoDB, S3 (SSE) and the timers of SDD 2.4, and its workers behind the SQS worker binding (SDD 3a.7) — fake
 * models, a fake Confluence. A plain meeting and the one whose decision goes to review both end with published minutes,
 * in one chain each; the raw audio rests encrypted under its retention class; the report is built.
 */
class FlociProductMeetingIT {

    private static final String ROLE = "arn:aws:iam::000000000000:role/stint-scheduler";

    @Test
    void the_milestone_runs_on_sqs_dynamodb_and_s3_with_the_worker_binding() throws Exception {
        List<Meeting> corpus = Corpus.load();
        List<Meeting> meetings = corpus.stream().filter(m -> m.id().equals("m-001") || m.id().equals("m-008"))
                .toList();
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .forcePathStyle(true).credentialsProvider(creds)
                         .httpClient(UrlConnectionHttpClient.create()).build();
                 SchedulerClient scheduler = SchedulerClient.builder().endpointOverride(endpoint)
                         .region(Region.US_EAST_1).credentialsProvider(creds)
                         .httpClient(UrlConnectionHttpClient.create()).build();
                 FakeConfluenceServer confluence = new FakeConfluenceServer(ProductMeetingPipeline.CONFLUENCE_EMAIL,
                         ProductMeetingPipeline.CONFLUENCE_TOKEN, ProductMeetingPipeline.SPACE);
                 ReviewChannel channel = new ReviewChannel(URI.create(ProductMeetingPipeline.REVIEW_SOURCE))) {

                String invoke = sqs.createQueue(q -> q.queueName("pm-invoke")).queueUrl();
                String result = sqs.createQueue(q -> q.queueName("pm-result")).queueUrl();
                String timers = sqs.createQueue(q -> q.queueName("stint-timers")).queueUrl();
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createOutbox(ddb, "stint-outbox");
                DynamoDbTestTables.createInbox(ddb, "stint-inbox");
                s3.createBucket(b -> b.bucket("stint-blobs"));
                S3BlobStore blobs = new S3BlobStore(s3, "stint-blobs");

                RunMetrics metrics = new RunMetrics(Map.of(), false);
                OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(metrics)).build().get("stint-it-3a6"));
                ProductMeetingPipeline.Stack stack = ProductMeetingPipeline.fakes(corpus);
                WorkerRuntime runtime = new WorkerRuntime(ProductMeetingPipeline.handlers(corpus, stack, metrics,
                        blobs, tracer, confluence.baseUri()), blobs, tracer);

                DelayRoutingTimerService timerService = new DelayRoutingTimerService(
                        new SqsDelayTimerService(sqs, timers),
                        new EventBridgeSchedulerTimerService(scheduler, "stint-timers", ROLE,
                                queueArn(sqs, timers), Duration.ofDays(365)), InstantSource.system());
                WorkflowRegistry registry = new WorkflowRegistry(timerService.maxDelay()).requirePolicy();
                WorkflowDefinition def;
                try (var yaml = ProductMeetingPipeline.class.getClassLoader()
                        .getResourceAsStream("stint/workflows/product-meeting.yaml")) {
                    def = new DslLoader().load(yaml, "product-meeting.yaml", LoadOptions.STRICT);
                }
                registry.register(def);
                StateStore state = new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4,
                        "stint-inbox");
                InMemoryAuditLog audit = new InMemoryAuditLog();
                WorkflowEngine engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invoke, result), state,
                        timerService, blobs, new TreeInterpreter(new JqExpressionEvaluator()),
                        InstantSource.system(), channel, null, tracer).auditLog(audit);
                channel.answersTo(engine);

                try (SqsWorkerBinding binding = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke)
                        .resultQueueUrl(result).runtime(runtime).concurrency(4).waitTime(Duration.ofSeconds(1))
                        .build().start()) {
                    HttpClient http = HttpClient.newHttpClient();
                    List<ProductMeetingPipeline.MeetingRun> runs = new ArrayList<>();
                    for (Meeting meeting : meetings) {
                        URI audioRef = blobs.put(ProductMeetingPipeline.syntheticAudio(meeting),
                                "audio/" + meeting.id() + ".wav", ProductMeetingPipeline.RAW_AUDIO)
                                .toCompletableFuture().get(10, TimeUnit.SECONDS);
                        java.time.Instant started = java.time.Instant.now();
                        String id = engine.startFromEvent(def.ref(), ProductMeetingPipeline.meetingRecorded(meeting,
                                audioRef, 0)).toCompletableFuture().get(30, TimeUnit.SECONDS);
                        List<String> answered = new ArrayList<>();
                        Set<String> seen = new HashSet<>();
                        InstanceSnapshot snapshot = load(state, id);
                        long until = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
                        while (!terminal(snapshot) && System.nanoTime() < until) {
                            for (JsonNode request : channel.openFor(id)) {
                                if (seen.add(request.path("reviewId").asText())) {
                                    ProductMeetingPipeline.answerAsTheKey(http, channel, meeting, request);
                                    answered.add(request.path("kind").asText());
                                }
                            }
                            Thread.onSpinWait();
                            snapshot = load(state, id);
                        }
                        runs.add(new ProductMeetingPipeline.MeetingRun(meeting, id, snapshot.status(),
                                snapshot.context(), null, Duration.between(started, java.time.Instant.now()),
                                metrics.stepLatencies(id), metrics.chainIds(id), answered));
                    }

                    for (ProductMeetingPipeline.MeetingRun run : runs) {
                        assertThat(run.status()).as(run.meeting().id()).isEqualTo(InstanceStatus.COMPLETED);
                        assertThat(run.context().path("docType").asText()).isEqualTo(
                                run.meeting().expected().docType());
                        assertThat(run.context().path("pageId").asText()).isNotBlank();
                        assertThat(run.chains()).as("one chain for %s", run.meeting().id()).hasSize(1);
                        assertThat(run.reviewsAnswered().contains("choice")).isEqualTo(
                                run.meeting().expected().review());
                    }
                    assertThat(confluence.pages()).hasSize(2);
                    assertThat(channel.published()).filteredOn(e -> e.getType().endsWith("minutes-published.v1"))
                            .hasSize(2);
                    assertThat(s3.headObject(h -> h.bucket("stint-blobs").key("retention/raw-audio/audio/m-001.wav"))
                            .serverSideEncryption()).isEqualTo(ServerSideEncryption.AES256);
                    ObjectNode report = Report.build(runs, metrics, Map.of(), "fake on floci");
                    assertThat(report.path("summary").path("completed").asInt()).isEqualTo(2);
                    assertThat(report.path("meetings").get(0).path("stepsMs").size()).isGreaterThanOrEqualTo(6);
                }
            }
        }
    }

    private static InstanceSnapshot load(StateStore state, String id) throws Exception {
        return state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private static boolean terminal(InstanceSnapshot snapshot) {
        return snapshot.status() == InstanceStatus.COMPLETED || snapshot.status() == InstanceStatus.FAILED;
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }
}
