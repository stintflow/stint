package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

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
import io.stintflow.example.meeting.ArchitectureAdrPipeline;
import io.stintflow.example.meeting.Corpus;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.example.meeting.ProductMeetingPipeline;
import io.stintflow.example.meeting.ReviewChannel;
import io.stintflow.example.meeting.RunMetrics;
import io.stintflow.example.meeting.ScenarioReport;
import io.stintflow.example.meeting.ScenarioRun;
import io.stintflow.example.meeting.ScenarioWorkers;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.otel.OpenTelemetryExecutionTracer;
import io.stintflow.spi.StateStore;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.publishing.FakeConfluenceServer;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.scheduler.SchedulerClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 3c.6, CA1 on floci (dummy {@code test}/{@code test} credentials): the scenario of the milestone end to end — the product
 * meetings that take decisions, their minutes ingested, announced and linked, then the architecture meetings drafted into decisions
 * that run {@code architecture-adr} with a person on every conflict and every pair the machine cannot settle — with the state in
 * DynamoDB, the tasks over SQS, the blobs in S3 and every worker behind one SQS worker binding. Fake models and a fake Confluence;
 * the answer key is the corpus's, the same as in the process.
 */
class FlociScenarioIT {

    private static final String ROLE = "arn:aws:iam::000000000000:role/stint-scheduler";

    @Test
    void the_scenario_runs_on_sqs_dynamodb_and_s3_and_follows_the_key() throws Exception {
        List<Meeting> meetings = Corpus.load();
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
                         ProductMeetingPipeline.CONFLUENCE_TOKEN, ProductMeetingPipeline.SPACE, "HR", ArchitectureAdrPipeline.SPACE);
                 ReviewChannel channel = new ReviewChannel(URI.create(ProductMeetingPipeline.REVIEW_SOURCE))) {

                String invoke = sqs.createQueue(q -> q.queueName("sc-invoke")).queueUrl();
                String result = sqs.createQueue(q -> q.queueName("sc-result")).queueUrl();
                String timers = sqs.createQueue(q -> q.queueName("stint-timers")).queueUrl();
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createOutbox(ddb, "stint-outbox");
                DynamoDbTestTables.createInbox(ddb, "stint-inbox");
                s3.createBucket(b -> b.bucket("stint-blobs"));
                S3BlobStore blobs = new S3BlobStore(s3, "stint-blobs");

                RunMetrics metrics = new RunMetrics(Map.of(), false);
                OpenTelemetryExecutionTracer tracer = new OpenTelemetryExecutionTracer(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(metrics)).build().get("stint-it-3c6"));
                InMemoryAuditLog audit = new InMemoryAuditLog();
                ScenarioWorkers workers = new ScenarioWorkers(meetings, ProductMeetingPipeline.fakes(meetings), metrics, blobs, tracer,
                        confluence.baseUri(), InstantSource.system(), audit);
                WorkerRuntime runtime = new WorkerRuntime(workers.handlers, blobs, tracer);

                DelayRoutingTimerService timerService = new DelayRoutingTimerService(
                        new SqsDelayTimerService(sqs, timers),
                        new EventBridgeSchedulerTimerService(scheduler, "stint-timers", ROLE,
                                queueArn(sqs, timers), Duration.ofDays(365)), InstantSource.system());
                WorkflowRegistry registry = new WorkflowRegistry(timerService.maxDelay()).requirePolicy();
                List<WorkflowDefinition> definitions = ScenarioRun.definitions();
                definitions.forEach(registry::register);
                StateStore state = new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4, "stint-inbox");
                WorkflowEngine engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invoke, result), state, timerService,
                        blobs, new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(), channel, null, tracer)
                        .auditLog(audit);
                channel.answersTo(engine);

                try (SqsWorkerBinding binding = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke)
                        .resultQueueUrl(result).runtime(runtime).concurrency(4).waitTime(Duration.ofSeconds(1))
                        .build().start()) {
                    ScenarioRun.Env env = new ScenarioRun.Env(engine, state, channel, blobs, metrics, definitions, workers,
                            Duration.ofMinutes(5));
                    ScenarioRun.Result run = ScenarioRun.execute(env, meetings);

                    assertThat(ScenarioRun.disagreements(run)).as("where the run does not follow the key").isEmpty();
                    assertThat(run.architecture()).filteredOn(ScenarioRun.PairRun::reviewed).hasSize(3);
                    assertThat(confluence.pages()).filteredOn(p -> p.spaceKey().equals(ArchitectureAdrPipeline.SPACE)).hasSize(4);

                    ObjectNode report = ScenarioReport.build(run, env, "floci", "fake");
                    assertThat(report.path("conflicts").path("truePositive").asInt()).isEqualTo(1);
                    assertThat(report.path("conflicts").path("falseNegative").asInt()).isZero();
                    assertThat(report.path("productDecisions").path("inTheGraph").asInt()).isEqualTo(3);
                    Files.createDirectories(Path.of("target"));
                    Files.writeString(Path.of("target", "3c.6-report-floci.json"), report.toPrettyString(), StandardCharsets.UTF_8);
                }
            }
        }
    }

    private static String queueArn(SqsClient sqs, String queueUrl) {
        return sqs.getQueueAttributes(r -> r.queueUrl(queueUrl).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }
}
