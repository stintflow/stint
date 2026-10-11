package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.SqsDelayTimerService;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * SDD 2.9, CA3: the S.2 finding 8i on real SQS and DynamoDB. Fifty instances each fail attempt 1 with a transient
 * 503 and retry after {@code delay: 1 s} — the shortest delay, the one that went out as {@code DelaySeconds=0} and
 * could fire before the retry wait was saved. Every instance must complete; none may be left WAITING on a retry wait
 * with no timer behind it. No sleeps: completions and long polls.
 */
class FlociEarlyFireIT {

    private static final int INSTANCES = 50;
    private static final String YAML = """
            document:
              dsl: '1.0.0'
              namespace: early
              name: one-second-retry
              version: '1.0.0'
            do:
              - render:
                  try:
                    - work:
                        call: remote
                        with:
                          task: flaky
                        timeout:
                          after: { seconds: 60 }
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
            """;

    @Test
    void fifty_one_second_retries_never_strand_an_instance() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                String invokeQueue = sqs.createQueue(q -> q.queueName("early-invoke")).queueUrl();
                String resultQueue = sqs.createQueue(q -> q.queueName("early-result")).queueUrl();
                // The documented timer queue setup (docs/connectors.md): visibility 30 s, redrive after 5 receives.
                String timerDlq = sqs.createQueue(q -> q.queueName("early-timers-dlq")).queueUrl();
                String dlqArn = sqs.getQueueAttributes(r -> r.queueUrl(timerDlq).attributeNames(QueueAttributeName.QUEUE_ARN))
                        .attributes().get(QueueAttributeName.QUEUE_ARN);
                String timerQueue = sqs.createQueue(q -> q.queueName("early-timers").attributes(Map.of(
                        QueueAttributeName.VISIBILITY_TIMEOUT, "30",
                        QueueAttributeName.REDRIVE_POLICY,
                        "{\"deadLetterTargetArn\":\"" + dlqArn + "\",\"maxReceiveCount\":\"5\"}"))).queueUrl();
                DynamoDbTestTables.create(ddb, "early-instances", "early-waits");
                StateStore state = new DynamoDbStateStore(ddb, "early-instances", "early-waits");

                WorkflowDefinition def = new DslLoader().load(new ByteArrayInputStream(YAML.getBytes(StandardCharsets.UTF_8)),
                        "one-second-retry.yaml", LoadOptions.STRICT);
                SqsDelayTimerService timers = new SqsDelayTimerService(sqs, timerQueue);
                WorkflowRegistry registry = new WorkflowRegistry(timers.maxDelay());
                registry.register(def);
                BlobStore blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-sdd29-floci"));
                WorkflowEngine engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invokeQueue, resultQueue),
                        state, timers, blobs, new TreeInterpreter(new JqExpressionEvaluator()), InstantSource.system(),
                        null, null);
                WorkerRuntime worker = new WorkerRuntime(new TaskHandlerRegistry().register("flaky", ctx ->
                        ctx.attempt() == 1
                                ? CompletableFuture.failedFuture(new TaskFailure(ErrorInfo.TYPE_COMMUNICATION, 503,
                                        "transient", null))
                                : CompletableFuture.completedFuture(Json.obj().put("attempt", ctx.attempt()))), blobs);

                try (SqsWorkerRunner runner = new SqsWorkerRunner(sqs, invokeQueue, resultQueue, worker)) {
                    List<String> ids = new ArrayList<>();
                    for (int i = 0; i < INSTANCES; i++) {
                        ids.add(engine.start(def.ref(), Json.obj().put("n", i)));
                    }

                    List<String> stranded = new ArrayList<>();
                    int completed = 0;
                    for (String id : ids) {
                        engine.awaitCompletion(id, Duration.ofMinutes(3)).toCompletableFuture().handle((r, e) -> null)
                                .get(4, TimeUnit.MINUTES);
                        InstanceSnapshot snap = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
                        if (snap.status() == InstanceStatus.COMPLETED && snap.context().get("attempt").asInt() == 2) {
                            completed++;
                        } else {
                            stranded.add(id + " " + snap.status() + " waiting on " + snap.waitingKey());
                        }
                    }

                    long waitsLeft = ddb.scan(r -> r.tableName("early-waits").select("COUNT")).count();
                    String dlqDepth = sqs.getQueueAttributes(r -> r.queueUrl(timerDlq)
                            .attributeNames(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)).attributes()
                            .get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES);
                    System.out.printf("CA3: %d of %d instances completed after a 1 s retry; stranded: %s; waits left: %d;"
                            + " timer DLQ depth: %s%n", completed, INSTANCES, stranded, waitsLeft, dlqDepth);
                    assertThat(stranded).isEmpty();
                    assertThat(completed).isEqualTo(INSTANCES);
                    assertThat(dlqDepth).isEqualTo("0");
                }
            }
        }
    }
}
