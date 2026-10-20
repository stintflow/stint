package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.DefaultCloudEventCodec;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

/**
 * SDD 3a.7, measurement (not a CA, not run by the build — its name matches no test pattern): a load in the spirit of
 * the sandbox's S.4 over floci. 50 tasks sent at once, 10 slow (their work completes 2 s later, on a delayed
 * executor — simulated work, no sleep in a thread of the test) and 40 fast (50 ms), through the binding with
 * concurrency 1 and 8. The latency of each task runs from its send to its result's arrival in the result queue.
 * <pre>mvn -o test -pl stint-it -Dtest=SqsWorkerLoadMeasurement</pre>
 */
class SqsWorkerLoadMeasurement {

    static final int TASKS = 50;
    static final Duration SLOW = Duration.ofSeconds(2);
    static final Duration FAST = Duration.ofMillis(50);

    @Test
    void measure() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                    .httpClient(UrlConnectionHttpClient.create()).build()) {
                List<String> report = new ArrayList<>();
                report.add(run(sqs, 1));
                report.add(run(sqs, 8));
                report.forEach(System.out::println);
            }
        }
    }

    private static String run(SqsClient sqs, int concurrency) throws Exception {
        String invoke = sqs.createQueue(q -> q.queueName("load-invoke-" + concurrency)).queueUrl();
        String result = sqs.createQueue(q -> q.queueName("load-result-" + concurrency)).queueUrl();
        TaskHandlerRegistry handlers = new TaskHandlerRegistry().register("work", ctx -> {
            Duration work = ctx.input().path("slow").asBoolean() ? SLOW : FAST;
            return CompletableFuture.supplyAsync(() -> Json.obj().put("done", ctx.correlationId()),
                    CompletableFuture.delayedExecutor(work.toMillis(), TimeUnit.MILLISECONDS));
        });
        DefaultCloudEventCodec codec = new DefaultCloudEventCodec();
        Map<String, Long> sentAt = new ConcurrentHashMap<>();
        Map<String, Long> latency = new ConcurrentHashMap<>();

        try (SqsWorkerBinding binding = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke)
                .resultQueueUrl(result).runtime(new WorkerRuntime(handlers, null)).concurrency(concurrency)
                .waitTime(Duration.ofSeconds(1)).build().start()) {
            long start = System.nanoTime();
            for (int batch = 0; batch < TASKS; batch += 10) {
                List<SendMessageBatchRequestEntry> entries = new ArrayList<>();
                for (int i = batch; i < batch + 10; i++) {
                    String id = "c-" + i;
                    boolean slow = i % 5 == 0;   // 10 of 50, spread through the load
                    TaskInvocation inv = new TaskInvocation("i-" + i, "/do/0/work", id,
                            new WorkflowRef("load", "s4", "1.0.0"), "work", Json.obj().put("slow", slow), 1,
                            Lineage.NONE, null);
                    entries.add(SendMessageBatchRequestEntry.builder().id(id)
                            .messageBody(new String(CeWire.toJson(codec.toEvent(inv)), StandardCharsets.UTF_8)).build());
                }
                long now = System.nanoTime();
                entries.forEach(e -> sentAt.put(e.id(), now));
                sqs.sendMessageBatch(r -> r.queueUrl(invoke).entries(entries));
            }
            long until = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
            while (latency.size() < TASKS && System.nanoTime() < until) {
                for (Message m : sqs.receiveMessage(r -> r.queueUrl(result).maxNumberOfMessages(10).waitTimeSeconds(1))
                        .messages()) {
                    long arrived = System.nanoTime();
                    String id = codec.toResult(CeWire.fromJson(m.body().getBytes(StandardCharsets.UTF_8))).correlationId();
                    latency.putIfAbsent(id, arrived - sentAt.get(id));
                    sqs.deleteMessage(r -> r.queueUrl(result).receiptHandle(m.receiptHandle()));
                }
            }
            long total = System.nanoTime() - start;
            assertThat(latency).hasSize(TASKS);
            List<Long> sorted = latency.values().stream().sorted().toList();
            return String.format(Locale.ROOT,
                    "MEASURE concurrency=%d tasks=%d (10 x %d ms, 40 x %d ms): median %d ms, p95 %d ms, max %d ms, "
                            + "all done in %d ms", concurrency, TASKS, SLOW.toMillis(), FAST.toMillis(),
                    ms(percentile(sorted, 0.5)), ms(percentile(sorted, 0.95)), ms(sorted.getLast()), ms(total));
        }
    }

    /** Nearest rank. */
    private static long percentile(List<Long> sorted, double q) {
        int index = (int) Math.ceil(q * sorted.size()) - 1;
        return sorted.get(Math.clamp(index, 0, sorted.size() - 1));
    }

    private static long ms(long nanos) {
        return TimeUnit.NANOSECONDS.toMillis(nanos);
    }
}
