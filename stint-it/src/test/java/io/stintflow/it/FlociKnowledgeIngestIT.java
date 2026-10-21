package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.aws.worker.SqsWorkerBinding;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.example.meeting.Corpus;
import io.stintflow.example.meeting.FakeKnowledgeModel;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.extraction.Glossary;
import io.stintflow.knowledge.inmemory.CompositeKnowledgeStore;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.StateStore;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import io.stintflow.worker.decision.SensitivityPolicy;
import io.stintflow.worker.knowledge.KnowledgeWorkers;
import io.stintflow.worker.knowledge.ScopeAllowList;
import io.stintflow.worker.langchain4j.KnowledgeExtractionWorker;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * SDD 3b.3, CA10, on floci: the ingestion of a published meeting with the state in DynamoDB and the tasks over SQS, answered by
 * a worker behind the SQS binding; the minutes and the batch rest in blobs, the base lives in the worker. The fact that starts
 * it is the one the product definition emits; the one it emits carries ids and counts only.
 */
class FlociKnowledgeIngestIT {

    private static final String MINUTES = "# Minutes m-9 (ata)\n\nThe Checkout Service needs a retry policy before the SLA review.\n\n"
            + "Action: PERSON_a1b2c3 writes the rollback plan for the Payments API.";

    @Test
    void a_published_meeting_is_ingested_over_dynamodb_and_sqs() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                DynamoDbTestTables.create(ddb, "stint-instances", "stint-waits");
                DynamoDbTestTables.createOutbox(ddb, "stint-outbox");
                StateStore state = new DynamoDbStateStore(ddb, "stint-instances", "stint-waits", "stint-outbox", 4);
                String invoke = sqs.createQueue(q -> q.queueName("kb-invoke")).queueUrl();
                String results = sqs.createQueue(q -> q.queueName("kb-results")).queueUrl();

                var blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3b3-floci"));
                URI minutes = blobs.put(MINUTES.getBytes(StandardCharsets.UTF_8), "minutes/m-9.md",
                        new RetentionClass("minutes", Duration.ofDays(90))).toCompletableFuture().get(10, TimeUnit.SECONDS);

                KnowledgeScope scope = KnowledgeScope.named("examples", "product-meetings");
                CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs,
                        new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());
                var base = Corpus.loadKnowledge().base();
                Glossary glossary = new Glossary("product", "1", base.glossary().stream()
                        .map(t -> new Glossary.Term(t.id(), t.type(), t.name(), t.aliases(), null)).toList());
                TaskHandlerRegistry handlers = new TaskHandlerRegistry().register("extract-knowledge",
                        KnowledgeExtractionWorker.builder().chatModel(new FakeKnowledgeModel(base.glossary()))
                                .provider("fake", false).policy(SensitivityPolicy.fromProperties(java.util.Map.of()))
                                .glossaries(ref -> ref.equals("product@1") ? glossary : null).store(store)
                                .blobs(blobs, new RetentionClass("knowledge-batch", Duration.ofDays(7))).build());
                KnowledgeWorkers.register(handlers, store, blobs, new ScopeAllowList(Set.of(scope)), null,
                        InstantSource.system());

                try (SqsWorkerBinding worker = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke)
                        .resultQueueUrl(results).runtime(new WorkerRuntime(handlers, blobs)).concurrency(4)
                        .waitTime(Duration.ofSeconds(1)).visibilityTimeout(Duration.ofSeconds(60)).build().start()) {
                    WorkflowDefinition ingest;
                    try (InputStream yaml = getClass().getClassLoader()
                            .getResourceAsStream("stint/workflows/knowledge-ingest.yaml")) {
                        ingest = new DslLoader().load(yaml, "knowledge-ingest.yaml", LoadOptions.STRICT);
                    }
                    WorkflowRegistry registry = new WorkflowRegistry().requirePolicy();
                    registry.register(ingest);
                    InMemoryEventPublisher facts = new InMemoryEventPublisher();
                    WorkflowEngine engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invoke, results), state,
                            new InMemoryTimerService(), blobs, new TreeInterpreter(new JqExpressionEvaluator()),
                            InstantSource.system(), facts, null, ExecutionTracer.NOOP);

                    ObjectNode data = Json.obj().put("meetingId", "m-9").put("minutesRef", minutes.toString())
                            .put("docType", "ata").put("label", "confidential")
                            .put("publishedAtMillis", System.currentTimeMillis());
                    CloudEvent fact = CloudEventBuilder.v1().withId("pub-m-9")
                            .withSource(URI.create("https://product-meeting.example"))
                            .withType("io.stintflow.examples.minutes-published.v1").withSubject("m-9")
                            .withDataContentType("application/json").withData(Json.bytes(data)).build();

                    String id = engine.startFromEvent(ingest.ref(), fact).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    awaitStatus(state, id, InstanceStatus.COMPLETED);

                    assertThat(facts.published()).hasSize(1);
                    JsonNode indexed = Json.read(facts.published().getFirst().getData().toBytes());
                    assertThat(facts.published().getFirst().getType())
                            .isEqualTo("io.stintflow.knowledge.document-indexed.v1");
                    assertThat(indexed.path("documentId").asText()).isEqualTo("meeting:m-9");
                    assertThat(indexed.path("status").asText()).isEqualTo("APPLIED");
                    assertThat(indexed.path("extracted").path("passages").asInt()).isPositive();

                    var items = store.retrieve(scope, new RetrievalQuery("checkout retry policy", 5, Sensitivity.CONFIDENTIAL))
                            .toCompletableFuture().get(10, TimeUnit.SECONDS);
                    assertThat(items).isNotEmpty();
                    assertThat(items.getFirst().entities()).extracting(e -> e.label()).contains("Checkout Service");

                    // the same fact again starts the same instance: nothing runs twice, the base does not change
                    long entries = store.changes(scope, 0, 10_000).toCompletableFuture().get(10, TimeUnit.SECONDS).size();
                    assertThat(engine.startFromEvent(ingest.ref(), fact).toCompletableFuture().get(30, TimeUnit.SECONDS))
                            .isEqualTo(id);
                    assertThat(store.changes(scope, 0, 10_000).toCompletableFuture().get(10, TimeUnit.SECONDS)).hasSize((int) entries);
                    assertThat(facts.published()).hasSize(1);
                }
            }
        }
    }

    private static void awaitStatus(StateStore state, String id, InstanceStatus expected) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        InstanceStatus status = null;
        while (System.nanoTime() < until) {
            status = state.load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).map(InstanceSnapshot::status).orElse(null);
            if (status == expected || status == InstanceStatus.FAILED) {
                break;
            }
            Thread.onSpinWait();
        }
        assertThat(status).isEqualTo(expected);
    }
}
