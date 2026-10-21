package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.List;
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
import io.stintflow.example.meeting.ProductMeetingPipeline;
import io.stintflow.inmemory.FilesystemBlobStore;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.Link;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.knowledge.Source;
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
import io.stintflow.worker.knowledge.KnowledgeWorkers;
import io.stintflow.worker.knowledge.ScopeAllowList;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * SDD 3b.5, CA10, on floci: a person is erased by who she is, with the state in DynamoDB and the tasks over SQS, answered by a
 * worker behind the SQS binding. The identifier of the data subject rests in a blob and is resolved by the worker that holds the
 * secret; the base, the blobs the document came from and the subject all go, and the report says it was verified.
 */
class FlociKnowledgeEraseIT {

    private static final String NAME = "SYNTH-PII-0901";

    @Test
    void a_person_is_erased_by_who_she_is_over_dynamodb_and_sqs() throws Exception {
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
                String invoke = sqs.createQueue(q -> q.queueName("er-invoke")).queueUrl();
                String results = sqs.createQueue(q -> q.queueName("er-results")).queueUrl();

                var blobs = new FilesystemBlobStore(Files.createTempDirectory("stint-3b5-floci"));
                KnowledgeScope scope = KnowledgeScope.named("examples", "product-meetings");
                CompositeKnowledgeStore store = new CompositeKnowledgeStore(new DeterministicEmbedder(), blobs,
                        new RetentionClass("restricted", Duration.ofDays(30)), InstantSource.system());

                // a document that names the person, written as the ingestion would, with the minutes it came from
                var pseudonyms = ProductMeetingPipeline.pseudonyms();
                String pseudonym = pseudonyms.labelStable(scope.key(), "person", NAME);
                String entityId = EntityIds.of("Person", pseudonym);
                URI minutes = blobs.put(("# Minutes m-9\n\n" + pseudonym + " owns the rollback plan.").getBytes(StandardCharsets.UTF_8),
                        "minutes/m-9.md", new RetentionClass("minutes", Duration.ofDays(90))).toCompletableFuture().get(10, TimeUnit.SECONDS);
                String text = pseudonym + " owns the rollback plan for the Payments API.";
                String documentId = "meeting:m-9";
                Passage passage = new Passage(PassageIds.of(scope, documentId, 0, text), 0, text, "en", Sensitivity.CONFIDENTIAL);
                store.write(scope, new KnowledgeBatch("b-9", documentId, PassageIds.sha256Hex(text), 1L, Source.NONE,
                        "meetings@1.0.0", List.of(passage),
                        List.of(new Entity(entityId, "Person", pseudonym, Sensitivity.CONFIDENTIAL)), List.of(),
                        List.of(new Link(passage.passageId(), entityId)), List.of(minutes))).toCompletableFuture()
                        .get(10, TimeUnit.SECONDS);
                URI subject = blobs.put(NAME.getBytes(StandardCharsets.UTF_8), "erasure/subject",
                        new RetentionClass("erasure-subject", Duration.ofDays(1))).toCompletableFuture().get(10, TimeUnit.SECONDS);

                TaskHandlerRegistry handlers = new TaskHandlerRegistry();
                KnowledgeWorkers.registerErasure(handlers, store, blobs, new ScopeAllowList(Set.of(scope)), null,
                        InstantSource.system(), null, pseudonyms);

                try (SqsWorkerBinding worker = SqsWorkerBinding.builder().sqs(sqs).invokeQueueUrl(invoke)
                        .resultQueueUrl(results).runtime(new WorkerRuntime(handlers, blobs)).concurrency(4)
                        .waitTime(Duration.ofSeconds(1)).visibilityTimeout(Duration.ofSeconds(60)).build().start()) {
                    WorkflowDefinition erase;
                    try (InputStream yaml = getClass().getClassLoader()
                            .getResourceAsStream("stint/workflows/knowledge-erase.yaml")) {
                        erase = new DslLoader().load(yaml, "knowledge-erase.yaml", LoadOptions.STRICT);
                    }
                    WorkflowRegistry registry = new WorkflowRegistry().requirePolicy();
                    registry.register(erase);
                    InMemoryEventPublisher facts = new InMemoryEventPublisher();
                    WorkflowEngine engine = new WorkflowEngine(registry, new SqsTaskTransport(sqs, invoke, results), state,
                            new InMemoryTimerService(), blobs, new TreeInterpreter(new JqExpressionEvaluator()),
                            InstantSource.system(), facts, null, ExecutionTracer.NOOP);

                    ObjectNode data = Json.obj().put("requestId", "er-9").put("kind", "person").put("passages", "LINKED")
                            .put("label", "restricted").put("personRef", subject.toString());
                    CloudEvent request = CloudEventBuilder.v1().withId("erase-er-9")
                            .withSource(URI.create("https://dpo-channel.example"))
                            .withType("io.stintflow.knowledge.erasure-requested.v1").withSubject("er-9")
                            .withDataContentType("application/json").withData(Json.bytes(data)).build();

                    String id = engine.startFromEvent(erase.ref(), request).toCompletableFuture().get(30, TimeUnit.SECONDS);
                    awaitStatus(state, id, InstanceStatus.COMPLETED);
                    awaitFacts(facts, 1);   // the fact leaves a moment after the state is final

                    assertThat(facts.published()).hasSize(1);
                    CloudEvent fact = facts.published().getFirst();
                    assertThat(fact.getType()).isEqualTo("io.stintflow.knowledge.erasure-completed.v1");
                    // a restricted instance's fact rests in a blob; its data is ids and counts
                    JsonNode report = fact.getData() != null ? Json.read(fact.getData().toBytes())
                            : Json.read(Files.readAllBytes(Path.of(URI.create(String.valueOf(fact.getExtension("dataref"))))));
                    assertThat(report.path("verified").asBoolean()).isTrue();
                    assertThat(report.path("tombstone").asBoolean()).isTrue();
                    assertThat(report.path("counts").has("ERASE.ENTITY")).isTrue();
                    assertThat(report.toString()).doesNotContain(NAME).doesNotContain(pseudonym).doesNotContain(entityId);

                    assertThat(store.retrieve(scope, new RetrievalQuery("rollback plan payments", 5, Sensitivity.RESTRICTED))
                            .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEmpty();
                    assertThat(store.dump(scope)).doesNotContain(pseudonym).doesNotContain(NAME);
                    assertThat(exists(blobs, minutes)).isFalse();
                    assertThat(exists(blobs, subject)).isFalse();

                    // the same request again starts the same instance: nothing runs twice
                    assertThat(engine.startFromEvent(erase.ref(), request).toCompletableFuture().get(30, TimeUnit.SECONDS))
                            .isEqualTo(id);
                    assertThat(facts.published()).hasSize(1);
                }
            }
        }
    }

    private static boolean exists(FilesystemBlobStore blobs, URI uri) {
        try {
            blobs.get(uri).toCompletableFuture().join();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static void awaitFacts(InMemoryEventPublisher facts, int expected) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (facts.published().size() < expected && System.nanoTime() < until) {
            Thread.sleep(50);
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
