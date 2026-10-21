package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;

/**
 * SDD 3b.3, CA1 to CA3, CA5, CA7 and CA8, end to end over the synthetic corpus: a meeting runs through the product definition,
 * its published fact starts {@code knowledge-ingest}, and the base learns from the approved minutes — the passages linked to the
 * entities they mention, nothing of the planted personal data anywhere, a redelivery changing nothing, a retried write not
 * duplicating, an erased document not coming back, and a fact that carries nothing but ids and counts. Fake models, no sleep.
 */
class KnowledgeIngestionTest {

    static final String INDEXED = "io.stintflow.knowledge.document-indexed.v1";
    static final String REJECTED = "io.stintflow.knowledge.ingest-rejected.v1";

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    private static CloudEvent publishedFact(ProductMeetingPipeline pipeline, String meetingId) {
        return pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                .filter(e -> meetingId.equals(e.getSubject())).findFirst().orElseThrow();
    }

    private static JsonNode data(CloudEvent event) {
        return Json.read(event.getData().toBytes());
    }

    private KnowledgeIngestPipeline ingestion(ProductMeetingPipeline pipeline) throws Exception {
        return new KnowledgeIngestPipeline(pipeline.blobs, new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()));
    }

    @Test
    void ca8_the_published_fact_points_at_the_approved_minutes() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work)) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            JsonNode fact = data(publishedFact(pipeline, meeting.id()));

            assertThat(fact.path("minutesRef").asText()).startsWith("file:").contains("minutes");
            assertThat(fact.path("publishedAtMillis").asLong()).isGreaterThan(Instant.now().minusSeconds(120).toEpochMilli());
            assertThat(fact.path("label").asText()).isEqualTo("confidential");
            assertThat(fact.toString()).doesNotContain("SYNTH-PII");
        }
    }

    @Test
    void ca1_ca2_a_published_meeting_is_ingested_linked_and_clean() throws Exception {
        List<String> markers = Corpus.allMarkers(meetings);
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            for (Meeting meeting : meetings) {
                pipeline.run(meeting, Duration.ofSeconds(60));
            }
            for (Meeting meeting : meetings) {
                InstanceSnapshot done = ingest.ingest(publishedFact(pipeline, meeting.id()), Duration.ofSeconds(60));
                assertThat(done.status()).as("meeting %s at %s", meeting.id(), done.position()).isEqualTo(InstanceStatus.COMPLETED);
            }
            List<CloudEvent> indexed = ingest.emitted().stream().filter(e -> e.getType().equals(INDEXED)).toList();
            assertThat(indexed).hasSize(meetings.size());
            assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(REJECTED))).isEmpty();
            JsonNode first = data(indexed.getFirst());
            assertThat(first.path("documentId").asText()).startsWith("meeting:");
            assertThat(first.path("status").asText()).isEqualTo("APPLIED");
            assertThat(first.path("extracted").path("passages").asInt()).isPositive();
            assertThat(first.path("schemaVersion").asText()).isEqualTo("meetings@1.0.0");

            // the base holds passages linked to the entities they mention (CA1)
            var items = ingest.store.retrieve(KnowledgeIngestPipeline.SCOPE, new RetrievalQuery("service level agreement decision", 50,
                    Sensitivity.CONFIDENTIAL)).toCompletableFuture().join();
            assertThat(items).isNotEmpty();
            assertThat(items.stream().flatMap(i -> i.entities().stream()).map(e -> e.type()).distinct().toList())
                    .isNotEmpty().isSubsetOf("System", "Term", "Person", "Decision", "Requirement", "ExternalReference");
            assertThat(ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 10_000).toCompletableFuture().join())
                    .extracting(ChangeEntry::elementType).contains("Passage", "Link");

            // and nothing planted is anywhere it should not be (CA2)
            List<String> places = new ArrayList<>();
            places.add("base: " + ingest.store.dump(KnowledgeIngestPipeline.SCOPE));
            ingest.emitted().forEach(e -> places.add("fact: " + new String(CeWire.toJson(e), StandardCharsets.UTF_8)));
            for (Meeting meeting : meetings) {
                String id = ingest.engine.startFromEvent(ingest.definition.ref(), publishedFact(pipeline, meeting.id()))
                        .toCompletableFuture().get(10, TimeUnit.SECONDS);
                places.add("snapshot: " + ingest.state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS));
                places.add("audit: " + ingest.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS));
            }
            assertThat(places).allSatisfy(place -> markers.forEach(marker -> assertThat(place).doesNotContain(marker)));
            try (var files = Files.list(work.resolve("blobs"))) {
                for (Path file : files.toList()) {
                    if (file.getFileName().toString().contains("knowledge-batch")) {
                        String batch = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);
                        markers.forEach(marker -> assertThat(batch).doesNotContain(marker));
                    }
                }
            }
        }
    }

    @Test
    void ca3_a_redelivered_fact_and_a_republication_change_nothing() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            CloudEvent fact = publishedFact(pipeline, meeting.id());
            InstanceSnapshot first = ingest.ingest(fact, Duration.ofSeconds(60));
            long entries = ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 10_000).toCompletableFuture().join().size();
            assertThat(entries).isPositive();

            // the same fact again is the same instance: it is not run twice
            String sameInstance = ingest.engine.startFromEvent(ingest.definition.ref(), fact).toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            assertThat(sameInstance).isEqualTo(first.instanceId());
            assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(INDEXED))).hasSize(1);

            // the same minutes republished (another fact, same content and a later revision): a new instance, no new change
            CloudEvent republished = CloudEventBuilder.v1(fact).withId("republished-" + fact.getId()).build();
            InstanceSnapshot second = ingest.ingest(republished, Duration.ofSeconds(60));
            assertThat(second.instanceId()).isNotEqualTo(first.instanceId());
            assertThat(second.status()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode status = data(ingest.emitted().stream().filter(e -> e.getType().equals(INDEXED)).toList().get(1));
            assertThat(status.path("status").asText()).isEqualTo("UNCHANGED");
            assertThat(ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 10_000).toCompletableFuture().join())
                    .hasSize((int) entries);
        }
    }

    @Test
    void ca3_a_write_that_fails_is_retried_by_the_engine_and_the_base_has_each_thing_once() throws Exception {
        AtomicReference<Instant> now = new AtomicReference<>(Instant.now());
        InMemoryTimerService timer = new InMemoryTimerService(now::get);
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work)) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            CloudEvent fact = publishedFact(pipeline, meeting.id());
            try (KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(pipeline.blobs,
                    new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()), now::get, timer, true)) {
                AtomicInteger attempts = new AtomicInteger();
                ingest.store.faultBeforeSwap(() -> {
                    if (attempts.incrementAndGet() <= 2) {
                        throw new IllegalStateException("the base is not reachable");   // the first two writes fail, atomically
                    }
                });
                CompletableFuture<InstanceSnapshot> run = CompletableFuture.supplyAsync(() -> {
                    try {
                        return ingest.ingest(fact, Duration.ofSeconds(60));
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                });
                java.util.Set<String> passed = new java.util.HashSet<>();
                long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!run.isDone() && System.nanoTime() < until) {
                    for (String key : retryKeys(ingest)) {
                        if (passed.add(key)) {
                            now.set(now.get().plusSeconds(61));
                            timer.tick();
                        }
                    }
                    Thread.onSpinWait();
                }
                InstanceSnapshot done = run.get(5, TimeUnit.SECONDS);

                assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
                assertThat(attempts.get()).isEqualTo(3);   // two failures, then the same batch written
                var log = ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 10_000).toCompletableFuture().join();
                assertThat(log).extracting(ChangeEntry::targetId).doesNotHaveDuplicates();   // each thing was added once
                assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(INDEXED))).hasSize(1);
            }
        }
    }

    private static List<String> retryKeys(KnowledgeIngestPipeline ingest) {
        List<String> keys = new ArrayList<>();
        String id = ingest.current;
        if (id != null) {
            ingest.state.load(id).toCompletableFuture().join().map(InstanceSnapshot::waitingKey)
                    .filter(k -> k != null && k.startsWith("retry:")).ifPresent(keys::add);
        }
        return keys;
    }

    @Test
    void ca7_an_erased_meeting_does_not_come_back_with_a_redelivery_of_its_fact() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            CloudEvent fact = publishedFact(pipeline, meeting.id());
            ingest.ingest(fact, Duration.ofSeconds(60));
            ingest.store.delete(KnowledgeIngestPipeline.SCOPE, new DeleteRequest.ByDocument("erasure-1", "erasure:document",
                    "meeting:" + meeting.id())).toCompletableFuture().join();

            // another fact for the same minutes (the first instance is done and would not run again)
            InstanceSnapshot again = ingest.ingest(CloudEventBuilder.v1(fact).withId("again-" + fact.getId()).build(),
                    Duration.ofSeconds(60));

            assertThat(again.status()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode last = data(ingest.emitted().stream().filter(e -> e.getType().equals(INDEXED)).reduce((a, b) -> b)
                    .orElseThrow());
            assertThat(last.path("status").asText()).isEqualTo("REJECTED_ERASED");
            assertThat(ingest.store.retrieve(KnowledgeIngestPipeline.SCOPE, new RetrievalQuery("checkout", 10,
                    Sensitivity.RESTRICTED)).toCompletableFuture().join()).isEmpty();
        }
    }

    @Test
    void ca5_an_extraction_out_of_the_schema_emits_a_rejection_writes_nothing_and_ends() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work)) {
            Meeting meeting = meetings.getFirst();
            pipeline.run(meeting, Duration.ofSeconds(60));
            // a model that invents a type the schema does not have
            dev.langchain4j.model.chat.ChatModel wrong = new dev.langchain4j.model.chat.ChatModel() {
                @Override
                public dev.langchain4j.model.chat.response.ChatResponse doChat(
                        dev.langchain4j.model.chat.request.ChatRequest request) {
                    ObjectNode answer = Json.obj();
                    answer.putArray("entities").addObject().put("ref", "x1").put("type", "Spaceship").put("mention", "Enterprise")
                            .putArray("passages").add(0);
                    answer.putArray("relations");
                    return dev.langchain4j.model.chat.response.ChatResponse.builder()
                            .aiMessage(dev.langchain4j.data.message.AiMessage.from(answer.toString())).build();
                }
            };
            try (KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(pipeline.blobs, wrong)) {
                InstanceSnapshot done = ingest.ingest(publishedFact(pipeline, meeting.id()), Duration.ofSeconds(60));

                assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
                assertThat(ingest.emitted()).hasSize(1).allSatisfy(e -> assertThat(e.getType()).isEqualTo(REJECTED));
                JsonNode rejection = data(ingest.emitted().getFirst());
                assertThat(rejection.path("reason").asText()).isEqualTo("out-of-schema");
                assertThat(rejection.toString()).doesNotContain("Spaceship", "Enterprise");
                assertThat(ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 10).toCompletableFuture().join()).isEmpty();
            }
        }
    }

    @Test
    void ca4_a_fact_of_another_base_is_refused_by_the_worker_and_nothing_is_written() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            // the worker serves examples/product-meetings only: asking for another base through the task is refused
            ObjectNode in = Json.obj();
            in.put("batchRef", "file:///nowhere").putObject("scope").put("namespace", "examples").put("base", "hr");
            var refused = new io.stintflow.worker.knowledge.KnowledgeWriteHandler(ingest.store, pipeline.blobs,
                    new io.stintflow.worker.knowledge.ScopeAllowList(java.util.Set.of(KnowledgeIngestPipeline.SCOPE)))
                    .execute(new io.stintflow.worker.TaskContext("/do/1/x", "i", "c", 1, in, ingest.definition.ref(), "chn",
                            io.stintflow.spi.Lineage.NONE));
            assertThat(refused.toCompletableFuture()).isCompletedExceptionally();
            assertThat(ingest.store.changes(new io.stintflow.knowledge.KnowledgeScope("examples", "hr"), 0, 10)
                    .toCompletableFuture().join()).isEmpty();
        }
    }
}
