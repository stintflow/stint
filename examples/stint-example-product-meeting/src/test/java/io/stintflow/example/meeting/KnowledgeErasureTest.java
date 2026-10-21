package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.BaseInfo;
import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.DeterministicEmbedder;
import io.stintflow.knowledge.Embedder;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.RetrievalQuery;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.Json;

/**
 * SDD 3b.5, end to end over the synthetic corpus: a person is erased from the base by who they are (a pointer to a blob that
 * holds the identifier), through the definition {@code knowledge-erase} — the base, the blobs the documents came from, a look at
 * what is left, the subject forgotten — and the base moves to another embedding model through {@code knowledge-reindex}. Fake
 * models, no sleep.
 */
class KnowledgeErasureTest {

    static final String COMPLETED = "io.stintflow.knowledge.erasure-completed.v1";
    static final String INCOMPLETE = "io.stintflow.knowledge.erasure-incomplete.v1";
    static final String REINDEXED = "io.stintflow.knowledge.reindexed.v1";
    static final RetentionClass SUBJECT = new RetentionClass("erasure-subject", Duration.ofDays(1));

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    private static CloudEvent publishedFact(ProductMeetingPipeline pipeline, String meetingId) {
        return pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                .filter(e -> meetingId.equals(e.getSubject())).findFirst().orElseThrow();
    }

    /** The data of a fact: inline, or — a restricted instance's fact — in the blob its {@code dataref} points at. */
    private static JsonNode data(CloudEvent event) {
        try {
            if (event.getData() != null) {
                return Json.read(event.getData().toBytes());
            }
            return Json.read(Files.readAllBytes(Path.of(URI.create(String.valueOf(event.getExtension("dataref"))))));
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private record Named(Meeting meeting, String name, String pseudonym) {
    }

    /** A meeting whose approved minutes name a person (by the stable pseudonym) and who she is. Every meeting has run. */
    private Named named(ProductMeetingPipeline pipeline) {
        for (Meeting meeting : meetings) {
            String minutes = new String(pipeline.blobs.get(URI.create(data(publishedFact(pipeline, meeting.id()))
                    .path("minutesRef").asText())).toCompletableFuture().join(), StandardCharsets.UTF_8);
            for (String name : meeting.personMarkers()) {
                String pseudonym = ProductMeetingPipeline.pseudonyms().labelStable(KnowledgeIngestPipeline.SCOPE.key(), "person",
                        name);
                if (minutes.contains(pseudonym)) {
                    return new Named(meeting, name, pseudonym);
                }
            }
        }
        throw new AssertionError("no meeting of the corpus names a person in its minutes");
    }

    private void runAll(ProductMeetingPipeline pipeline) throws Exception {
        for (Meeting meeting : meetings) {
            pipeline.run(meeting, Duration.ofSeconds(60));
        }
    }

    private KnowledgeIngestPipeline ingestion(ProductMeetingPipeline pipeline) throws Exception {
        return new KnowledgeIngestPipeline(pipeline.blobs, new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()));
    }

    private static CloudEvent request(String id, String type, ObjectNode data) {
        return CloudEventBuilder.v1().withId(id).withSource(URI.create("https://dpo-channel.example")).withType(type)
                .withSubject(id).withDataContentType("application/json").withData(Json.bytes(data)).build();
    }

    private static CloudEvent erasureOf(String requestId, URI subject) {
        ObjectNode data = Json.obj().put("requestId", requestId).put("kind", "person").put("passages", "LINKED")
                .put("label", "restricted");
        data.put("personRef", subject.toString());
        return request("erase-" + requestId, KnowledgeIngestPipeline.ERASURE_REQUESTED, data);
    }

    private static CloudEvent documentErasureOf(String requestId, String documentId) {
        ObjectNode data = Json.obj().put("requestId", requestId).put("kind", "document").put("documentId", documentId)
                .put("label", "confidential");
        return request("erase-" + requestId, KnowledgeIngestPipeline.ERASURE_REQUESTED, data);
    }

    private URI subject(ProductMeetingPipeline pipeline, String name) {
        return pipeline.blobs.put(name.getBytes(StandardCharsets.UTF_8), "erasure/subject-" + Math.abs(name.hashCode()), SUBJECT)
                .toCompletableFuture().join();
    }

    private static boolean exists(BlobStore blobs, URI uri) {
        try {
            blobs.get(uri).toCompletableFuture().join();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** The first meeting's minutes published a second time as another meeting: the same person is in both documents. */
    private CloudEvent secondPublicationOf(ProductMeetingPipeline pipeline, CloudEvent first) {
        JsonNode fact = data(first);
        byte[] minutes = pipeline.blobs.get(URI.create(fact.path("minutesRef").asText())).toCompletableFuture().join();
        URI copy = pipeline.blobs.put(minutes, "minutes/" + fact.path("meetingId").asText() + "-again.md",
                new RetentionClass("minutes", Duration.ofDays(90))).toCompletableFuture().join();
        ObjectNode again = ((ObjectNode) fact.deepCopy()).put("meetingId", fact.path("meetingId").asText() + "-again")
                .put("minutesRef", copy.toString());
        return CloudEventBuilder.v1(first).withId("pub-" + again.path("meetingId").asText())
                .withSubject(again.path("meetingId").asText()).withData(Json.bytes(again)).build();
    }

    private static long entries(KnowledgeIngestPipeline ingest) {
        return ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 100_000).toCompletableFuture().join().size();
    }

    @Test
    void ca1_a_person_named_in_two_meetings_is_erased_everywhere_by_who_she_is_and_the_subject_is_forgotten() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            runAll(pipeline);
            Named person = named(pipeline);
            Meeting named = person.meeting();
            Meeting other = meetings.stream().filter(m -> !m.id().equals(named.id())).findFirst().orElseThrow();
            CloudEvent first = publishedFact(pipeline, named.id());
            CloudEvent again = secondPublicationOf(pipeline, first);
            CloudEvent unrelated = publishedFact(pipeline, other.id());
            for (CloudEvent fact : List.of(first, again, unrelated)) {
                assertThat(ingest.ingest(fact, Duration.ofSeconds(60)).status()).isEqualTo(InstanceStatus.COMPLETED);
            }
            URI minutesOfFirst = URI.create(data(first).path("minutesRef").asText());
            URI minutesOfAgain = URI.create(data(again).path("minutesRef").asText());
            URI minutesOfOther = URI.create(data(unrelated).path("minutesRef").asText());
            String name = person.name();
            String pseudonym = person.pseudonym();
            assertThat(ingest.store.dump(KnowledgeIngestPipeline.SCOPE)).contains(pseudonym);   // one node for both documents
            URI subject = subject(pipeline, name);

            InstanceSnapshot done = ingest.erase(erasureOf("er-1", subject), Duration.ofSeconds(60));

            assertThat(done.status()).as("at %s", done.position()).isEqualTo(InstanceStatus.COMPLETED);
            List<CloudEvent> completed = ingest.emitted().stream().filter(e -> e.getType().equals(COMPLETED)).toList();
            assertThat(completed).hasSize(1);
            JsonNode report = data(completed.getFirst());
            assertThat(report.path("verified").asBoolean()).isTrue();
            assertThat(report.path("tombstone").asBoolean()).isTrue();
            assertThat(report.path("counts").has("ERASE.ENTITY")).isTrue();
            assertThat(report.path("counts").has("ERASE.PASSAGE")).isTrue();
            assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(INCOMPLETE))).isEmpty();

            // the person is nowhere: not in the base, not in the blobs the documents came from, not in the log (L2: opaque ids)
            assertThat(ingest.store.dump(KnowledgeIngestPipeline.SCOPE)).doesNotContain(pseudonym).doesNotContain(name);
            assertThat(exists(pipeline.blobs, minutesOfFirst)).isFalse();
            assertThat(exists(pipeline.blobs, minutesOfAgain)).isFalse();
            assertThat(exists(pipeline.blobs, subject)).isFalse();   // forgotten once the look was clean
            assertThat(exists(pipeline.blobs, minutesOfOther)).isTrue();
            try (var files = Files.list(work.resolve("blobs"))) {
                for (Path file : files.toList()) {
                    if (file.getFileName().toString().contains("knowledge")) {
                        assertThat(Files.readString(file)).doesNotContain(pseudonym).doesNotContain(name);
                    }
                }
            }
            // what was not about her stays and is still found
            assertThat(ingest.store.retrieve(KnowledgeIngestPipeline.SCOPE, new RetrievalQuery(
                    other.text().split("\n")[1], 5, Sensitivity.RESTRICTED)).toCompletableFuture().join()).isNotEmpty();
            // the log holds ERASE entries, ids only
            List<ChangeEntry> log = ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 100_000).toCompletableFuture().join();
            assertThat(log).anyMatch(e -> e.op() == ChangeEntry.Op.ERASE && "erasure:person".equals(e.reason()));
            assertThat(log.toString()).doesNotContain(name).doesNotContain(pseudonym);

            // CA8: nothing planted, no name and no pseudonym, in a fact, a snapshot or the audit
            List<String> places = new ArrayList<>();
            ingest.emitted().forEach(e -> places.add(new String(CeWire.toJson(e), StandardCharsets.UTF_8)));
            places.add(done.toString());
            places.add(ingest.audit.of(ingest.current).toCompletableFuture().join().toString());
            assertThat(places).allSatisfy(place -> assertThat(place).doesNotContain(name).doesNotContain(pseudonym)
                    .doesNotContain("Synthia"));

            // CA5: the same request again is the same instance and changes nothing
            long before = entries(ingest);
            int facts = ingest.emitted().size();
            assertThat(ingest.engine.startFromEvent(ingest.eraseDefinition.ref(), erasureOf("er-1", subject))
                    .toCompletableFuture().get(10, TimeUnit.SECONDS)).isEqualTo(ingest.current);
            assertThat(entries(ingest)).isEqualTo(before);
            assertThat(ingest.emitted()).hasSize(facts);
        }
    }

    @Test
    void ca2_a_document_is_erased_with_the_batch_and_the_minutes_and_the_other_document_stays() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            Meeting gone = meetings.getFirst();
            Meeting kept = meetings.get(1);
            pipeline.run(gone, Duration.ofSeconds(60));
            pipeline.run(kept, Duration.ofSeconds(60));
            CloudEvent goneFact = publishedFact(pipeline, gone.id());
            CloudEvent keptFact = publishedFact(pipeline, kept.id());
            ingest.ingest(goneFact, Duration.ofSeconds(60));
            ingest.ingest(keptFact, Duration.ofSeconds(60));
            URI minutesOfGone = URI.create(data(goneFact).path("minutesRef").asText());
            URI minutesOfKept = URI.create(data(keptFact).path("minutesRef").asText());

            InstanceSnapshot done = ingest.erase(documentErasureOf("er-d", "meeting:" + gone.id()), Duration.ofSeconds(60));

            assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode report = data(ingest.emitted().stream().filter(e -> e.getType().equals(COMPLETED)).findFirst().orElseThrow());
            assertThat(report.path("documentId").asText()).isEqualTo("meeting:" + gone.id());
            assertThat(report.path("verified").asBoolean()).isTrue();
            assertThat(exists(pipeline.blobs, minutesOfGone)).isFalse();
            assertThat(exists(pipeline.blobs, minutesOfKept)).isTrue();
            assertThat(ingest.store.dump(KnowledgeIngestPipeline.SCOPE)).doesNotContain("meeting:" + gone.id() + "|");
            assertThat(ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join().documents()).isEqualTo(1);

            // asked again for the same document with another request id: nothing to remove, and still clean
            ingest.erase(documentErasureOf("er-d2", "meeting:" + gone.id()), Duration.ofSeconds(60));
            assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(COMPLETED))).hasSize(2);
        }
    }

    @Test
    void ca6_a_person_erased_before_she_is_in_the_base_is_kept_out_of_the_meeting_that_names_her_later() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work); KnowledgeIngestPipeline ingest = ingestion(pipeline)) {
            runAll(pipeline);
            Named person = named(pipeline);
            Meeting later = person.meeting();
            String name = person.name();
            String pseudonym = person.pseudonym();

            InstanceSnapshot erased = ingest.erase(erasureOf("er-pre", subject(pipeline, name)), Duration.ofSeconds(60));
            assertThat(erased.status()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode report = data(ingest.emitted().stream().filter(e -> e.getType().equals(COMPLETED)).findFirst().orElseThrow());
            assertThat(report.path("counts").size()).isZero();   // nothing was there

            InstanceSnapshot ingested = ingest.ingest(publishedFact(pipeline, later.id()), Duration.ofSeconds(60));

            assertThat(ingested.status()).isEqualTo(InstanceStatus.COMPLETED);
            assertThat(ingest.store.dump(KnowledgeIngestPipeline.SCOPE)).doesNotContain(EntityIds.of("Person", pseudonym) + "|Person");
            assertThat(ingest.store.entities(KnowledgeIngestPipeline.SCOPE, new io.stintflow.knowledge.EntityQuery("Person",
                    EntityIds.normalize(pseudonym), Sensitivity.RESTRICTED, 5)).toCompletableFuture().join()).isEmpty();
        }
    }

    @Test
    void ca7_an_erasure_that_left_a_blob_behind_is_not_reported_as_done_and_the_subject_is_kept() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work)) {
            // a blob store whose delete answers yes and does nothing
            BlobStore lying = (BlobStore) Proxy.newProxyInstance(BlobStore.class.getClassLoader(),
                    new Class<?>[] {BlobStore.class}, (proxy, method, args) -> method.getName().equals("delete")
                            ? CompletableFuture.completedFuture(null) : method.invoke(pipeline.blobs, args));
            try (KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(lying,
                    new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
                runAll(pipeline);
                Named person = named(pipeline);
                Meeting meeting = person.meeting();
                ingest.ingest(publishedFact(pipeline, meeting.id()), Duration.ofSeconds(60));
                URI subject = subject(pipeline, person.name());

                InstanceSnapshot done = ingest.erase(erasureOf("er-lie", subject), Duration.ofSeconds(60));

                assertThat(done.status()).isEqualTo(InstanceStatus.COMPLETED);
                assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(COMPLETED))).isEmpty();
                JsonNode alarm = data(ingest.emitted().stream().filter(e -> e.getType().equals(INCOMPLETE)).findFirst().orElseThrow());
                assertThat(alarm.path("requestId").asText()).isEqualTo("er-lie");
                assertThat(alarm.path("remaining").path("BLOB").asInt()).isPositive();
                assertThat(alarm.path("tombstone").asBoolean()).isTrue();   // the base itself is clean
                assertThat(alarm.toString()).doesNotContain(person.name());
                assertThat(exists(pipeline.blobs, subject)).isTrue();   // not forgotten: the erasure is not done
            }
        }
    }

    // --- the reindexation --------------------------------------------------------------------------------------------

    private static final class Other implements Embedder {
        @Override
        public float[] embed(String text) {
            float[] v = new float[16];
            for (String w : EntityIds.normalize(text).split(" ")) {
                if (!w.isEmpty()) {
                    v[Math.floorMod(w.hashCode(), 16)] += 1f;
                }
            }
            return v;
        }

        @Override
        public int dimension() {
            return 16;
        }

        @Override
        public String model() {
            return "other-bag-16";
        }

        @Override
        public Set<String> languages() {
            return Set.of("en");
        }
    }

    private static CloudEvent embeddingChanged(String id, String requestId, String model) {
        return request(id, KnowledgeIngestPipeline.EMBEDDING_CHANGED,
                Json.obj().put("requestId", requestId).put("targetModel", model).put("label", "confidential"));
    }

    @Test
    void ca3_a_change_of_model_moves_the_base_and_only_the_new_vectors_are_consulted() throws Exception {
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work);
             KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(pipeline.blobs,
                     new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()), InstantSource.system(),
                     new InMemoryTimerService(), true, new Other())) {
            for (Meeting meeting : meetings.subList(0, 3)) {
                pipeline.run(meeting, Duration.ofSeconds(60));
                ingest.ingest(publishedFact(pipeline, meeting.id()), Duration.ofSeconds(60));
            }
            var question = new RetrievalQuery("checkout service retry policy", 5, Sensitivity.RESTRICTED);
            List<String> before = ingest.store.retrieve(KnowledgeIngestPipeline.SCOPE, question).toCompletableFuture().join()
                    .stream().map(i -> i.origin().id()).toList();
            assertThat(ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join().model())
                    .isEqualTo(new DeterministicEmbedder().model());
            int passages = ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join().passages();

            // a worker that serves another model refuses to move the base to the one asked
            InstanceSnapshot refused = ingest.reindex(embeddingChanged("ev-0", "ri-0", "some-other-model"), Duration.ofSeconds(30));
            assertThat(refused.status()).isEqualTo(InstanceStatus.FAILED);
            assertThat(ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join().model())
                    .isEqualTo(new DeterministicEmbedder().model());

            InstanceSnapshot done = ingest.reindex(embeddingChanged("ev-1", "ri-1", "other-bag-16"), Duration.ofSeconds(60));

            assertThat(done.status()).as("at %s", done.position()).isEqualTo(InstanceStatus.COMPLETED);
            JsonNode fact = data(ingest.emitted().stream().filter(e -> e.getType().equals(REINDEXED)).findFirst().orElseThrow());
            assertThat(fact.path("status").asText()).isEqualTo("APPLIED");
            assertThat(fact.path("to").asText()).isEqualTo("other-bag-16");
            assertThat(fact.path("passages").asInt()).isEqualTo(passages);
            BaseInfo info = ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join();
            assertThat(info.model()).isEqualTo("other-bag-16");
            assertThat(info.dimension()).isEqualTo(16);
            assertThat(info.passages()).isEqualTo(passages);
            assertThat(ingest.store.retrieve(KnowledgeIngestPipeline.SCOPE, question).toCompletableFuture().join().stream()
                    .map(i -> i.origin().id()).toList()).isNotEmpty().containsAnyElementsOf(before);
            assertThat(ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 100_000).toCompletableFuture().join())
                    .filteredOn(e -> e.op() == ChangeEntry.Op.REINDEX).hasSize(1);

            // the same change again, as another fact: the base is already on the model
            long reindexes = ingest.emitted().stream().filter(e -> e.getType().equals(REINDEXED)).count();
            ingest.reindex(embeddingChanged("ev-2", "ri-2", "other-bag-16"), Duration.ofSeconds(60));
            JsonNode again = data(ingest.emitted().stream().filter(e -> e.getType().equals(REINDEXED)).reduce((a, b) -> b).orElseThrow());
            assertThat(again.path("status").asText()).isEqualTo("UNCHANGED");
            assertThat(ingest.emitted().stream().filter(e -> e.getType().equals(REINDEXED)).count()).isEqualTo(reindexes + 1);
            assertThat(ingest.store.changes(KnowledgeIngestPipeline.SCOPE, 0, 100_000).toCompletableFuture().join())
                    .filteredOn(e -> e.op() == ChangeEntry.Op.REINDEX).hasSize(1);

            // and a meeting ingested after is embedded by the new model, found by it
            Meeting next = meetings.get(3);
            pipeline.run(next, Duration.ofSeconds(60));
            assertThat(ingest.ingest(publishedFact(pipeline, next.id()), Duration.ofSeconds(60)).status())
                    .isEqualTo(InstanceStatus.COMPLETED);
            assertThat(ingest.store.describe(KnowledgeIngestPipeline.SCOPE).toCompletableFuture().join().model())
                    .isEqualTo("other-bag-16");
        }
    }
}
