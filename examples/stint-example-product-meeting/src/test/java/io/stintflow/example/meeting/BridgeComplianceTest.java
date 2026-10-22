package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.EntityIds;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.Source;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.knowledge.AuditTrail;
import io.stintflow.worker.knowledge.ChannelAttestation;
import io.stintflow.worker.knowledge.ResolvePointersHandler;

/**
 * SDD 3c.5 — the compliance of the bridge: who is denied, what is never exposed, what the audit says of each resolution, and the
 * trail of a whole execution of the scenario. Fake models, no sleep; synthetic data only.
 */
class BridgeComplianceTest {

    @TempDir
    Path work;

    private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-10-22T09:00:00Z"));

    private static ObjectNode ask(String pointer) {
        ObjectNode input = Json.obj().put("depth", 0);
        input.putArray("pointers").add(pointer);
        return input;
    }

    private static JsonNode firstResult(JsonNode out) {
        return out.path("results").get(0);
    }

    private static JsonNode resolveAs(ArchitectureAdrPipeline p, String definition, String pointer) {
        return p.resolveHandler.execute(new TaskContext("t", "bridge-" + definition, "c", 1, ask(pointer),
                new WorkflowRef("examples", definition, "1.0.0"))).toCompletableFuture().join();
    }

    @Test
    void ca1_a_consumer_the_contract_does_not_list_is_denied_the_way_a_node_that_does_not_exist_is() throws Exception {
        try (ArchitectureAdrPipeline p = new ArchitectureAdrPipeline(work, now::get, new InMemoryTimerService(now::get))) {
            String pointer = ArchitectureAdrTest.productDecision(p, "sync", "Keep the checkout synchronous", ArchitectureAdrTest.PRODUCT_SECRET);
            String nothing = GraphPointer.of(ArchitectureAdrPipeline.PRODUCT, EntityIds.of("Decision", "never-written"), 1).format();

            JsonNode stranger = firstResult(resolveAs(p, "somebody-else", pointer));
            JsonNode missing = firstResult(resolveAs(p, "architecture-adr", nothing));

            assertThat(stranger.path("status").asText()).isEqualTo("DENIED");
            assertThat(missing.path("status").asText()).isEqualTo("DENIED");
            assertThat(stranger.has("view")).isFalse();
            assertThat(p.audit.of("bridge-somebody-else").toCompletableFuture().join()).anySatisfy(r ->
                    assertThat(r.detail()).containsEntry("requester", "examples:somebody-else").containsEntry("denied", "1"));
        }
    }

    @Test
    void ca2_a_restricted_node_is_denied_to_a_consumer_the_contract_lists() throws Exception {
        try (ArchitectureAdrPipeline p = new ArchitectureAdrPipeline(work, now::get, new InMemoryTimerService(now::get))) {
            String id = EntityIds.of("Decision", "secret");
            Entity entity = new Entity(id, "Decision", "A restricted decision", Map.of("key", "secret", "summary", "SYNTH-RESTRICTED",
                    "status", "accepted"), Sensitivity.RESTRICTED);
            p.product.write(ArchitectureAdrPipeline.PRODUCT, new KnowledgeBatch("t:secret", "product-doc:secret",
                    PassageIds.sha256Hex(entity.toString()), 1L, Source.NONE, "meetings@1.0.0", List.of(), List.of(entity), List.of(),
                    List.of())).toCompletableFuture().join();

            JsonNode result = firstResult(resolveAs(p, "architecture-adr", GraphPointer.of(ArchitectureAdrPipeline.PRODUCT, id, 1).format()));

            assertThat(result.path("status").asText()).isEqualTo("DENIED");
            assertThat(result.toString()).doesNotContain("SYNTH-RESTRICTED").doesNotContain("A restricted decision");
        }
    }

    @Test
    void ca7_ca8_the_audit_cites_the_contract_and_the_channel_and_an_unattested_channel_is_refused() throws Exception {
        try (ArchitectureAdrPipeline p = new ArchitectureAdrPipeline(work, now::get, new InMemoryTimerService(now::get))) {
            String pointer = ArchitectureAdrTest.productDecision(p, "sync", "Keep the checkout synchronous", ArchitectureAdrTest.PRODUCT_SECRET);
            var contract = ProductBridge.contract();
            TaskContext asAdr = new TaskContext("t", "attested", "c", 1, ask(pointer), new WorkflowRef("examples", "architecture-adr", "1.0.0"));
            ResolvePointersHandler attested = new ResolvePointersHandler(p.product, p.product, contract.policy(), p.blobs, null, p.audit,
                    now::get, new ChannelAttestation("sqs-iam", true));
            ResolvePointersHandler notAttested = new ResolvePointersHandler(p.product, p.product, contract.policy(), p.blobs, null,
                    p.audit, now::get, new ChannelAttestation("sqs-open", false));

            attested.execute(asAdr).toCompletableFuture().join();
            Throwable refused = org.junit.jupiter.api.Assertions.assertThrows(CompletionException.class,
                    () -> notAttested.execute(new TaskContext("t", "unattested", "c", 1, ask(pointer),
                            new WorkflowRef("examples", "architecture-adr", "1.0.0"))).toCompletableFuture().join()).getCause();

            assertThat(p.audit.of("attested").toCompletableFuture().join()).anySatisfy(r -> assertThat(r.detail())
                    .containsEntry("contract", "1.0.0").containsEntry("contractHash", contract.contentHash())
                    .containsEntry("channel", "sqs-iam").containsEntry("allowSensitive", "true"));
            assertThat(((TaskFailure) refused).toErrorInfo().status()).isEqualTo(403);
            assertThat(p.audit.of("unattested").toCompletableFuture().join()).singleElement().satisfies(r -> {
                assertThat(r.outcome()).isEqualTo("refused");
                assertThat(r.detail()).containsEntry("channel", "sqs-open").doesNotContainKey("resolved");
            });
        }
    }

    @Test
    void ca3_ca9_a_whole_execution_of_the_scenario_leaves_a_complete_trail_with_no_text() throws Exception {
        List<Meeting> meetings = Corpus.load();
        Meeting meeting = meetings.getFirst();
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, ProductMeetingPipeline.fakes(meetings),
                new RunMetrics(Map.of(), false), work.resolve("product"));
                KnowledgeIngestPipeline bridge = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()));
                ArchitectureAdrPipeline adr = new ArchitectureAdrPipeline(work.resolve("adr"), now::get, new InMemoryTimerService(now::get))) {
            AuditTrail.Builder trail = AuditTrail.builder();
            String minutes = pipeline.run(meeting, Duration.ofSeconds(60)).instanceId();
            trail.add(pipeline.audit, minutes);
            CloudEvent published = pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                    .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow();
            assertThat(bridge.ingest(published, Duration.ofSeconds(60)).status()).isEqualTo(io.stintflow.spi.InstanceSnapshot.InstanceStatus.COMPLETED);
            String ingest = bridge.current;
            CloudEvent indexed = last(bridge, "io.stintflow.knowledge.document-indexed.v1");
            bridge.announce(indexed, Duration.ofSeconds(60));
            String announce = bridge.current;
            bridge.link(last(bridge, "io.stintflow.knowledge.nodes-published.v1"), Duration.ofSeconds(60));
            String link = bridge.current;
            CloudEvent linked = last(bridge, "io.stintflow.knowledge.references-linked.v1");
            bridge.resolve(linked, Duration.ofSeconds(60));
            String resolve = bridge.current;
            bridge.catalogLinkProduct(indexed, Duration.ofSeconds(60));
            String catalogProduct = bridge.current;
            bridge.catalogLinkArchitecture(linked, Duration.ofSeconds(60));
            String catalogArchitecture = bridge.current;
            trail.add(bridge.audit, ingest, announce, link, resolve, catalogProduct, catalogArchitecture);

            String pointer = ArchitectureAdrTest.productDecision(adr, "sync", "Keep the checkout synchronous", ArchitectureAdrTest.PRODUCT_SECRET);
            java.net.URI draft = adr.draft("arch-trail", ArchitectureAdrTest.decisions(ArchitectureAdrTest.decision("async-checkout",
                    "Make the checkout asynchronous", "Make the checkout asynchronous through a queue", null, pointer)));
            String id = adr.start(ArchitectureAdrPipeline.drafted("arch-trail", draft, null));
            adr.await(id, Duration.ofSeconds(60), request -> "conflicts");
            trail.add(adr.audit, id);

            AuditTrail all = trail.build();
            assertThat(all.covers(Set.of("publish", "knowledge-write", "knowledge-announce", "knowledge-reference", "pointer-resolve",
                    "catalog-lookup", "catalog-link", "adr-write"))).as("kinds: %s", all.kinds()).isTrue();
            assertThat(all.records()).extracting(r -> r.at()).isSorted();
            String dump = all.records().toString();
            List<String> planted = new java.util.ArrayList<>(Corpus.allMarkers(meetings));
            planted.add("SYNTH-PRODUCT-SUMMARY");
            planted.add("Make the checkout asynchronous through a queue");
            planted.forEach(text -> assertThat(dump).as("the trail holds no %s", text).doesNotContain(text));
        }
    }

    /** The last fact of {@code type}; a fact is published a moment after its instance is saved as ended, so it is waited for. */
    private static CloudEvent last(KnowledgeIngestPipeline bridge, String type) {
        long until = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < until) {
            var found = bridge.emitted().stream().filter(e -> e.getType().equals(type)).reduce((a, b) -> b);
            if (found.isPresent()) {
                return found.get();
            }
            java.util.concurrent.locks.LockSupport.parkNanos(10_000_000L);
        }
        throw new AssertionError("No fact " + type + " was published");
    }
}
