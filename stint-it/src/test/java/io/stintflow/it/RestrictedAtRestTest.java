package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.policy.RestrictedContentStateStore;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.CeWire;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;
import io.stintflow.worker.redaction.LayeredRedactor;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionHandler;

/**
 * SDD 3a.5, RF6 (decision 8e, E1): a restricted instance's input, context and task inputs never rest in the state
 * store nor ride the queue — they rest in blobs of the {@code restricted} class (the policy's TTL), and the snapshot and
 * the invoke carry pointers. A restricted fact travels by {@code dataref}, and the instance it starts reads the data
 * back. Content gone past its TTL fails the instance; a redaction that lowers the level brings the content back inline;
 * a pointer to restricted content is refused on a route restricted data may not take. Synthetic data, controllable
 * clock.
 */
class RestrictedAtRestTest {

    static final WorkflowRef SUMMARIZE = new WorkflowRef("meetings", "summarize", "1.0.0");
    static final String MARKER = "SYNTH-PII-0101";

    static final String POLICY = """
            routes:
              restricted: [redactor, summarize-local]
              confidential: [redactor, summarize-local, summarize-remote]
              internal: ['*']
            retention:
              restricted: { days: 2 }""";

    static final String WAITING = SensitivityPolicyTest.header("summarize", POLICY) + """
            do:
              - ask:
                  call: remote
                  with:
                    task: summarize-local
                    input: ${ . }
            """;

    static final String LISTENING = SensitivityPolicyTest.header("summarize", POLICY) + """
            do:
              - wait:
                  listen:
                    to:
                      one:
                        with: { type: io.example.meeting.note.v1 }
                        correlate:
                          instance: { from: '${ .data.id }', expect: '${ $workflow.id }' }
            """;

    private static void awaitWaiting(PolicyFixture f, String id) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < until && f.state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS)
                .map(InstanceSnapshot::waitingKey).isEmpty()) {
            Thread.onSpinWait();
        }
        assertThat(f.snapshot(id).waitingKey()).isNotNull();
    }

    private static boolean atRest(JsonNode node) {
        return node != null && node.has(RestrictedContentStateStore.AT_REST_FIELD);
    }

    /** Every blob file holding {@code marker}. */
    private static List<String> blobsHolding(PolicyFixture f, String marker) throws Exception {
        try (Stream<Path> files = Files.list(f.blobDir)) {
            return files.filter(p -> {
                try {
                    return new String(Files.readAllBytes(p), StandardCharsets.UTF_8).contains(marker);
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }).map(p -> p.getFileName().toString()).toList();
        }
    }

    @Test
    void a_restricted_instance_s_content_rests_only_in_restricted_blobs_and_the_invoke_carries_a_pointer()
            throws Exception {
        PolicyFixture f = new PolicyFixture(WAITING);

        String id = f.engine.start(SUMMARIZE, Json.obj().put("transcript", "Speaker A: " + MARKER));
        awaitWaiting(f, id);

        InstanceSnapshot stored = f.snapshot(id);
        assertThat(stored.sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(atRest(stored.input())).isTrue();
        assertThat(atRest(stored.context())).isTrue();
        assertThat(stored.toString()).doesNotContain(MARKER);

        JsonNode wire = f.dispatched.getFirst().input();
        assertThat(ClaimCheck.isPointer(wire)).isTrue();
        assertThat(wire.toString()).doesNotContain(MARKER).contains("retention_restricted_");
        assertThat(blobsHolding(f, MARKER)).isNotEmpty().allMatch(name -> name.startsWith("retention_restricted_"));

        RestrictedContentStateStore view = new RestrictedContentStateStore(f.state, f.blobs,
                ref -> RetentionClass.RESTRICTED_DEFAULT, f.now::get);
        InstanceSnapshot restored = view.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
        assertThat(restored.input().path("transcript").asText()).isEqualTo("Speaker A: " + MARKER);
        assertThat(restored.context().toString()).contains(MARKER);
    }

    @Test
    void content_gone_past_the_policy_s_ttl_fails_the_instance_and_is_never_read() throws Exception {
        PolicyFixture f = new PolicyFixture(LISTENING);
        String id = f.engine.start(SUMMARIZE, Json.obj().put("transcript", MARKER));
        awaitWaiting(f, id);

        f.now.set(PolicyFixture.T0.plus(Duration.ofDays(2)).plusSeconds(1));   // the policy's restricted TTL: 2 days
        CloudEvent note = CloudEventBuilder.v1().withId("note-1").withSource(URI.create("https://meetings.example"))
                .withType("io.example.meeting.note.v1").withDataContentType("application/json")
                .withData(Json.bytes(Json.obj().put("id", id)))
                .withExtension(StintEvents.EXT_SENSITIVITY, "internal").build();
        f.engine.onDomainEvent(note, Duration.ofSeconds(5)).toCompletableFuture().get(10, TimeUnit.SECONDS);

        InstanceSnapshot failed = f.snapshot(id);
        assertThat(failed.status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(failed.input()).isNull();
        assertThat(failed.context()).isNull();
        assertThat(blobsHolding(f, MARKER)).isEmpty();   // read past its TTL: deleted
    }

    @Test
    void a_redaction_that_lowers_the_level_brings_the_context_back_inline_and_the_raw_input_stays_at_rest()
            throws Exception {
        PolicyFixture f = new PolicyFixture(SensitivityPolicyTest.header("summarize", POLICY) + """
                do:
                  - redact:
                      call: redact
                      with:
                        task: redactor
                        input: ${ .transcript }
                        lowersTo: confidential
                      export: { as: '${ {transcript: .output} }' }
                  - summarize:
                      call: remote
                      with:
                        task: summarize-remote
                        input: ${ .output }
                """).handler("redactor", new RedactionHandler(LayeredRedactor.reference(),
                new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8))));

        String id = f.engine.start(SUMMARIZE, Json.obj().put("transcript", "mail " + MARKER + "@example.com"));
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (f.dispatched.size() < 2 && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
        awaitWaiting(f, id);

        InstanceSnapshot stored = f.snapshot(id);
        assertThat(stored.sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(atRest(stored.context())).isFalse();
        assertThat(stored.context().toString()).contains("EMAIL_").doesNotContain(MARKER);
        assertThat(atRest(stored.input())).isTrue();   // never redacted: it stays where it rests (sec. 9)
        assertThat(stored.toString()).doesNotContain(MARKER);
        assertThat(ClaimCheck.isPointer(f.dispatched.get(0).input())).isTrue();    // to the redactor: restricted
        assertThat(ClaimCheck.isPointer(f.dispatched.get(1).input())).isFalse();   // after it: confidential, inline
        assertThat(f.dispatched.get(1).input().toString()).doesNotContain(MARKER);
    }

    @Test
    void a_restricted_fact_travels_by_dataref_and_the_instance_it_starts_reads_the_data_back() throws Exception {
        String announcer = SensitivityPolicyTest.header("announce", POLICY) + """
                do:
                  - announce:
                      emit:
                        event:
                          with:
                            source: https://meetings.example
                            type: io.example.meeting.transcribed.v1
                            data: '${ {text: .transcript} }'
                """;
        String follower = SensitivityPolicyTest.header("follow", POLICY) + """
                do:
                  - read:
                      set: '${ {got: .[0].data.text} }'
                      export: { as: '${ . }' }
                """;
        PolicyFixture f = new PolicyFixture(announcer, follower);

        String id = f.engine.start(new WorkflowRef("meetings", "announce", "1.0.0"),
                Json.obj().put("transcript", MARKER));
        f.awaitCompleted(id);
        CloudEvent fact = f.publisher.published().getFirst();

        assertThat(fact.getData()).isNull();
        assertThat(fact.getExtension("dataref").toString()).contains("retention_restricted_");
        assertThat(new String(CeWire.toJson(fact), StandardCharsets.UTF_8)).doesNotContain(MARKER);
        assertThat(fact.getExtension(StintEvents.EXT_SENSITIVITY)).isEqualTo("restricted");

        String followerId = f.engine.startFromEvent(new WorkflowRef("meetings", "follow", "1.0.0"), fact)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(f.awaitCompleted(followerId).path("got").asText()).isEqualTo(MARKER);
        InstanceSnapshot followed = f.snapshot(followerId);
        assertThat(followed.sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(followed.toString()).doesNotContain(MARKER);
    }

    @Test
    void a_pointer_to_restricted_content_is_refused_on_a_route_restricted_data_may_not_take() throws Exception {
        PolicyFixture f = new PolicyFixture(SensitivityPolicyTest.header("summarize", POLICY) + """
                do:
                  - guarded:
                      try:
                        - summarize:
                            call: remote
                            with:
                              task: summarize-remote
                              input: ${ . }
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/authorization
                        do:
                          - note:
                              set: '${ {refused: .status} }'
                  - outcome:
                      set: '${ . }'
                      export:
                        as: '${ . }'
                """).worker("summarize-remote", in -> Json.obj());

        String id = f.engine.start(SUMMARIZE, Json.obj().put("ref",
                f.blobDir.resolve("retention_restricted_instances_x_v1.json").toUri().toString()),
                Sensitivity.CONFIDENTIAL);

        assertThat(f.awaitCompleted(id).path("refused").asInt()).isEqualTo(403);
        assertThat(f.dispatched).isEmpty();
        assertThat(f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS)).filteredOn(
                r -> AuditRecord.POLICY.equals(r.kind())).singleElement().satisfies(r -> assertThat(
                        r.detail().get("reason")).contains("pointer to restricted content"));
    }

    @Test
    void deleting_an_instance_erases_its_content_blob() throws Exception {
        PolicyFixture f = new PolicyFixture(WAITING);
        String id = f.engine.start(SUMMARIZE, Json.obj().put("transcript", MARKER));
        awaitWaiting(f, id);
        assertThat(blobsHolding(f, MARKER)).isNotEmpty();

        RestrictedContentStateStore view = new RestrictedContentStateStore(f.state, f.blobs,
                ref -> RetentionClass.RESTRICTED_DEFAULT, f.now::get);
        view.delete(id).toCompletableFuture().get(5, TimeUnit.SECONDS);

        assertThat(f.state.load(id).toCompletableFuture().get(5, TimeUnit.SECONDS)).isEmpty();
        // the instance's content blob is gone; what remains is the invoke's claim-check, left to its retention
        assertThat(blobsHolding(f, MARKER)).allMatch(name -> name.contains("_ask_"));
    }
}
