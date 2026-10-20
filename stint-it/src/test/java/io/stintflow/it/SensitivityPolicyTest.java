package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.dsl.DslValidationException;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.TaskInvocation;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;

/**
 * SDD 3a.5, RF1–RF4 and RF8 (engine side): an instance's level comes from its trigger (extension, the policy's
 * expression, else restricted), only rises, travels on dispatches and facts; the policy refuses a route the level
 * doesn't allow — {@code authorization} 403, caught like any error — and the audit trail records it. Synthetic data.
 */
class SensitivityPolicyTest {

    static final WorkflowRef SUMMARIZE = new WorkflowRef("meetings", "summarize", "1.0.0");

    static String header(String name, String policy) {
        return """
                document:
                  dsl: '1.0.0'
                  namespace: meetings
                  name: %s
                  version: '1.0.0'
                  metadata:
                    x-stint-policy:
                %s""".formatted(name, policy.indent(6).stripTrailing()) + "\n";
    }

    static final String POLICY = """
            sensitivity:
              from: '${ .[0].data.label }'
            routes:
              restricted: [summarize-local]
              confidential: [summarize-local, summarize-remote]
              internal: ['*']""";

    /** The remote summary in a try; a refusal is caught and goes to the fallback. */
    static final String GUARDED = header("summarize", POLICY) + """
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
                    then: fallback
              - remote:
                  set: '${ {done: "remote"} }'
                  export: { as: '${ $context + . }' }
                  then: end
              - fallback:
                  set: '${ {done: "refused"} }'
                  export: { as: '${ $context + . }' }
            """;

    static CloudEvent event(String id, String sensitivity, String label) {
        CloudEventBuilder builder = CloudEventBuilder.v1().withId(id).withSource(URI.create("https://meetings.example"))
                .withType("io.example.meeting.recorded.v1").withDataContentType("application/json")
                .withData(Json.bytes(label == null ? Json.obj() : Json.obj().put("label", label)));
        if (sensitivity != null) {
            builder.withExtension(StintEvents.EXT_SENSITIVITY, sensitivity);
        }
        return builder.build();
    }

    private static List<AuditRecord> policyRecords(PolicyFixture f, String id) throws Exception {
        return f.audit.of(id).toCompletableFuture().get(5, TimeUnit.SECONDS).stream()
                .filter(r -> r.kind().equals(AuditRecord.POLICY)).toList();
    }

    @Test
    void without_a_level_an_instance_is_restricted_and_a_remote_route_is_refused_with_a_catchable_403()
            throws Exception {
        PolicyFixture f = new PolicyFixture(GUARDED).worker("summarize-remote", in -> Json.obj().put("ok", true));

        String id = f.engine.start(SUMMARIZE, Json.obj().put("text", "SYNTH-PII-0001"));
        JsonNode done = f.awaitCompleted(id);

        assertThat(done.path("done").asText()).isEqualTo("refused");
        assertThat(f.snapshot(id).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(f.dispatched).isEmpty();                      // the data never left: nothing was dispatched
        assertThat(policyRecords(f, id)).singleElement().satisfies(r -> {
            assertThat(r.outcome()).isEqualTo("refused");
            assertThat(r.route()).isEqualTo("summarize-remote");
            assertThat(r.sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
            assertThat(r.detail().get("reason")).isEqualTo("route 'summarize-remote' is not allowed for restricted data");
            assertThat(r.toString()).doesNotContain("SYNTH-PII");
        });
    }

    @Test
    void an_allowed_route_is_dispatched_with_the_level_and_audited() throws Exception {
        PolicyFixture f = new PolicyFixture(GUARDED).worker("summarize-remote", in -> Json.obj().put("ok", true));

        String id = f.engine.start(SUMMARIZE, Json.obj().put("text", "synthetic"), Sensitivity.CONFIDENTIAL);

        assertThat(f.awaitCompleted(id).path("done").asText()).isEqualTo("remote");
        assertThat(f.dispatched).singleElement().extracting(TaskInvocation::sensitivity)
                .isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(policyRecords(f, id)).singleElement().extracting(AuditRecord::outcome).isEqualTo("allowed");
    }

    @Test
    void the_trigger_s_level_comes_from_its_extension_else_the_policy_s_expression_else_restricted()
            throws Exception {
        PolicyFixture f = new PolicyFixture(header("summarize", POLICY) + """
                do:
                  - wait:
                      call: remote
                      with:
                        task: summarize-local
                """);

        String byExtension = start(f, event("e-1", "internal", "restricted"));
        String byLabel = start(f, event("e-2", null, "confidential"));
        String byNothing = start(f, event("e-3", null, null));
        String byUnknownLabel = start(f, event("e-4", null, "secret"));
        String byUnreadableExtension = start(f, event("e-5", "bogus", "internal"));

        assertThat(f.snapshot(byExtension).sensitivity()).isEqualTo(Sensitivity.INTERNAL);
        assertThat(f.snapshot(byLabel).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(f.snapshot(byNothing).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(f.snapshot(byUnknownLabel).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(f.snapshot(byUnreadableExtension).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
    }

    private static String start(PolicyFixture f, CloudEvent trigger) throws Exception {
        return f.engine.startFromEvent(SUMMARIZE, trigger).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void an_uncaught_refusal_fails_the_instance() throws Exception {
        PolicyFixture f = new PolicyFixture(header("summarize", POLICY) + """
                do:
                  - summarize:
                      call: remote
                      with:
                        task: summarize-remote
                """);

        String id = f.engine.start(SUMMARIZE, Json.obj());

        assertThatThrownBy(() -> f.awaitCompleted(id)).isInstanceOf(Exception.class);
        assertThat(f.snapshot(id).status()).isEqualTo(InstanceStatus.FAILED);
        assertThat(f.dispatched).isEmpty();
    }

    @Test
    void a_task_s_own_ceiling_refuses_a_level_its_route_would_allow() throws Exception {
        PolicyFixture f = new PolicyFixture(header("summarize", POLICY) + """
                do:
                  - guarded:
                      try:
                        - summarize:
                            call: remote
                            with:
                              task: summarize-remote
                            metadata:
                              x-stint-policy:
                                maxSensitivity: internal
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/authorization
                        then: fallback
                  - remote:
                      set: '${ {done: "remote"} }'
                      export: { as: '${ $context + . }' }
                      then: end
                  - fallback:
                      set: '${ {done: "refused"} }'
                      export: { as: '${ $context + . }' }
                """).worker("summarize-remote", in -> Json.obj());

        String id = f.engine.start(SUMMARIZE, Json.obj(), Sensitivity.CONFIDENTIAL);

        assertThat(f.awaitCompleted(id).path("done").asText()).isEqualTo("refused");
        assertThat(policyRecords(f, id)).singleElement().satisfies(r -> assertThat(r.detail().get("reason"))
                .isEqualTo("the task accepts data up to internal, the instance is confidential"));
    }

    @Test
    void a_fact_carries_the_level_and_the_instance_it_starts_inherits_it() throws Exception {
        String announcer = header("announce", POLICY) + """
                do:
                  - announce:
                      emit:
                        event:
                          with:
                            source: https://meetings.example
                            type: io.example.meeting.summarized.v1
                            data: '${ {n: 1} }'
                """;
        String follower = header("follow", """
                routes:
                  restricted: [summarize-local]""") + """
                do:
                  - noop:
                      set: '${ . }'
                """;
        PolicyFixture f = new PolicyFixture(announcer, follower);

        String id = f.engine.start(new WorkflowRef("meetings", "announce", "1.0.0"), Json.obj(),
                Sensitivity.CONFIDENTIAL);
        f.awaitCompleted(id);
        CloudEvent fact = f.publisher.published().getFirst();
        String followerId = f.engine.startFromEvent(new WorkflowRef("meetings", "follow", "1.0.0"), fact)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);

        assertThat(fact.getExtension(StintEvents.EXT_SENSITIVITY)).isEqualTo("confidential");
        assertThat(f.snapshot(followerId).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
    }

    @Test
    void an_event_can_raise_the_level_but_never_lower_it() throws Exception {
        String listener = header("listen", POLICY) + """
                do:
                  - wait:
                      listen:
                        to:
                          one:
                            with: { type: io.example.meeting.note.v1 }
                            correlate:
                              instance: { from: '${ .data.id }', expect: '${ $workflow.id }' }
                  - guarded:
                      try:
                        - summarize:
                            call: remote
                            with:
                              task: summarize-remote
                      catch:
                        errors:
                          with:
                            type: https://serverlessworkflow.io/spec/1.0.0/errors/authorization
                        then: fallback
                  - remote:
                      set: '${ {done: "remote"} }'
                      export: { as: '${ $context + . }' }
                      then: end
                  - fallback:
                      set: '${ {done: "refused"} }'
                      export: { as: '${ $context + . }' }
                """;
        PolicyFixture f = new PolicyFixture(listener).worker("summarize-remote", in -> Json.obj());
        WorkflowRef ref = new WorkflowRef("meetings", "listen", "1.0.0");

        String raised = f.engine.start(ref, Json.obj(), Sensitivity.CONFIDENTIAL);
        String kept = f.engine.start(ref, Json.obj(), Sensitivity.CONFIDENTIAL);
        deliver(f, raised, "restricted");
        deliver(f, kept, "internal");

        assertThat(f.awaitCompleted(raised).path("done").asText()).isEqualTo("refused");
        assertThat(f.snapshot(raised).sensitivity()).isEqualTo(Sensitivity.RESTRICTED);
        assertThat(f.awaitCompleted(kept).path("done").asText()).isEqualTo("remote");
        assertThat(f.snapshot(kept).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);   // internal didn't lower it
    }

    private static void deliver(PolicyFixture f, String instanceId, String sensitivity) throws Exception {
        CloudEvent note = CloudEventBuilder.v1().withId("note-" + instanceId)
                .withSource(URI.create("https://meetings.example")).withType("io.example.meeting.note.v1")
                .withDataContentType("application/json").withData(Json.bytes(Json.obj().put("id", instanceId)))
                .withExtension(StintEvents.EXT_SENSITIVITY, sensitivity).build();
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (f.snapshot(instanceId).waitingKey() == null && System.nanoTime() < until) {
            Thread.onSpinWait();
        }
        f.engine.onDomainEvent(note, Duration.ofSeconds(5)).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void strict_rejects_what_the_policy_does_not_know() {
        assertThatThrownBy(() -> PolicyFixture.load(header("bad", POLICY + "\nproviders: [openai]") + """
                do:
                  - noop:
                      set: '${ . }'
                """)).isInstanceOf(DslValidationException.class)
                .hasMessageContaining("/document/metadata/x-stint-policy/providers");
        assertThatThrownBy(() -> PolicyFixture.load(header("bad", """
                routes:
                  secret: ['*']""") + """
                do:
                  - noop:
                      set: '${ . }'
                """)).hasMessageContaining("/document/metadata/x-stint-policy/routes/secret");
        assertThatThrownBy(() -> PolicyFixture.load(header("bad", """
                retention:
                  raw-audio: { days: 0 }""") + """
                do:
                  - noop:
                      set: '${ . }'
                """)).hasMessageContaining("/document/metadata/x-stint-policy/retention/raw-audio/days");
        assertThatThrownBy(() -> PolicyFixture.load("""
                document:
                  dsl: '1.0.0'
                  namespace: meetings
                  name: bad
                  version: '1.0.0'
                do:
                  - summarize:
                      call: remote
                      with:
                        task: summarize-local
                      metadata:
                        x-stint-policy:
                          maxSensitivity: internal
                          limit: 3
                """)).hasMessageContaining("/do/0/summarize/metadata/x-stint-policy/limit")
                .hasMessageContaining("no document.metadata.x-stint-policy");
    }

    @Test
    void a_registry_that_requires_a_policy_refuses_a_workflow_without_one() {
        var registry = new io.stintflow.core.WorkflowRegistry().requirePolicy();

        assertThatThrownBy(() -> registry.register(PolicyFixture.load("""
                document:
                  dsl: '1.0.0'
                  namespace: meetings
                  name: plain
                  version: '1.0.0'
                do:
                  - noop:
                      set: '${ . }'
                """))).hasMessageContaining("has no compliance policy");
        registry.register(PolicyFixture.load(GUARDED));
    }
}
