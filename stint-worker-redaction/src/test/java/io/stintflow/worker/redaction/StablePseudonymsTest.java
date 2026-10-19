package io.stintflow.worker.redaction;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.wire.redaction.RedactionContract;
import io.stintflow.worker.TaskContext;

/**
 * SDD 3b.2, CA11 and 4.6 (decision P2): a pseudonym that is the same for the same person in every meeting of one knowledge
 * base — 128 bits, no instance in it — asked by the definition and allowed by the worker; off by default, and never the
 * pseudonym of SDD 3a.5 when it is not both.
 */
class StablePseudonymsTest {

    static final Pseudonyms PSEUDONYMS = new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8));
    static final RedactionLayer.Scope ALL = new RedactionLayer.Scope("i-1", Set.of(), PSEUDONYMS);

    @Test
    void the_same_person_gets_the_same_128_bit_pseudonym_in_two_instances_of_one_scope_and_another_in_another_scope() {
        String one = PSEUDONYMS.labelStable("meetings/notes", "person", "Synthia Example");
        String again = PSEUDONYMS.labelStable("meetings/notes", "person", "  synthia   example ".replace("  ", " ").strip());
        String elsewhere = PSEUDONYMS.labelStable("meetings/other", "person", "Synthia Example");

        assertThat(one).matches("PERSON_[0-9a-f]{32}");
        assertThat(PSEUDONYMS.labelStable("meetings/notes", "person", "SYNTHIA EXAMPLE")).isEqualTo(one);   // case
        assertThat(again).isEqualTo(PSEUDONYMS.labelStable("meetings/notes", "person", "synthia example"));
        assertThat(elsewhere).isNotEqualTo(one);
        assertThat(PSEUDONYMS.labelStable("meetings/notes", "person", "Pat Synthetic")).isNotEqualTo(one);
        // the label of SDD 3a.5 is another thing: 24 bits, and per instance
        assertThat(PSEUDONYMS.label("i-1", "person", "Synthia Example")).matches("PERSON_[0-9a-f]{6}");
        assertThat(PSEUDONYMS.label("i-1", "person", "Synthia Example")).isNotEqualTo(PSEUDONYMS.label("i-2", "person",
                "Synthia Example"));
        // another secret, another pseudonym: whoever does not hold the secret cannot compute who a pseudonym is
        assertThat(new Pseudonyms("another-synthetic-secret-01234".getBytes(StandardCharsets.UTF_8))
                .labelStable("meetings/notes", "person", "Synthia Example")).isNotEqualTo(one);
    }

    private static TaskContext task(String instanceId, ObjectNode request) {
        return new TaskContext("/do/0/clean", instanceId, "corr-" + instanceId, 1, request,
                new WorkflowRef("meetings", "notes", "2.0.0"), null, Lineage.NONE);
    }

    private static ObjectNode request(boolean asksScope) {
        ObjectNode request = Json.obj();
        request.put(RedactionContract.INPUT, "Synthia Example said hello, mail synth@example.com");
        request.putArray(RedactionContract.TYPES);
        if (asksScope) {
            request.putObject(RedactionContract.PSEUDONYMS).put(RedactionContract.PSEUDONYMS_MODE, RedactionContract.MODE_SCOPE)
                    .put(RedactionContract.PSEUDONYMS_SCOPE, "meetings/notes");
        }
        return request;
    }

    private static JsonNode run(RedactionHandler handler, String instance, boolean asksScope) {
        return handler.execute(task(instance, request(asksScope))).toCompletableFuture().join();
    }

    private static LayeredRedactor withPersonTerms() {
        return new LayeredRedactor("people", "1", List.of(new PatternLayer(),
                new TermListLayer("person", List.of("Synthia Example"))));
    }

    @Test
    void the_worker_gives_stable_pseudonyms_only_when_the_request_asks_and_the_worker_allows() {
        RedactionHandler allowing = new RedactionHandler(withPersonTerms(), PSEUDONYMS).allowingStablePseudonyms();
        RedactionHandler plain = new RedactionHandler(withPersonTerms(), PSEUDONYMS);

        JsonNode first = run(allowing, "i-1", true);
        JsonNode second = run(allowing, "i-2", true);
        String stable = personLabel(first);

        assertThat(stable).matches("PERSON_[0-9a-f]{32}");
        assertThat(personLabel(second)).isEqualTo(stable);   // two instances, one person
        assertThat(first.path("provenance").path("pseudonyms").asText()).isEqualTo("scope");
        assertThat(first.toString()).doesNotContain("Synthia");

        // asked but not allowed: the pseudonyms of the instance, and the result says so
        JsonNode refused = run(plain, "i-1", true);
        assertThat(personLabel(refused)).matches("PERSON_[0-9a-f]{6}");
        assertThat(refused.path("provenance").path("pseudonyms").asText()).isEqualTo("instance");
        assertThat(personLabel(run(plain, "i-2", true))).isNotEqualTo(personLabel(refused));
        // allowed but not asked: the same
        JsonNode notAsked = run(allowing, "i-1", false);
        assertThat(personLabel(notAsked)).matches("PERSON_[0-9a-f]{6}");
        assertThat(notAsked.path("provenance").path("pseudonyms").asText()).isEqualTo("instance");
        // the redaction contract still holds
        assertThat(RedactionContract.violations(first)).isEmpty();
    }

    private static String personLabel(JsonNode result) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("PERSON_[0-9a-f]+")
                .matcher(result.path(RedactionContract.OUTPUT).asText());
        assertThat(m.find()).as(result.toString()).isTrue();
        return m.group();
    }

    @Test
    void the_layers_ask_the_scope_for_their_labels() {
        RedactionLayer.Scope stable = new RedactionLayer.Scope("meetings/notes", Set.of(), PSEUDONYMS, true);

        assertThat(new PatternLayer().apply("Write to synth.one@example.com", stable).text())
                .matches(".*EMAIL_[0-9a-f]{32}.*");
        assertThat(new PatternLayer().apply("Write to synth.one@example.com", ALL).text()).matches(".*EMAIL_[0-9a-f]{6}.*");
    }
}
