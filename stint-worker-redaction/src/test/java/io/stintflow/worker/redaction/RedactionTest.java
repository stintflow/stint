package io.stintflow.worker.redaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.wire.redaction.RedactionContract;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;

/**
 * SDD 3a.5, RF5 (worker side): the reference layer finds each type in synthetic text, cards only when Luhn-valid;
 * what it can't classify is doubt; pseudonyms are stable within an instance and differ across instances; layers
 * compose; the handler answers the redaction contract and never returns the original text on failure.
 */
class RedactionTest {

    static final Pseudonyms PSEUDONYMS = new Pseudonyms("synthetic-secret-0123456789".getBytes(StandardCharsets.UTF_8));
    static final RedactionLayer.Scope ALL = new RedactionLayer.Scope("i-1", Set.of(), PSEUDONYMS);

    private static RedactionLayer.Result patterns(String text) {
        return new PatternLayer().apply(text, ALL);
    }

    @Test
    void the_reference_layer_finds_each_type_in_synthetic_text() {
        String text = "Mail SYNTH-PII-0001@example.com, call +55 11 98765-4321 or (21) 3456-7890, "
                + "CPF 123.456.789-09, CNPJ 12.345.678/0001-95, card 4111 1111 1111 1111, from 192.168.10.20 "
                + "and 2001:db8::ff00:42:8329, at Rua das Flores, 123 or 742 Evergreen Street.";

        RedactionLayer.Result result = patterns(text);

        assertThat(result.counts()).containsEntry("email", 1).containsEntry("phone", 2).containsEntry("document", 2)
                .containsEntry("card", 1).containsEntry("ip", 2).containsEntry("address", 2);
        assertThat(result.text()).doesNotContain("SYNTH-PII-0001", "98765", "3456-7890", "123.456.789", "0001-95",
                "4111", "192.168", "2001:db8", "Flores", "Evergreen");
        assertThat(result.text()).contains("EMAIL_", "PHONE_", "DOCUMENT_", "CARD_", "IP_", "ADDRESS_");
        assertThat(result.doubt()).isFalse();
    }

    @Test
    void a_card_number_failing_luhn_is_not_a_card_and_unclassified_digits_are_doubt() {
        RedactionLayer.Result card = patterns("Order 4111 1111 1111 1112 shipped.");
        RedactionLayer.Result account = patterns("Account 1234-5678-9012 is overdue.");

        assertThat(card.counts()).isEmpty();
        assertThat(card.doubt()).isTrue();      // digits the layer can't classify
        assertThat(account.counts()).isEmpty();
        assertThat(account.doubt()).isTrue();
        assertThat(PatternLayer.luhn("4111 1111 1111 1111")).isTrue();
        assertThat(PatternLayer.luhn("4111 1111 1111 1112")).isFalse();
    }

    @Test
    void pseudonyms_are_stable_within_an_instance_and_differ_across_instances() {
        String once = patterns("Write to synth.one@example.com").text();
        String again = patterns("SYNTH.ONE@EXAMPLE.COM wrote back").text();
        String elsewhere = new PatternLayer().apply("Write to synth.one@example.com",
                new RedactionLayer.Scope("i-2", Set.of(), PSEUDONYMS)).text();

        String label = once.substring(once.indexOf("EMAIL_"), once.indexOf("EMAIL_") + 12);
        assertThat(again).startsWith(label);                 // same person, same label, in the same instance
        assertThat(elsewhere).doesNotContain(label);         // another instance can't correlate it
        assertThatThrownBy(() -> new Pseudonyms(new byte[4])).hasMessageContaining("at least 16 bytes");
    }

    @Test
    void layers_compose_and_a_term_list_redacts_names() {
        LayeredRedactor redactor = new LayeredRedactor("sandbox", "1", List.of(new PatternLayer(),
                new TermListLayer("person", List.of("Synthia Example", "Pat Synthetic"))));

        RedactionLayer.Result result = redactor.redact("Synthia Example asked pat synthetic to mail a@example.com",
                ALL);

        assertThat(result.counts()).containsEntry("person", 2).containsEntry("email", 1);
        assertThat(result.text()).doesNotContain("Synthia", "Pat", "pat", "a@example.com");
        assertThat(redactor.layerNames()).containsExactly("patterns", "terms:person");
    }

    @Test
    void only_the_types_asked_for_are_redacted() {
        RedactionLayer.Result result = new PatternLayer().apply("a@example.com and +55 11 98765-4321",
                new RedactionLayer.Scope("i-1", Set.of("email"), PSEUDONYMS));

        assertThat(result.counts()).containsOnlyKeys("email");
        assertThat(result.text()).contains("98765-4321");
    }

    private static TaskContext task(JsonNode input) {
        return new TaskContext("/do/1/redact", "i-1", "c-1", 1, input, new WorkflowRef("test", "redact", "1.0.0"),
                "chn-1", Lineage.NONE);
    }

    @Test
    void the_handler_answers_the_redaction_contract_and_keeps_structure() {
        RedactionHandler handler = new RedactionHandler(LayeredRedactor.reference(), PSEUDONYMS);
        JsonNode request = Json.obj().<com.fasterxml.jackson.databind.node.ObjectNode>set("input", Json.obj()
                .put("transcript", "Speaker A: mail SYNTH-PII-0002@example.com").put("minutes", 42))
                .set("types", Json.MAPPER.createArrayNode());

        JsonNode result = handler.execute(task(request)).toCompletableFuture().join();

        assertThat(RedactionContract.violations(result)).isEmpty();
        assertThat(result.path("output").path("transcript").asText()).doesNotContain("SYNTH-PII-0002")
                .startsWith("Speaker A: mail EMAIL_");
        assertThat(result.path("output").path("minutes").asInt()).isEqualTo(42);
        assertThat(result.path("counts").path("email").asInt()).isEqualTo(1);
        assertThat(result.path("doubt").asBoolean()).isFalse();
        assertThat(result.path("provenance").path("redactor").asText()).isEqualTo("patterns");
        assertThat(RedactionContract.record(result).toString()).doesNotContain("Speaker", "EMAIL_");
    }

    @Test
    void a_long_number_is_doubt_and_a_failing_redactor_fails_closed() {
        RedactionHandler handler = new RedactionHandler(LayeredRedactor.reference(), PSEUDONYMS);
        JsonNode numeric = handler.execute(task(Json.obj().set("input", Json.obj().put("phone", 11987654321L))))
                .toCompletableFuture().join();
        assertThat(numeric.path("doubt").asBoolean()).isTrue();

        RedactionLayer broken = new RedactionLayer() {
            @Override
            public String name() {
                return "broken";
            }

            @Override
            public Result apply(String text, Scope scope) {
                throw new IllegalStateException("synthetic failure echoing " + text);
            }
        };
        RedactionHandler failing = new RedactionHandler(new LayeredRedactor("x", "1", List.of(broken)), PSEUDONYMS);
        assertThatThrownBy(() -> failing.execute(task(Json.obj().put("input", "SYNTH-PII-0003")))
                .toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> {
                    ErrorInfo error = ((TaskFailure) e.getCause()).toErrorInfo();
                    assertThat(error.status()).isEqualTo(500);
                    assertThat(error.detail()).isEqualTo("The redaction failed (IllegalStateException)")
                            .doesNotContain("SYNTH-PII-0003");
                });
    }
}
