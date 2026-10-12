package io.stintflow.wire.decision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;

/** SDD 3a.1, RF1: the decision contract and its validator. */
class DecisionContractTest {

    private static final DecisionRequest CHOICE = DecisionRequest.shape(DecisionKind.CHOICE,
            List.of(new DecisionOption("approve", "approved"), new DecisionOption("reject", null)));

    private static ObjectNode result(JsonNode value) {
        return new DecisionResult(value, 0.8, false,
                new DecisionResult.Provenance("fake-rules", null, null, "1")).toJson();
    }

    @Test
    void a_valid_answer_has_no_violations_for_each_kind() {
        assertThat(DecisionContract.violations(CHOICE, result(Json.MAPPER.getNodeFactory().textNode("reject"))))
                .isEmpty();
        assertThat(DecisionContract.violations(DecisionRequest.shape(DecisionKind.BOOLEAN, List.of()),
                result(Json.MAPPER.getNodeFactory().booleanNode(true)))).isEmpty();
        assertThat(DecisionContract.violations(DecisionRequest.shape(DecisionKind.SCORE, List.of()),
                result(Json.MAPPER.getNodeFactory().numberNode(42.5)))).isEmpty();
    }

    @Test
    void value_must_fit_the_kind() {
        assertThat(DecisionContract.violations(CHOICE, result(Json.MAPPER.getNodeFactory().textNode("maybe"))))
                .containsExactly("value is not one of the options");
        assertThat(DecisionContract.violations(DecisionRequest.shape(DecisionKind.BOOLEAN, List.of()),
                result(Json.MAPPER.getNodeFactory().textNode("true")))).containsExactly("value must be a boolean");
        assertThat(DecisionContract.violations(DecisionRequest.shape(DecisionKind.SCORE, List.of()),
                result(Json.MAPPER.getNodeFactory().textNode("high")))).containsExactly("value must be a finite number");
    }

    @Test
    void confidence_uncertain_and_provenance_are_checked() {
        ObjectNode output = result(Json.MAPPER.getNodeFactory().textNode("approve"));
        output.put("confidence", 1.5);
        output.remove("uncertain");
        ((ObjectNode) output.get("provenance")).put("adapter", "");
        ((ObjectNode) output.get("provenance")).put("model", 3);
        assertThat(DecisionContract.violations(CHOICE, output)).containsExactly(
                "confidence must be a number in [0,1]",
                "uncertain must be a boolean",
                "provenance.adapter must be a non-empty string",
                "provenance.model must be a string or null");
        assertThat(DecisionContract.violations(CHOICE, Json.MAPPER.getNodeFactory().textNode("approve")))
                .containsExactly("the output is not an object");
    }

    @Test
    void violations_never_repeat_values_from_the_output() {
        ObjectNode output = result(Json.MAPPER.getNodeFactory().textNode("SECRET-MARKER"));
        output.put("confidence", "SECRET-MARKER");
        assertThat(String.join(" ", DecisionContract.violations(CHOICE, output))).doesNotContain("SECRET-MARKER");
    }

    @Test
    void the_record_keeps_only_the_contract_fields() {
        ObjectNode output = result(Json.MAPPER.getNodeFactory().textNode("approve"));
        output.put("reasoning", "free text the adapter may add");
        ObjectNode record = DecisionContract.record(CHOICE, output);
        assertThat(record.toString()).isEqualTo("{\"kind\":\"choice\",\"value\":\"approve\",\"confidence\":0.8,"
                + "\"uncertain\":false,\"provenance\":{\"adapter\":\"fake-rules\",\"provider\":null,\"model\":null,"
                + "\"version\":\"1\"}}");
    }

    @Test
    void options_must_fit_the_kind() {
        assertThatThrownBy(() -> DecisionRequest.shape(DecisionKind.CHOICE, List.of(new DecisionOption("a", null))))
                .hasMessageContaining("at least two options");
        assertThatThrownBy(() -> DecisionRequest.shape(DecisionKind.CHOICE,
                List.of(new DecisionOption("a", null), new DecisionOption("a", null))))
                .hasMessageContaining("declared twice");
        assertThatThrownBy(() -> DecisionRequest.shape(DecisionKind.BOOLEAN, List.of(new DecisionOption("yes", null),
                new DecisionOption("no", null)))).hasMessageContaining("exactly 'true' and 'false'");
        assertThatThrownBy(() -> DecisionRequest.shape(DecisionKind.SCORE, List.of(new DecisionOption("a", null))))
                .hasMessageContaining("no options");
    }

    @Test
    void a_request_round_trips_as_json() {
        DecisionRequest request = new DecisionRequest(DecisionKind.CHOICE, CHOICE.options(),
                Json.obj().put("text", "synthetic"));
        assertThat(DecisionRequest.fromJson(request.toJson())).isEqualTo(request);
        assertThat(request.toJson().toString()).isEqualTo("{\"kind\":\"choice\",\"options\":[{\"name\":\"approve\","
                + "\"description\":\"approved\"},{\"name\":\"reject\"}],\"input\":{\"text\":\"synthetic\"}}");
    }
}
