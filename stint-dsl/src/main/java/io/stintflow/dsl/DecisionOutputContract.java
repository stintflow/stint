package io.stintflow.dsl;

import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.model.OutputContract;
import io.stintflow.wire.decision.DecisionContract;
import io.stintflow.wire.decision.DecisionRequest;

/**
 * SDD 3a.1, RF4/RF5: the contract the engine checks a {@code call: decision}'s output against — the decision
 * contract of {@code stint-wire}, for the kind and options the definition declares — and the record it keeps.
 *
 * @param shape the declared kind and options (without input)
 */
record DecisionOutputContract(DecisionRequest shape) implements OutputContract {

    static final String TITLE = "Decision output out of contract";

    @Override
    public String title() {
        return TITLE;
    }

    @Override
    public List<String> violations(JsonNode output) {
        return DecisionContract.violations(shape, output);
    }

    @Override
    public JsonNode record(JsonNode output) {
        return DecisionContract.record(shape, output);
    }
}
