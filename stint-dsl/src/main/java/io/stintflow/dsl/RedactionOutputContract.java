package io.stintflow.dsl;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.core.model.OutputContract;
import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.redaction.RedactionContract;

/**
 * SDD 3a.5, RF5: the contract the engine checks a {@code call: redact}'s output against, and the only way an instance's
 * level goes down: a valid result <em>without doubt</em> lowers it to {@code lowersTo}. Doubt, an output out of
 * contract or an error keep it (fail closed). What is recorded and audited is counts, doubt and provenance — never the
 * redacted output.
 *
 * @param lowersTo the level after a verified redaction, or {@code null} to never lower
 */
record RedactionOutputContract(Sensitivity lowersTo) implements OutputContract {

    static final String TITLE = "Redaction output out of contract";

    @Override
    public String title() {
        return TITLE;
    }

    @Override
    public List<String> violations(JsonNode output) {
        return RedactionContract.violations(output);
    }

    @Override
    public JsonNode record(JsonNode output) {
        return RedactionContract.record(output);
    }

    @Override
    public Optional<Sensitivity> sensitivityAfter(JsonNode output) {
        return lowersTo != null && RedactionContract.certain(output) ? Optional.of(lowersTo) : Optional.empty();
    }

    @Override
    public String auditKind() {
        return AuditLog.AuditRecord.REDACTION;
    }
}
