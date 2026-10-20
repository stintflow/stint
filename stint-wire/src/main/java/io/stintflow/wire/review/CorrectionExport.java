package io.stintflow.wire.review;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.CorrectionLog.Correction;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;

/**
 * SDD 3a.4, RF7: the evaluation set, one JSON object per line (JSONL). Each line has the decision as the adapter made
 * it (value, confidence, uncertain, provenance), the human answer, whether they agree, the role, the reviewer's opaque
 * id and the dates; the material and any note only as pointers. Only corrections at or below {@code maxLevel} are
 * written; a correction without a level counts as {@code restricted} (fail closed).
 */
public final class CorrectionExport {

    private CorrectionExport() {
    }

    /** Writes the corrections at or below {@code maxLevel}; how many were written. */
    public static int write(List<Correction> corrections, Sensitivity maxLevel, Writer out) {
        int written = 0;
        try {
            for (Correction correction : corrections) {
                Sensitivity level = correction.sensitivity() == null ? Sensitivity.RESTRICTED
                        : correction.sensitivity();
                if (level.compareTo(maxLevel) > 0) {
                    continue;
                }
                out.write(line(correction).toString());
                out.write('\n');
                written++;
            }
            out.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return written;
    }

    static ObjectNode line(Correction c) {
        ObjectNode line = Json.obj();
        line.put("definition", c.definition().canonical());
        line.put("instanceId", c.instanceId());
        line.put("pointer", c.pointer());
        line.put("reviewId", c.reviewId());
        line.put("kind", c.kind());
        line.put("sensitivity", c.sensitivity() == null ? null : c.sensitivity().wireName());
        line.put("materialRef", c.materialRef());
        line.set("decision", c.decision());
        line.set("human", c.human());
        if (c.decision() != null && c.human() != null && c.human().has(ReviewContract.VALUE)) {
            JsonNode decided = c.decision().get(ReviewContract.VALUE);
            line.put("agrees", decided != null && decided.asText().equals(c.human().get(ReviewContract.VALUE).asText()));
        }
        line.put("role", c.role());
        line.put("reviewer", c.reviewer());
        line.put("requestedAt", c.requestedAt() == null ? null : c.requestedAt().toString());
        line.put("reviewedAt", c.reviewedAt() == null ? null : c.reviewedAt().toString());
        line.put("escalated", c.escalated());
        return line;
    }
}
