package io.stintflow.worker.knowledge;

import java.security.SecureRandom;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import io.stintflow.knowledge.Entity;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.Passage;
import io.stintflow.worker.redaction.PatternLayer;
import io.stintflow.worker.redaction.Pseudonyms;
import io.stintflow.worker.redaction.RedactionLayer;

/**
 * SDD 3b.3, RF3 and 4.4: the last line before the base. Looks, with the reference patterns of the redactor (e-mail, card,
 * document, IP, phone, address), for personal data still in a batch — the text of its passages, and the labels and properties
 * of its entities — and counts what it finds by type. A person is a pseudonym by then; anything the patterns still match was
 * not redacted, and the batch is refused whole: it fails closed.
 * <p>
 * It reports numbers by type only — never a value, nor where it was.
 */
public final class PiiGate {

    /** The patterns only count here; the labels they would have made are discarded, so any secret will do. */
    private static final Pseudonyms DISCARDED = new Pseudonyms(randomSecret());

    private static final PatternLayer PATTERNS = new PatternLayer();

    private PiiGate() {
    }

    /** What the patterns find in the batch, by type; empty means clean. */
    public static Map<String, Integer> findings(KnowledgeBatch batch) {
        Map<String, Integer> found = new LinkedHashMap<>();
        for (Passage p : batch.passages()) {
            scan(p.text(), found);
        }
        for (Entity e : batch.entities()) {
            scan(e.label(), found);
            e.properties().values().forEach(v -> scan(v, found));
        }
        return found;
    }

    private static void scan(String text, Map<String, Integer> found) {
        RedactionLayer.Result result = PATTERNS.apply(text, new RedactionLayer.Scope("gate", Set.of(), DISCARDED));
        result.counts().forEach((type, n) -> found.merge(type, n, Integer::sum));
    }

    private static byte[] randomSecret() {
        byte[] secret = new byte[32];
        new SecureRandom().nextBytes(secret);
        return secret;
    }
}
