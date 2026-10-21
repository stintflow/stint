package io.stintflow.knowledge.extraction;

import java.util.ArrayList;
import java.util.List;

import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.Passage;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.spi.Sensitivity;

/**
 * Splits a transcript into passages, and passages into the windows of one call (SDD 3b.2, 4.3 and 4.7). Deterministic —
 * the same turns always give the same passages, so their ids (and with them the idempotence of a write) hold across runs.
 */
public final class PassageSplitter {

    private PassageSplitter() {
    }

    /** One turn of a conversation, already redacted. */
    public record Turn(String speaker, String text) {
    }

    /**
     * Groups consecutive turns, as {@code speaker: text} lines, into passages of at most {@code maxChars}; a turn longer than
     * that is cut at the last space before the limit.
     */
    public static List<String> split(List<Turn> turns, int maxChars) {
        List<String> passages = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (Turn turn : turns) {
            String line = turn.speaker() + ": " + turn.text().strip();
            while (line.length() > maxChars) {
                int cut = line.lastIndexOf(' ', maxChars);
                cut = cut <= 0 ? maxChars : cut;
                flush(passages, current);
                passages.add(line.substring(0, cut).strip());
                line = line.substring(cut).strip();
            }
            if (current.length() > 0 && current.length() + 1 + line.length() > maxChars) {
                flush(passages, current);
            }
            if (current.length() > 0) {
                current.append('\n');
            }
            current.append(line);
        }
        flush(passages, current);
        return passages;
    }

    /**
     * SDD 3b.3, 4.3: a plain text (the approved minutes) as passages of at most {@code maxChars}: whole paragraphs (separated by
     * a blank line) are grouped; a paragraph longer than that is cut at the last space before the limit. Deterministic, so the
     * same text always gives the same passages (and the same ids).
     */
    public static List<String> splitText(String text, int maxChars) {
        List<String> passages = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (String paragraph : text.split("\\R\\s*\\R")) {
            String line = paragraph.strip();
            if (line.isEmpty()) {
                continue;
            }
            while (line.length() > maxChars) {
                int cut = line.lastIndexOf(' ', maxChars);
                cut = cut <= 0 ? maxChars : cut;
                flush(passages, current);
                passages.add(line.substring(0, cut).strip());
                line = line.substring(cut).strip();
            }
            if (current.length() > 0 && current.length() + 2 + line.length() > maxChars) {
                flush(passages, current);
            }
            if (current.length() > 0) {
                current.append("\n\n");
            }
            current.append(line);
        }
        flush(passages, current);
        return passages;
    }

    private static void flush(List<String> passages, StringBuilder current) {
        if (current.length() > 0) {
            passages.add(current.toString());
            current.setLength(0);
        }
    }

    /** Makes the passages of a document: ids from {@link PassageIds}, ordinals from 0. */
    public static List<Passage> passages(KnowledgeScope scope, String documentId, List<String> texts, String language,
            Sensitivity level) {
        List<Passage> out = new ArrayList<>();
        for (int i = 0; i < texts.size(); i++) {
            out.add(new Passage(PassageIds.of(scope, documentId, i, texts.get(i)), i, texts.get(i), language, level));
        }
        return out;
    }

    /** Groups consecutive passages into windows of at most {@code maxWindowTokens} (a passage alone may exceed it). */
    public static List<List<Passage>> windows(List<Passage> passages, int maxWindowTokens) {
        List<List<Passage>> windows = new ArrayList<>();
        List<Passage> current = new ArrayList<>();
        int tokens = 0;
        for (Passage passage : passages) {
            int cost = ExtractionLimits.tokens(passage.text());
            if (!current.isEmpty() && tokens + cost > maxWindowTokens) {
                windows.add(current);
                current = new ArrayList<>();
                tokens = 0;
            }
            current.add(passage);
            tokens += cost;
        }
        if (!current.isEmpty()) {
            windows.add(current);
        }
        return windows;
    }
}
