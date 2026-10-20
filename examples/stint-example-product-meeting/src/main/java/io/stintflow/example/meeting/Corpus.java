package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.stintflow.wire.Json;

/**
 * The synthetic meeting corpus (SDD 3a.6, 4.3; format in {@code corpus/README.md}), read from the classpath — the
 * build copies {@code corpus/meetings} and the manifest there. Read as trees, without reflection, so it works the same
 * in a native image.
 */
public final class Corpus {

    private static final YAMLMapper YAML = new YAMLMapper();

    /** One meeting of the corpus. */
    public record Meeting(String id, String language, String label, List<Line> script, Expected expected,
            List<String> pii, String scriptSha256) {

        /** The script's text, one turn per line. */
        public String text() {
            return String.join("\n", script.stream().map(Line::text).toList());
        }

        /** The markers planted as names (not part of an e-mail): the redactor's term list. */
        public List<String> personMarkers() {
            List<String> names = new ArrayList<>();
            for (String marker : pii) {
                if (!text().contains(marker + "@")) {
                    names.add(marker);
                }
            }
            return names;
        }
    }

    public record Line(String speaker, String text) {
    }

    /** The answer key. */
    public record Expected(String docType, boolean urgent, List<String> items, boolean review) {
    }

    private Corpus() {
    }

    /** Every meeting of the manifest, in its order. */
    public static List<Meeting> load() {
        JsonNode manifest = read("corpus/manifest.json", false);
        List<Meeting> meetings = new ArrayList<>();
        for (JsonNode entry : manifest.path("meetings")) {
            JsonNode m = read("corpus/" + entry.path("script").asText(), true);
            List<Line> script = new ArrayList<>();
            m.path("script").forEach(l -> script.add(new Line(l.path("speaker").asText(), l.path("text").asText())));
            JsonNode e = m.path("expected");
            List<String> items = new ArrayList<>();
            e.path("items").forEach(i -> items.add(i.asText()));
            List<String> pii = new ArrayList<>();
            m.path("pii").forEach(p -> pii.add(p.asText()));
            meetings.add(new Meeting(m.path("id").asText(), m.path("language").asText(), m.path("label").asText(),
                    List.copyOf(script), new Expected(e.path("docType").asText(), e.path("urgent").asBoolean(),
                            List.copyOf(items), e.path("review").asBoolean()), List.copyOf(pii),
                    entry.path("scriptSha256").asText()));
        }
        return List.copyOf(meetings);
    }

    /** Every name marker of the corpus: the example's term list for the redactor. */
    public static List<String> personMarkers(List<Meeting> meetings) {
        Set<String> markers = new LinkedHashSet<>();
        meetings.forEach(m -> markers.addAll(m.personMarkers()));
        return List.copyOf(markers);
    }

    /** Every marker of the corpus, for the scanners. */
    public static List<String> allMarkers(List<Meeting> meetings) {
        Set<String> markers = new LinkedHashSet<>();
        meetings.forEach(m -> markers.addAll(m.pii()));
        return List.copyOf(markers);
    }

    static byte[] bytes(String resource) {
        try (InputStream in = Corpus.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Missing resource " + resource);
            }
            return in.readAllBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static JsonNode read(String resource, boolean yaml) {
        try {
            return yaml ? YAML.readTree(bytes(resource)) : Json.read(bytes(resource));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
