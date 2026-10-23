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

    // --- the entity-resolution evaluation set (SDD 3b.2, 4.8) -----------------------------------------------------

    /**
     * One thing a model would cite in a meeting, and what it should resolve to.
     *
     * @param surface    the words as the script has them (found in its text, ignoring case)
     * @param expect     the id it resolves to, or {@code new} when nothing in the base is it
     * @param how        for a resolved one: {@code glossary}, {@code alias} or {@code existing}
     * @param candidates for a {@code new} one: the ids a duplicate check must flag beside it
     */
    public record Mention(String type, String surface, String expect, String how, List<String> candidates) {
    }

    /** A meeting with the gold of its mentions. */
    public record KnowledgeMeeting(String id, String text, List<String> pii, List<Mention> mentions,
            String scriptSha256, boolean inPipeline) {
    }

    public record Term(String id, String type, String name, List<String> aliases) {
    }

    /** What the base holds before any meeting of the set: glossary, entities from earlier meetings, and pairs never to merge. */
    public record KnowledgeBase(List<Term> glossary, List<Term> existing, List<List<String>> mustNotMerge,
            String sha256) {
    }

    public record Knowledge(KnowledgeBase base, List<KnowledgeMeeting> meetings) {
        public long mentionCount() {
            return meetings.stream().mapToLong(m -> m.mentions().size()).sum();
        }
    }

    /**
     * The evaluation set: the base ({@code corpus/kb/base.yaml}) and every meeting that carries a {@code kb} section — the
     * eight of the milestone, and the ones the manifest lists under {@code knowledge}, which the milestone does not run.
     */
    public static Knowledge loadKnowledge() {
        JsonNode manifest = read("corpus/manifest.json", false);
        JsonNode baseEntry = manifest.path("kbBase");
        JsonNode base = read("corpus/" + baseEntry.path("file").asText(), true);
        List<Term> glossary = new ArrayList<>();
        base.path("glossary").forEach(t -> glossary.add(term(t)));
        List<Term> existing = new ArrayList<>();
        base.path("existing").forEach(t -> existing.add(term(t)));
        List<List<String>> pairs = new ArrayList<>();
        base.path("mustNotMerge").forEach(p -> pairs.add(List.of(p.get(0).asText(), p.get(1).asText())));

        List<KnowledgeMeeting> meetings = new ArrayList<>();
        for (String list : List.of("meetings", "knowledge")) {
            for (JsonNode entry : manifest.path(list)) {
                JsonNode m = read("corpus/" + entry.path("script").asText(), true);
                if (!m.has("kb")) {
                    continue;
                }
                StringBuilder text = new StringBuilder();
                m.path("script").forEach(l -> text.append(l.path("text").asText()).append('\n'));
                List<Mention> mentions = new ArrayList<>();
                m.path("kb").path("mentions").forEach(x -> {
                    List<String> candidates = new ArrayList<>();
                    x.path("candidates").forEach(c -> candidates.add(c.asText()));
                    mentions.add(new Mention(x.path("type").asText(), x.path("surface").asText(), x.path("expect").asText(),
                            x.path("how").asText(null), List.copyOf(candidates)));
                });
                List<String> pii = new ArrayList<>();
                m.path("pii").forEach(p -> pii.add(p.asText()));
                meetings.add(new KnowledgeMeeting(m.path("id").asText(), text.toString(), List.copyOf(pii),
                        List.copyOf(mentions), entry.path("scriptSha256").asText(), list.equals("meetings")));
            }
        }
        return new Knowledge(new KnowledgeBase(List.copyOf(glossary), List.copyOf(existing), List.copyOf(pairs),
                baseEntry.path("sha256").asText()), List.copyOf(meetings));
    }

    private static Term term(JsonNode t) {
        List<String> aliases = new ArrayList<>();
        t.path("aliases").forEach(a -> aliases.add(a.asText()));
        return new Term(t.path("id").asText(), t.path("type").asText(), t.path("name").asText(), List.copyOf(aliases));
    }

    // --- the scenario of the milestone (SDD 3c.6, format v1.2) ----------------------------------------------------

    /**
     * A decision a product meeting takes, as its gold says a model would extract it.
     *
     * @param surface the words as the script has them (found in its text, ignoring case)
     * @param label   the decision's name in the product graph
     * @param summary what the decision says; sensitive, only the definition that checks conflicts sees it
     */
    public record ProductDecision(String meetingId, String surface, String label, String summary, String status) {
    }

    /** What an architecture meeting should draft: one decision, the product decisions it answers, and its verdict. */
    public record Draft(String id, String title, List<String> motivatedBy, String verdict, boolean reviewed, boolean originDown) {
    }

    /** A meeting of the architecture area (text only: the redacted transcript) and the gold of its drafts. */
    public record ArchitectureMeeting(String id, String label, String text, List<Draft> drafts, String scriptSha256) {
    }

    /** The product decisions of the milestone's meetings (the {@code kb.decisions} of each script), in manifest order. */
    public static List<ProductDecision> loadProductDecisions() {
        JsonNode manifest = read("corpus/manifest.json", false);
        List<ProductDecision> decisions = new ArrayList<>();
        for (JsonNode entry : manifest.path("meetings")) {
            JsonNode m = read("corpus/" + entry.path("script").asText(), true);
            m.path("kb").path("decisions").forEach(d -> decisions.add(new ProductDecision(m.path("id").asText(),
                    d.path("surface").asText(), d.path("label").asText(), d.path("summary").asText(),
                    d.path("status").asText("accepted"))));
        }
        return List.copyOf(decisions);
    }

    /** The architecture meetings of the manifest, in its order. */
    public static List<ArchitectureMeeting> loadArchitecture() {
        JsonNode manifest = read("corpus/manifest.json", false);
        List<ArchitectureMeeting> meetings = new ArrayList<>();
        for (JsonNode entry : manifest.path("architecture")) {
            JsonNode m = read("corpus/" + entry.path("script").asText(), true);
            StringBuilder text = new StringBuilder();
            m.path("script").forEach(l -> text.append(l.path("text").asText()).append('\n'));
            List<Draft> drafts = new ArrayList<>();
            m.path("expected").path("drafts").forEach(d -> {
                List<String> motivatedBy = new ArrayList<>();
                d.path("motivatedBy").forEach(x -> motivatedBy.add(x.asText()));
                drafts.add(new Draft(d.path("id").asText(), d.path("title").asText(), List.copyOf(motivatedBy),
                        d.path("verdict").asText(), "reviewed".equals(d.path("how").asText()),
                        d.path("originDown").asBoolean(false)));
            });
            meetings.add(new ArchitectureMeeting(m.path("id").asText(), m.path("label").asText(), text.toString(),
                    List.copyOf(drafts), entry.path("scriptSha256").asText()));
        }
        return List.copyOf(meetings);
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
