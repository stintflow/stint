package io.stintflow.worker.redaction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A redaction as a pipeline of layers (SDD 3a.5, 4.4): each layer sees the text the previous ones left, so a value one
 * layer replaced is a pseudonym for the next. Counts add up; any layer's doubt is the redaction's doubt. The Stint
 * gives the reference {@link PatternLayer} and a {@link TermListLayer}; a pipeline adds its own (a local detector, a
 * yes/no question to a local decision model) — never relying on a single one.
 */
public final class LayeredRedactor {

    private final String name;
    private final String version;
    private final List<RedactionLayer> layers;

    public LayeredRedactor(String name, String version, List<RedactionLayer> layers) {
        this.name = Objects.requireNonNull(name, "name");
        this.version = Objects.requireNonNull(version, "version");
        if (layers.isEmpty()) {
            throw new IllegalArgumentException("A redactor needs at least one layer");
        }
        this.layers = List.copyOf(layers);
    }

    /** The reference redactor: the {@link PatternLayer} alone. */
    public static LayeredRedactor reference() {
        return new LayeredRedactor("patterns", "1", List.of(new PatternLayer()));
    }

    public String name() {
        return name;
    }

    public String version() {
        return version;
    }

    public List<String> layerNames() {
        return layers.stream().map(RedactionLayer::name).toList();
    }

    /** Runs every layer over {@code text}. */
    public RedactionLayer.Result redact(String text, RedactionLayer.Scope scope) {
        String current = text;
        Map<String, Integer> counts = new LinkedHashMap<>();
        boolean doubt = false;
        for (RedactionLayer layer : layers) {
            RedactionLayer.Result result = layer.apply(current, scope);
            current = result.text();
            result.counts().forEach((type, n) -> counts.merge(type, n, Integer::sum));
            doubt |= result.doubt();
        }
        return new RedactionLayer.Result(current, counts, doubt);
    }
}
