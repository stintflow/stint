package io.stintflow.knowledge.inspector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.stintflow.knowledge.ChangeEntry.Kind;
import io.stintflow.spi.Sensitivity;

/**
 * SDD 3b.6, RF6: the structure as GraphML, with the date each thing was added and, if it left, removed. The same mask as the view:
 * a restricted node is its id and type with {@code masked="true"}, and no label; a thing that is gone has no label either (the log
 * never held one). Nothing above the ceiling is written.
 */
public final class GraphMlExport {

    private GraphMlExport() {
    }

    static String write(LogState state, Map<String, String> labels, Sensitivity ceiling) {
        List<LogState.Element> nodes = new ArrayList<>();
        List<LogState.Element> edges = new ArrayList<>();
        collect(state.alive(Kind.ENTITY).values(), nodes, ceiling);
        collect(state.alive(Kind.RELATION).values(), edges, ceiling);
        state.gone().stream().filter(e -> e.kind() == Kind.ENTITY && allowed(e, ceiling)).forEach(nodes::add);
        state.gone().stream().filter(e -> e.kind() == Kind.RELATION && allowed(e, ceiling)).forEach(edges::add);
        nodes.sort(Comparator.comparing(LogState.Element::id).thenComparing(LogState.Element::addedSequence));
        edges.sort(Comparator.comparing(LogState.Element::id).thenComparing(LogState.Element::addedSequence));
        Set<String> nodeIds = new HashSet<>();
        nodes.forEach(n -> nodeIds.add(n.id()));

        StringBuilder xml = new StringBuilder(4096);
        xml.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        xml.append("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n");
        key(xml, "type", "node", "string");
        key(xml, "level", "node", "string");
        key(xml, "label", "node", "string");
        key(xml, "masked", "node", "boolean");
        key(xml, "added", "node", "string");
        key(xml, "removed", "node", "string");
        key(xml, "chain", "node", "string");
        key(xml, "type", "edge", "string");
        key(xml, "level", "edge", "string");
        key(xml, "added", "edge", "string");
        key(xml, "removed", "edge", "string");
        key(xml, "chain", "edge", "string");
        xml.append("<graph id=\"G\" edgedefault=\"directed\">\n");
        for (LogState.Element n : nodes) {
            boolean masked = n.level() == Sensitivity.RESTRICTED;
            String label = masked || n.removedAt() != null ? null : labels.get(n.id());
            xml.append("<node id=\"").append(esc(n.id())).append("\">");
            data(xml, "type", "node", n.type());
            data(xml, "level", "node", n.level().name());
            data(xml, "label", "node", label);
            data(xml, "masked", "node", Boolean.toString(masked));
            data(xml, "added", "node", n.addedAt().toString());
            data(xml, "removed", "node", n.removedAt() == null ? null : n.removedAt().toString());
            data(xml, "chain", "node", n.chainId());
            xml.append("</node>\n");
        }
        int edgeNo = 0;
        for (LogState.Element e : edges) {
            String[] parts = e.id().split("\\|", 3);
            if (parts.length != 3 || !nodeIds.contains(parts[1]) || !nodeIds.contains(parts[2])) {
                continue;
            }
            xml.append("<edge id=\"e").append(++edgeNo).append("\" source=\"").append(esc(parts[1])).append("\" target=\"")
                    .append(esc(parts[2])).append("\">");
            data(xml, "type", "edge", e.type());
            data(xml, "level", "edge", e.level().name());
            data(xml, "added", "edge", e.addedAt().toString());
            data(xml, "removed", "edge", e.removedAt() == null ? null : e.removedAt().toString());
            data(xml, "chain", "edge", e.chainId());
            xml.append("</edge>\n");
        }
        return xml.append("</graph>\n</graphml>\n").toString();
    }

    private static void collect(java.util.Collection<LogState.Element> from, List<LogState.Element> into, Sensitivity ceiling) {
        from.stream().filter(e -> allowed(e, ceiling)).forEach(into::add);
    }

    private static boolean allowed(LogState.Element e, Sensitivity ceiling) {
        return e.level() == null || e.level().compareTo(ceiling) <= 0;
    }

    private static void key(StringBuilder xml, String name, String target, String type) {
        xml.append("<key id=\"").append(target.charAt(0)).append('_').append(name).append("\" for=\"").append(target)
                .append("\" attr.name=\"").append(name).append("\" attr.type=\"").append(type).append("\"/>\n");
    }

    private static void data(StringBuilder xml, String name, String target, String value) {
        if (value != null) {
            xml.append("<data key=\"").append(target.charAt(0)).append('_').append(name).append("\">").append(esc(value))
                    .append("</data>");
        }
    }

    /** XML text, without the characters XML 1.0 does not allow. */
    private static String esc(String text) {
        StringBuilder out = new StringBuilder(text.length());
        text.codePoints().forEach(cp -> {
            if (cp == '&') {
                out.append("&amp;");
            } else if (cp == '<') {
                out.append("&lt;");
            } else if (cp == '>') {
                out.append("&gt;");
            } else if (cp == '"') {
                out.append("&quot;");
            } else if (cp == '\'') {
                out.append("&apos;");
            } else if (cp == 0x9 || cp == 0xA || cp == 0xD || cp >= 0x20 && cp <= 0xD7FF || cp >= 0xE000 && cp <= 0xFFFD
                    || cp >= 0x10000 && cp <= 0x10FFFF) {
                out.appendCodePoint(cp);
            }
        });
        return out.toString();
    }
}
