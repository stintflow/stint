package io.stintflow.example.meeting;

import java.util.List;

import dev.langchain4j.service.UserMessage;
import io.quarkus.runtime.annotations.RegisterForProxy;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * The milestone's AI Service (SDD 3a.6, 4.4): structures a redacted transcript into minutes. Registered for the JDK
 * proxy and reflection, for the native image (SDD 3a.3, ADR 3a.3-A).
 */
@RegisterForProxy
@RegisterForReflection(methods = true)
public interface MinutesWriter {

    String PROMPT = "Structure the meeting below into minutes. Answer with JSON: summary, requirements, decisions, "
            + "constraints, openQuestions, and items (each {id, text}, the action items in order, ids item-1, "
            + "item-2...). Keep the pseudonyms as they are.\nDocument type: %s\nTranscript:\n%s";

    Minutes structure(@UserMessage String prompt);

    @RegisterForReflection
    record Minutes(String summary, List<String> requirements, List<String> decisions, List<String> constraints,
            List<String> openQuestions, List<Item> items) {

        @RegisterForReflection
        public record Item(String id, String text) {
        }

        /** The minutes as the little markdown the publisher turns into a page. */
        public String markdown(String title) {
            StringBuilder md = new StringBuilder("# ").append(title).append("\n## Summary\n").append(summary)
                    .append('\n');
            section(md, "Requirements", requirements);
            section(md, "Decisions", decisions);
            section(md, "Constraints", constraints);
            section(md, "Open questions", openQuestions);
            section(md, "Action items", items == null ? List.of()
                    : items.stream().map(i -> i.id() + ": " + i.text()).toList());
            return md.toString();
        }

        private static void section(StringBuilder md, String name, List<String> lines) {
            if (lines == null || lines.isEmpty()) {
                return;
            }
            md.append("## ").append(name).append('\n');
            lines.forEach(line -> md.append("- ").append(line).append('\n'));
        }
    }
}
