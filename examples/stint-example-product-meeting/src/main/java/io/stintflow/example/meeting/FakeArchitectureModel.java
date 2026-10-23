package io.stintflow.example.meeting;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The CI's stand-in for the model that structures an architecture meeting into the decisions it took (SDD 3c.6): rules over the
 * redacted transcript. A sentence that opens with {@code We decided to} is a decision — its title is the rest of the sentence —
 * the sentences that follow are its summary, and {@code It answers the product decision "…"} names the product decision it
 * answers. It proves the plumbing of the scenario, not the quality of a model: the answer key reads the same script.
 */
public final class FakeArchitectureModel {

    private static final Pattern DECIDED = Pattern.compile("^We decided to (.+?)\\.$");
    private static final Pattern ANSWERS = Pattern.compile("^It answers the product decision \"([^\"]+)\"\\.$");

    /** One decision of the draft; {@code motivatedBy} are the names of the product decisions it answers. */
    public record Drafted(String id, String title, String summary, List<String> motivatedBy) {
    }

    private FakeArchitectureModel() {
    }

    public static List<Drafted> draft(String transcript) {
        List<Drafted> drafts = new ArrayList<>();
        for (String line : transcript.split("\n")) {
            String title = null;
            List<String> summary = new ArrayList<>();
            List<String> motivatedBy = new ArrayList<>();
            for (String sentence : line.strip().split("(?<=\\.\"?)\\s+(?=[A-Z])")) {
                Matcher decided = DECIDED.matcher(sentence);
                Matcher answers = ANSWERS.matcher(sentence);
                if (title == null && decided.matches()) {
                    String rest = decided.group(1);
                    title = Character.toUpperCase(rest.charAt(0)) + rest.substring(1);
                    summary.add(sentence);
                } else if (answers.matches()) {
                    motivatedBy.add(answers.group(1));
                } else if (title != null) {
                    summary.add(sentence);
                }
            }
            if (title != null) {
                drafts.add(new Drafted(slug(title), title, String.join(" ", summary), List.copyOf(motivatedBy)));
            }
        }
        return List.copyOf(drafts);
    }

    /** The id of a decision: its title in lower case, the words joined by a dash. */
    static String slug(String title) {
        return title.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");
    }
}
