package io.stintflow.wire.review;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * The review contract of SDD 3a.4 (4.2): what a {@code io.stintflow.review.completed.v1} carries in its {@code data},
 * and how it is checked before it may resume an instance. No free text: the reviewer's note goes by pointer
 * ({@code noteRef}), and the reviewer is an opaque id the channel gives — never a name or an e-mail. Shared by the
 * engine, which checks answers, and by channels, which build them.
 * <ul>
 *   <li>{@code choice}: {@code value}, one of the options;</li>
 *   <li>{@code approval}: {@code verdict} ({@code approved} or {@code rejected}); for a review of items, {@code items},
 *       one {@code {id, verdict, noteRef?}} per item asked, no more, no less;</li>
 *   <li>always {@code reviewer}; optionally {@code noteRef}.</li>
 * </ul>
 */
public final class ReviewContract {

    public static final String CHOICE = "choice";
    public static final String APPROVAL = "approval";
    public static final String APPROVED = "approved";
    public static final String REJECTED = "rejected";

    public static final String VALUE = "value";
    public static final String VERDICT = "verdict";
    public static final String ITEMS = "items";
    public static final String ID = "id";
    public static final String NOTE_REF = "noteRef";
    public static final String REVIEWER = "reviewer";

    private static final Set<String> ANSWER_KEYS = Set.of(VALUE, VERDICT, ITEMS, NOTE_REF, REVIEWER);
    private static final Set<String> ITEM_KEYS = Set.of(ID, VERDICT, NOTE_REF);
    private static final int MAX_REVIEWER = 128;

    private ReviewContract() {
    }

    /**
     * What is wrong with an answer to a review of {@code kind}, with these {@code options} (a choice) and these
     * {@code items} (an approval of items; empty for a whole document). Field names only, never values.
     */
    public static List<String> violations(JsonNode data, String kind, List<String> options, List<String> items) {
        List<String> problems = new ArrayList<>();
        if (data == null || !data.isObject()) {
            problems.add("data must be an object");
            return problems;
        }
        for (Iterator<String> names = data.fieldNames(); names.hasNext(); ) {
            String name = names.next();
            if (!ANSWER_KEYS.contains(name)) {
                problems.add("unknown field '" + name + "' (no free text: a note goes by noteRef)");
            }
        }
        if (!isOpaqueId(data.get(REVIEWER))) {
            problems.add("reviewer must be an opaque id (no '@', no spaces, at most " + MAX_REVIEWER + " characters)");
        }
        if (data.has(NOTE_REF) && !isPointer(data.get(NOTE_REF))) {
            problems.add("noteRef must be a pointer (a URI with a scheme)");
        }
        if (CHOICE.equals(kind)) {
            JsonNode value = data.get(VALUE);
            if (value == null || !value.isValueNode() || !options.contains(value.asText())) {
                problems.add("value must be one of the options");
            }
            if (data.has(VERDICT) || data.has(ITEMS)) {
                problems.add("a choice has no verdict nor items");
            }
        } else if (APPROVAL.equals(kind)) {
            if (data.has(VALUE)) {
                problems.add("an approval has no value");
            }
            if (data.has(VERDICT) && !isVerdict(data.get(VERDICT))) {
                problems.add("verdict must be approved or rejected");
            }
            if (items.isEmpty()) {
                if (!data.has(VERDICT)) {
                    problems.add("verdict is required");
                }
                if (data.has(ITEMS)) {
                    problems.add("no items were asked");
                }
            } else {
                problems.addAll(itemViolations(data.get(ITEMS), items));
            }
        } else {
            problems.add("unknown review kind");
        }
        return problems;
    }

    private static List<String> itemViolations(JsonNode answered, List<String> asked) {
        List<String> problems = new ArrayList<>();
        if (answered == null || !answered.isArray()) {
            problems.add("items is required: one verdict per item asked");
            return problems;
        }
        Set<String> seen = new HashSet<>();
        for (JsonNode item : answered) {
            if (!item.isObject()) {
                problems.add("each item must be an object");
                continue;
            }
            for (Iterator<String> names = item.fieldNames(); names.hasNext(); ) {
                String name = names.next();
                if (!ITEM_KEYS.contains(name)) {
                    problems.add("unknown item field '" + name + "'");
                }
            }
            String id = item.path(ID).asText(null);
            if (id == null || !asked.contains(id) || !seen.add(id)) {
                problems.add("each item id must be one asked, once");
            }
            if (!isVerdict(item.get(VERDICT))) {
                problems.add("each item verdict must be approved or rejected");
            }
            if (item.has(NOTE_REF) && !isPointer(item.get(NOTE_REF))) {
                problems.add("an item noteRef must be a pointer (a URI with a scheme)");
            }
        }
        if (seen.size() != asked.size()) {
            problems.add("every item asked needs a verdict");
        }
        return problems;
    }

    /** The overall verdict of a valid approval: rejected if it, or any of its items, is. */
    public static String verdictOf(JsonNode data) {
        if (REJECTED.equals(data.path(VERDICT).asText())) {
            return REJECTED;
        }
        for (JsonNode item : data.path(ITEMS)) {
            if (REJECTED.equals(item.path(VERDICT).asText())) {
                return REJECTED;
            }
        }
        return APPROVED;
    }

    /** The ids of the rejected items of a valid approval, in the order answered. */
    public static List<String> rejectedItems(JsonNode data) {
        List<String> rejected = new ArrayList<>();
        for (JsonNode item : data.path(ITEMS)) {
            if (REJECTED.equals(item.path(VERDICT).asText())) {
                rejected.add(item.path(ID).asText());
            }
        }
        return rejected;
    }

    private static boolean isVerdict(JsonNode verdict) {
        return verdict != null && (APPROVED.equals(verdict.asText()) || REJECTED.equals(verdict.asText()));
    }

    /** An id a channel gives for a person: never a name nor an e-mail. */
    public static boolean isOpaqueId(JsonNode id) {
        if (id == null || !id.isTextual()) {
            return false;
        }
        String text = id.textValue();
        return !text.isBlank() && text.length() <= MAX_REVIEWER && !text.contains("@") && text.chars()
                .noneMatch(Character::isWhitespace);
    }

    /** A pointer: a text that parses as a URI with a scheme ({@code s3://}, {@code file:}, {@code https://}…). */
    public static boolean isPointer(JsonNode node) {
        if (node == null || !node.isTextual()) {
            return false;
        }
        String text = node.textValue();
        if (text.isBlank() || text.chars().anyMatch(Character::isWhitespace)) {
            return false;
        }
        try {
            URI uri = new URI(text);
            return uri.getScheme() != null && uri.getScheme().length() > 1;
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
