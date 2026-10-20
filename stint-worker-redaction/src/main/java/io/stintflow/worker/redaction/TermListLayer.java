package io.stintflow.worker.redaction;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A layer that redacts known terms of one type — e.g. the names of a synthetic roster, as {@code person} (SDD 3a.5,
 * 4.4). Whole words, case-insensitive; the longest term wins where two overlap. Never in doubt: what it doesn't list,
 * it doesn't see — another layer must.
 */
public final class TermListLayer implements RedactionLayer {

    private final String type;
    private final Pattern pattern;

    public TermListLayer(String type, List<String> terms) {
        this.type = Objects.requireNonNull(type, "type");
        if (terms.isEmpty()) {
            throw new IllegalArgumentException("A term list layer needs at least one term");
        }
        String alternatives = terms.stream().filter(t -> !t.isBlank()).sorted(Comparator.comparingInt(String::length)
                .reversed()).map(Pattern::quote).reduce((a, b) -> a + "|" + b).orElseThrow();
        this.pattern = Pattern.compile("(?<![\\p{L}\\d])(?:" + alternatives + ")(?![\\p{L}\\d])",
                Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
    }

    @Override
    public String name() {
        return "terms:" + type;
    }

    @Override
    public Result apply(String text, Scope scope) {
        if (!scope.wants(type)) {
            return new Result(text, Map.of(), false);
        }
        Matcher matcher = pattern.matcher(text);
        StringBuilder out = new StringBuilder();
        int found = 0;
        while (matcher.find()) {
            found++;
            matcher.appendReplacement(out,
                    Matcher.quoteReplacement(scope.pseudonyms().label(scope.instanceId(), type, matcher.group())));
        }
        matcher.appendTail(out);
        return new Result(out.toString(), found == 0 ? Map.of() : Map.of(type, found), false);
    }
}
