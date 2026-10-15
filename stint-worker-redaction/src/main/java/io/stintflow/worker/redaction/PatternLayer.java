package io.stintflow.worker.redaction;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The deterministic reference layer of SDD 3a.5 (4.4): regular expressions, no model. Finds, in this order — so a card
 * or a document is never taken for a phone:
 * <ul>
 *   <li>{@code email};</li>
 *   <li>{@code card}: 13 to 19 digits, with spaces or dashes, that pass the Luhn check;</li>
 *   <li>{@code document}: CPF and CNPJ, formatted;</li>
 *   <li>{@code ip}: IPv4 and IPv6;</li>
 *   <li>{@code phone}: Brazilian and international, with or without country and area code;</li>
 *   <li>{@code address}: a street word (Portuguese or English) with a name and a number, either order.</li>
 * </ul>
 * <b>Doubt:</b> a run of 8 or more digits left after every pattern is something the layer can't classify (an id, an
 * account, a badly written phone) — the result says so, and the instance stays restricted.
 */
public final class PatternLayer implements RedactionLayer {

    public static final String EMAIL = "email";
    public static final String CARD = "card";
    public static final String DOCUMENT = "document";
    public static final String IP = "ip";
    public static final String PHONE = "phone";
    public static final String ADDRESS = "address";

    private record Rule(String type, Pattern pattern, boolean luhn) {
    }

    private static final String STREET_PT = "(?:Rua|R\\.|Avenida|Av\\.|Travessa|Tv\\.|Alameda|Al\\.|Rodovia|Pra[çc]a|Estrada)";
    private static final String STREET_EN = "(?:Street|St\\.|Avenue|Ave\\.|Road|Rd\\.|Boulevard|Blvd\\.|Lane|Ln\\.|Drive|Dr\\.)";

    private static final List<Rule> RULES = List.of(
            new Rule(EMAIL, Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}"), false),
            new Rule(CARD, Pattern.compile("\\b(?:\\d[ -]?){12,18}\\d\\b"), true),
            new Rule(DOCUMENT, Pattern.compile("\\b\\d{2}\\.\\d{3}\\.\\d{3}/\\d{4}-\\d{2}\\b"
                    + "|\\b\\d{3}\\.\\d{3}\\.\\d{3}-\\d{2}\\b"), false),
            new Rule(IP, Pattern.compile("\\b(?:(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)\\.){3}(?:25[0-5]|2[0-4]\\d|1?\\d?\\d)\\b"
                    + "|\\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\\b"
                    + "|\\b(?:[0-9a-fA-F]{1,4}:){1,6}:(?:[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,5})?\\b"), false),
            new Rule(PHONE, Pattern.compile("(?:\\+\\d{1,3}[\\s.-]?)?\\(?\\b\\d{2,3}\\)?[\\s.-]?\\d{4,5}[\\s.-]?\\d{4}\\b"),
                    false),
            new Rule(ADDRESS, Pattern.compile("\\b" + STREET_PT + "\\s+[\\p{L}' .-]{2,40}?,?\\s*(?:n[º°o]\\.?\\s*)?\\d{1,5}\\b"
                    + "|\\b\\d{1,5}\\s+[\\p{L}' .-]{2,30}?\\s" + STREET_EN + "(?=\\W|$)", Pattern.UNICODE_CASE), false));

    private static final Pattern UNCLASSIFIED_DIGITS = Pattern.compile("(?:\\d[\\s.-]?){7,}\\d");

    @Override
    public String name() {
        return "patterns";
    }

    @Override
    public Result apply(String text, Scope scope) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        String current = text;
        for (Rule rule : RULES) {
            if (!scope.wants(rule.type())) {
                continue;
            }
            Matcher matcher = rule.pattern().matcher(current);
            StringBuilder out = new StringBuilder();
            int found = 0;
            while (matcher.find()) {
                String value = matcher.group();
                if (rule.luhn() && !luhn(value)) {
                    continue;
                }
                found++;
                matcher.appendReplacement(out,
                        Matcher.quoteReplacement(scope.pseudonyms().label(scope.instanceId(), rule.type(), value)));
            }
            matcher.appendTail(out);
            current = out.toString();
            if (found > 0) {
                counts.merge(rule.type(), found, Integer::sum);
            }
        }
        boolean doubt = UNCLASSIFIED_DIGITS.matcher(current).find();
        return new Result(current, counts, doubt);
    }

    static boolean luhn(String value) {
        String digits = value.replaceAll("\\D", "");
        if (digits.length() < 13 || digits.length() > 19) {
            return false;
        }
        int sum = 0;
        boolean twice = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            int d = digits.charAt(i) - '0';
            if (twice) {
                d *= 2;
                if (d > 9) {
                    d -= 9;
                }
            }
            sum += d;
            twice = !twice;
        }
        return sum % 10 == 0;
    }
}
