package io.stintflow.spi;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.BitSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * A five-field cron expression — {@code minute hour day-of-month month day-of-week} — as used by DSL 1.0
 * {@code schedule.cron} ("Specifies the schedule using a CRON expression, e.g., '0 0 * * *' for daily at
 * midnight"), evaluated in a time zone (SDD 2.4, sec. 8b/8c).
 * <p>
 * The <strong>portable subset</strong> accepted here is what every schedule connector can express:
 * {@code *}, numbers, lists {@code ,}, ranges {@code -}, steps {@code /} (not on the day of the week),
 * month names {@code JAN-DEC} and day names {@code SUN-SAT} (day numbers 0–7, 0 and 7 = Sunday). Rejected:
 * a restricted day-of-month <em>and</em> day-of-week together (POSIX ORs them; AWS can't express it),
 * wrapping ranges ({@code FRI-MON}), macros ({@code @daily}), {@code ? L W #} and a seconds field.
 * <p>
 * Evaluation follows {@code java.time}: a local time that doesn't exist (clocks moving forward) fires at
 * the shifted instant; one that happens twice (clocks moving back) fires once, at the earlier offset.
 */
public final class CronExpression {

    private static final Map<String, Integer> MONTHS = Map.ofEntries(
            Map.entry("JAN", 1), Map.entry("FEB", 2), Map.entry("MAR", 3), Map.entry("APR", 4),
            Map.entry("MAY", 5), Map.entry("JUN", 6), Map.entry("JUL", 7), Map.entry("AUG", 8),
            Map.entry("SEP", 9), Map.entry("OCT", 10), Map.entry("NOV", 11), Map.entry("DEC", 12));
    private static final Map<String, Integer> DAYS = Map.of(
            "SUN", 0, "MON", 1, "TUE", 2, "WED", 3, "THU", 4, "FRI", 5, "SAT", 6);
    /** A search never needs more steps than this (several years of day/hour/minute jumps). */
    private static final int MAX_STEPS = 200_000;

    /**
     * One field: its text as written (upper-cased) and the values it allows.
     *
     * @param restricted whether it is anything other than {@code *}
     */
    public record Field(String text, BitSet values, boolean restricted) {
        public boolean allows(int value) {
            return values.get(value);
        }
    }

    private final Field minute;
    private final Field hour;
    private final Field dayOfMonth;
    private final Field month;
    private final Field dayOfWeek;

    private CronExpression(Field minute, Field hour, Field dayOfMonth, Field month, Field dayOfWeek) {
        this.minute = minute;
        this.hour = hour;
        this.dayOfMonth = dayOfMonth;
        this.month = month;
        this.dayOfWeek = dayOfWeek;
    }

    /** @throws IllegalArgumentException naming what is outside the portable subset */
    public static CronExpression parse(String expression) {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("cron expression is empty");
        }
        String trimmed = expression.trim();
        if (trimmed.startsWith("@")) {
            throw new IllegalArgumentException("cron macros like '" + trimmed + "' are not supported; write the five fields");
        }
        String[] parts = trimmed.split("\\s+");
        if (parts.length != 5) {
            throw new IllegalArgumentException("cron expression '" + trimmed + "' must have exactly five fields "
                    + "(minute hour day-of-month month day-of-week), got " + parts.length);
        }
        Field minute = field("minute", parts[0], 0, 59, Map.of(), true);
        Field hour = field("hour", parts[1], 0, 23, Map.of(), true);
        Field dayOfMonth = field("day-of-month", parts[2], 1, 31, Map.of(), true);
        Field month = field("month", parts[3], 1, 12, MONTHS, true);
        Field dayOfWeek = field("day-of-week", parts[4], 0, 7, DAYS, false);
        if (dayOfWeek.values().get(7)) { // 7 is Sunday too
            dayOfWeek.values().clear(7);
            dayOfWeek.values().set(0);
        }
        if (dayOfMonth.restricted() && dayOfWeek.restricted()) {
            throw new IllegalArgumentException("cron expression '" + trimmed + "' restricts both day-of-month and "
                    + "day-of-week; that combination has no portable equivalent — restrict only one");
        }
        return new CronExpression(minute, hour, dayOfMonth, month, dayOfWeek);
    }

    /**
     * DSL 1.0 {@code schedule.every} as a cron (sec. 8b): whole minutes dividing an hour, whole hours dividing a
     * day, or one day — the intervals with a reproducible scheduled instant.
     *
     * @throws IllegalArgumentException for any other interval
     */
    public static CronExpression every(Duration interval) {
        long seconds = interval.getSeconds();
        if (interval.getNano() != 0 || seconds < 60 || seconds % 60 != 0) {
            throw new IllegalArgumentException("schedule.every " + interval + " must be a whole number of minutes");
        }
        long minutes = seconds / 60;
        if (minutes < 60 && 60 % minutes == 0) {
            return parse(minutes == 1 ? "* * * * *" : "*/" + minutes + " * * * *");
        }
        if (minutes % 60 == 0) {
            long hours = minutes / 60;
            if (hours < 24 && 24 % hours == 0) {
                return parse(hours == 1 ? "0 * * * *" : "0 */" + hours + " * * *");
            }
            if (hours == 24) {
                return parse("0 0 * * *");
            }
        }
        throw new IllegalArgumentException("schedule.every " + interval + " has no reproducible cron equivalent: "
                + "use minutes dividing an hour, hours dividing a day, or one day");
    }

    public Field minute() {
        return minute;
    }

    public Field hour() {
        return hour;
    }

    public Field dayOfMonth() {
        return dayOfMonth;
    }

    public Field month() {
        return month;
    }

    /** Values 0–6, 0 = Sunday. */
    public Field dayOfWeek() {
        return dayOfWeek;
    }

    /** The five fields as written (upper-cased, single spaces). */
    public String expression() {
        return String.join(" ", minute.text(), hour.text(), dayOfMonth.text(), month.text(), dayOfWeek.text());
    }

    /** The first occurrence strictly after {@code after}, in {@code zone}. */
    public Instant nextAfter(Instant after, ZoneId zone) {
        LocalDateTime t = LocalDateTime.ofInstant(after, zone).truncatedTo(ChronoUnit.MINUTES).plusMinutes(1);
        for (int step = 0; step < MAX_STEPS; step++) {
            if (!month.allows(t.getMonthValue())) {
                t = t.withDayOfMonth(1).toLocalDate().atStartOfDay().plusMonths(1);
            } else if (!allowsDay(t)) {
                t = t.toLocalDate().plusDays(1).atStartOfDay();
            } else if (!hour.allows(t.getHour())) {
                t = t.truncatedTo(ChronoUnit.HOURS).plusHours(1);
            } else if (!minute.allows(t.getMinute())) {
                t = t.plusMinutes(1);
            } else {
                Instant candidate = ZonedDateTime.ofLocal(t, zone, null).toInstant();
                if (candidate.isAfter(after)) {
                    return candidate;
                }
                t = t.plusMinutes(1);
            }
        }
        throw new IllegalStateException("No occurrence of '" + expression() + "' found after " + after);
    }

    /** The last occurrence at or before {@code at}, in {@code zone}. */
    public Instant lastAtOrBefore(Instant at, ZoneId zone) {
        LocalDateTime t = LocalDateTime.ofInstant(at, zone).truncatedTo(ChronoUnit.MINUTES);
        for (int step = 0; step < MAX_STEPS; step++) {
            if (!month.allows(t.getMonthValue())) {
                t = t.withDayOfMonth(1).toLocalDate().atStartOfDay().minusMinutes(1);
            } else if (!allowsDay(t)) {
                t = t.toLocalDate().atStartOfDay().minusMinutes(1);
            } else if (!hour.allows(t.getHour())) {
                t = t.truncatedTo(ChronoUnit.HOURS).minusMinutes(1);
            } else if (!minute.allows(t.getMinute())) {
                t = t.minusMinutes(1);
            } else {
                Instant candidate = ZonedDateTime.ofLocal(t, zone, null).toInstant();
                if (!candidate.isAfter(at)) {
                    return candidate;
                }
                t = t.minusMinutes(1);
            }
        }
        throw new IllegalStateException("No occurrence of '" + expression() + "' found before " + at);
    }

    private boolean allowsDay(LocalDateTime t) {
        boolean dom = dayOfMonth.allows(t.getDayOfMonth());
        boolean dow = dayOfWeek.allows(t.getDayOfWeek().getValue() % 7);
        if (dayOfMonth.restricted()) {
            return dom;
        }
        return !dayOfWeek.restricted() || dow;
    }

    private static Field field(String name, String text, int min, int max, Map<String, Integer> names,
            boolean stepsAllowed) {
        // '?', 'L', 'W' and '#' (Quartz/AWS extensions) are not numbers or names: number() rejects them.
        String upper = text.toUpperCase(Locale.ROOT);
        BitSet values = new BitSet(max + 1);
        for (String part : upper.split(",", -1)) {
            if (part.isEmpty()) {
                throw new IllegalArgumentException(name + " '" + text + "' has an empty list element");
            }
            int step = 1;
            String range = part;
            int slash = part.indexOf('/');
            if (slash >= 0) {
                if (!stepsAllowed) {
                    throw new IllegalArgumentException(name + " '" + text + "': steps are not supported on the day of "
                            + "the week (no portable equivalent)");
                }
                step = number(name, part.substring(slash + 1), 1, max, Map.of());
                range = part.substring(0, slash);
            }
            int from;
            int to;
            if (range.equals("*")) {
                from = min;
                to = max == 7 ? 6 : max;
            } else if (range.contains("-")) {
                String[] bounds = range.split("-", -1);
                if (bounds.length != 2) {
                    throw new IllegalArgumentException(name + " '" + text + "' has an invalid range");
                }
                from = number(name, bounds[0], min, max, names);
                to = number(name, bounds[1], min, max, names);
                if (from > to) {
                    throw new IllegalArgumentException(name + " '" + text + "': wrapping ranges are not supported");
                }
            } else {
                from = number(name, range, min, max, names);
                to = slash >= 0 ? (max == 7 ? 6 : max) : from;
            }
            for (int v = from; v <= to; v += step) {
                values.set(v);
            }
        }
        return new Field(upper, values, !upper.equals("*"));
    }

    private static int number(String name, String token, int min, int max, Map<String, Integer> names) {
        Integer named = names.get(token);
        if (named != null) {
            return named;
        }
        try {
            int value = Integer.parseInt(token);
            if (value < min || value > max) {
                throw new IllegalArgumentException(name + " value " + value + " is outside " + min + "-" + max);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " '" + token + "' is not a number or a known name"
                    + " (?, L, W and # are not supported)");
        }
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CronExpression other && other.expression().equals(expression());
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression());
    }

    @Override
    public String toString() {
        return expression();
    }
}
