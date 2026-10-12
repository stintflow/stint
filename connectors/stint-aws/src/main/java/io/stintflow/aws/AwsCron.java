package io.stintflow.aws;

import java.util.ArrayList;
import java.util.List;

import io.stintflow.spi.CronExpression;

/**
 * Translates a five-field {@link CronExpression} into an EventBridge Scheduler expression,
 * {@code cron(minutes hours day-of-month month day-of-week year)} (SDD 2.4, sec. 8b):
 * <ul>
 *   <li>year is {@code *};</li>
 *   <li>the unrestricted day field becomes {@code ?}: AWS requires one of the two to be {@code ?}, and
 *       {@link CronExpression} never restricts both;</li>
 *   <li>a numeric day of the week moves from POSIX 0–6 (0 = Sunday) to AWS 1–7 (1 = Sunday); day names
 *       ({@code MON}) are kept as written.</li>
 * </ul>
 */
final class AwsCron {

    private AwsCron() {
    }

    static String of(CronExpression cron) {
        String dayOfMonth = cron.dayOfMonth().restricted() ? cron.dayOfMonth().text() : "*";
        String dayOfWeek = "?";
        if (cron.dayOfWeek().restricted()) {
            dayOfMonth = "?";
            dayOfWeek = dayOfWeek(cron);
        }
        return "cron(" + String.join(" ", cron.minute().text(), cron.hour().text(), dayOfMonth,
                cron.month().text(), dayOfWeek, "*") + ")";
    }

    /** Names as written; numeric tokens renumbered from the parsed values. */
    private static String dayOfWeek(CronExpression cron) {
        String text = cron.dayOfWeek().text();
        if (text.chars().anyMatch(Character::isLetter)) {
            boolean anyDigit = text.chars().anyMatch(Character::isDigit);
            if (!anyDigit) {
                return text;
            }
        }
        // Renumber from the values (0 = Sunday) into AWS numbers (1 = Sunday), compressed into ranges.
        List<String> parts = new ArrayList<>();
        int start = -1;
        int previous = -2;
        for (int v = cron.dayOfWeek().values().nextSetBit(0); v >= 0; v = cron.dayOfWeek().values().nextSetBit(v + 1)) {
            int aws = v + 1;
            if (aws != previous + 1) {
                if (start >= 0) {
                    parts.add(range(start, previous));
                }
                start = aws;
            }
            previous = aws;
        }
        parts.add(range(start, previous));
        return String.join(",", parts);
    }

    private static String range(int from, int to) {
        return from == to ? Integer.toString(from) : from + "-" + to;
    }
}
