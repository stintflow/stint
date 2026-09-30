package io.stintflow.core.model;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import io.stintflow.core.expr.Expr;

/**
 * {@code listen} (SDD 2.3, RF1): suspends the instance until correlated domain events arrive. DSL 1.0
 * {@code dsl-reference.md}, Listen: "Provides a mechanism for workflows to await and react to external
 * events"; {@code listen.to} is an Event Consumption Strategy ({@code one}, {@code any} or {@code all}
 * of a list of Event Filters) and {@code listen.read} one of {@code data} (default), {@code envelope} or
 * {@code raw}. The task's raw output is "a sequentially ordered array of all the events it has consumed"
 * — here, in the order they were consumed (sec. 8b).
 * <p>
 * Subset implemented (sec. 5/8f): every filter has a {@code type} and at least one {@code correlate}
 * entry, each with an {@code expect}; {@code with} is exact match; no {@code until}, no {@code foreach},
 * no empty {@code any}. The registry rejects anything else.
 *
 * @param timeout {@code timeout.after}, or {@code null} to wait indefinitely; validated against the
 *                timer's max delay when the definition is registered (sec. 8d)
 */
public record ListenNode(
        String name,
        String pointer,
        DataFlow dataFlow,
        FlowDirective then,
        Strategy strategy,
        List<Filter> filters,
        Read read,
        Duration timeout) implements TaskNode {

    public ListenNode {
        filters = List.copyOf(filters);
        read = read == null ? Read.DATA : read;
    }

    /** DSL 1.0 Event Consumption Strategy; {@code until} is not supported (sec. 8f). */
    public enum Strategy { ONE, ANY, ALL }

    /** DSL 1.0 Listen, {@code read}: what of each consumed event becomes part of the output (sec. 8e). */
    public enum Read { DATA, ENVELOPE, RAW }

    /**
     * DSL 1.0 Event Filter.
     *
     * @param type      required event {@code type}
     * @param with      other context attributes/extensions the event must carry with exactly these values
     * @param correlate name → correlation (DSL 1.0: "A name/definition mapping of the correlations to
     *                  attempt when filtering events"); all must match (sec. 8a)
     */
    public record Filter(String type, Map<String, String> with, SortedMap<String, Correlation> correlate) {
        public Filter {
            with = with == null ? Map.of() : Map.copyOf(with);
            correlate = correlate == null ? new TreeMap<>() : new TreeMap<>(correlate);
        }
    }

    /**
     * DSL 1.0 Correlation.
     *
     * @param from   "A runtime expression used to extract the correlation value from the filtered event" —
     *               evaluated with the event (structured JSON envelope) as {@code .}
     * @param expect the expected value, evaluated when the instance suspends against the task's input,
     *               {@code $context} and {@code $workflow}; required here (sec. 8a)
     */
    public record Correlation(Expr from, Expr expect) {
    }
}
