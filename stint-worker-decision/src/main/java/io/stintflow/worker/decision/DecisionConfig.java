package io.stintflow.worker.decision;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import io.stintflow.wire.decision.DecisionKind;

/**
 * The decision configuration of a worker (SDD 3a.2, 4.3): which adapter answers each routing key, with which question,
 * and each adapter's threshold, token ceiling and prices. Read from properties under {@value #PREFIX}; anything wrong —
 * an unknown key, a route without an adapter, a threshold outside [0, 1] — fails {@link #fromProperties} with every
 * problem and its key, so a misconfigured worker doesn't start. Credentials never go here.
 * <pre>
 * stint.decision.routes.classify-meeting.adapter=llm
 * stint.decision.routes.classify-meeting.instructions=Which document does this meeting produce?
 * stint.decision.adapters.llm.threshold=0.8
 * stint.decision.adapters.llm.threshold.boolean=0.9
 * stint.decision.adapters.llm.max-input-tokens=20000
 * stint.decision.adapters.llm.price.input-per-million=0.15
 * stint.decision.adapters.llm.price.output-per-million=0.60
 * stint.decision.adapters.llm.calibration.temperature=1.5
 * </pre>
 */
public final class DecisionConfig {

    public static final String PREFIX = "stint.decision.";
    /** Arbitrary until adapters are calibrated (SDD 3a.2, 4.3). */
    public static final double DEFAULT_THRESHOLD = 0.7;
    public static final String DEFAULT_INSTRUCTIONS = "Choose the option that best fits the input.";

    /** One routing key: the adapter that answers it and the question. */
    public record Route(String routingKey, String adapter, String instructions) {
    }

    /** One adapter's settings. */
    public record AdapterSettings(double threshold, Map<DecisionKind, Double> thresholdByKind, Integer maxInputTokens,
            BigDecimal inputPricePerMillion, BigDecimal outputPricePerMillion) {

        public static final AdapterSettings DEFAULTS = new AdapterSettings(DEFAULT_THRESHOLD, Map.of(), null, null, null);

        public double thresholdFor(DecisionKind kind) {
            return thresholdByKind.getOrDefault(kind, threshold);
        }

        /** Tokens times the prices, or {@code null} when no price is configured. */
        public BigDecimal cost(long inputTokens, long outputTokens) {
            if (inputPricePerMillion == null && outputPricePerMillion == null) {
                return null;
            }
            BigDecimal million = BigDecimal.valueOf(1_000_000);
            BigDecimal in = inputPricePerMillion == null ? BigDecimal.ZERO
                    : inputPricePerMillion.multiply(BigDecimal.valueOf(inputTokens)).divide(million);
            BigDecimal out = outputPricePerMillion == null ? BigDecimal.ZERO
                    : outputPricePerMillion.multiply(BigDecimal.valueOf(outputTokens)).divide(million);
            return in.add(out);
        }
    }

    private final Map<String, Route> routes;
    private final Map<String, AdapterSettings> adapters;
    private final Map<String, Double> temperatures;

    private DecisionConfig(Map<String, Route> routes, Map<String, AdapterSettings> adapters,
            Map<String, Double> temperatures) {
        this.routes = Map.copyOf(routes);
        this.adapters = Map.copyOf(adapters);
        this.temperatures = Map.copyOf(temperatures);
    }

    public Map<String, Route> routes() {
        return routes;
    }

    /** Settings of {@code adapterId}: the configured ones, or the defaults. */
    public AdapterSettings adapter(String adapterId) {
        return adapters.getOrDefault(adapterId, AdapterSettings.DEFAULTS);
    }

    /**
     * The temperature of {@code adapterId}'s {@link TemperatureScaling} ({@code calibration.temperature}, SDD 3a.2b,
     * 4.5), or empty.
     */
    public Optional<Double> temperature(String adapterId) {
        return Optional.ofNullable(temperatures.get(adapterId));
    }

    /** Adapter ids the configuration has settings for. */
    public Set<String> configuredAdapters() {
        return adapters.keySet();
    }

    /**
     * Reads every property under {@value #PREFIX}; the others are ignored.
     *
     * @throws IllegalArgumentException with every problem found, each with its key
     */
    public static DecisionConfig fromProperties(Map<String, String> properties) {
        List<String> problems = new ArrayList<>();
        Map<String, String> routeAdapters = new TreeMap<>();
        Map<String, String> routeInstructions = new TreeMap<>();
        Map<String, Map<String, String>> adapterKeys = new TreeMap<>();

        for (Map.Entry<String, String> entry : new TreeMap<>(properties).entrySet()) {
            String key = entry.getKey();
            if (!key.startsWith(PREFIX)) {
                continue;
            }
            String rest = key.substring(PREFIX.length());
            String value = entry.getValue() == null ? "" : entry.getValue().trim();
            if (rest.startsWith("routes.") && rest.endsWith(".adapter")) {
                String route = rest.substring("routes.".length(), rest.length() - ".adapter".length());
                if (route.isEmpty() || value.isEmpty()) {
                    problems.add(key + ": a route needs a routing key and an adapter id");
                } else {
                    routeAdapters.put(route, value);
                }
            } else if (rest.startsWith("routes.") && rest.endsWith(".instructions")) {
                String route = rest.substring("routes.".length(), rest.length() - ".instructions".length());
                if (value.isEmpty()) {
                    problems.add(key + ": instructions must not be empty");
                } else {
                    routeInstructions.put(route, value);
                }
            } else if (rest.startsWith("adapters.") && rest.indexOf('.', "adapters.".length()) > 0) {
                int dot = rest.indexOf('.', "adapters.".length());
                String adapter = rest.substring("adapters.".length(), dot);
                adapterKeys.computeIfAbsent(adapter, a -> new HashMap<>()).put(rest.substring(dot + 1), value);
            } else {
                problems.add(key + ": unknown decision key");
            }
        }

        Map<String, Route> routes = new HashMap<>();
        routeAdapters.forEach((route, adapter) -> routes.put(route,
                new Route(route, adapter, routeInstructions.getOrDefault(route, DEFAULT_INSTRUCTIONS))));
        routeInstructions.keySet().stream().filter(route -> !routeAdapters.containsKey(route)).forEach(route ->
                problems.add(PREFIX + "routes." + route + ".adapter: route '" + route + "' has instructions but no adapter"));

        Map<String, AdapterSettings> adapters = new HashMap<>();
        Map<String, Double> temperatures = new HashMap<>();
        adapterKeys.forEach((adapter, keys) -> {
            String temperature = keys.remove("calibration.temperature");
            if (temperature != null) {
                temperature(PREFIX + "adapters." + adapter + ".calibration.temperature", temperature, problems)
                        .ifPresent(t -> temperatures.put(adapter, t));
            }
            adapters.put(adapter, settings(adapter, keys, problems));
        });

        if (!problems.isEmpty()) {
            throw new IllegalArgumentException("Invalid decision configuration:\n  - " + String.join("\n  - ", problems));
        }
        return new DecisionConfig(routes, adapters, temperatures);
    }

    private static AdapterSettings settings(String adapter, Map<String, String> keys, List<String> problems) {
        String base = PREFIX + "adapters." + adapter + ".";
        double threshold = DEFAULT_THRESHOLD;
        Map<DecisionKind, Double> byKind = new EnumMap<>(DecisionKind.class);
        Integer maxInputTokens = null;
        BigDecimal priceIn = null;
        BigDecimal priceOut = null;
        for (Map.Entry<String, String> entry : new TreeMap<>(keys).entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            String key = base + name;
            if (name.equals("threshold")) {
                threshold = threshold(key, value, problems).orElse(threshold);
            } else if (name.startsWith("threshold.")) {
                String kind = name.substring("threshold.".length());
                Optional<DecisionKind> parsed = kind(kind);
                if (parsed.isEmpty()) {
                    problems.add(key + ": unknown decision kind '" + kind + "' (choice, score or boolean)");
                } else {
                    threshold(key, value, problems).ifPresent(t -> byKind.put(parsed.get(), t));
                }
            } else if (name.equals("max-input-tokens")) {
                try {
                    maxInputTokens = Integer.valueOf(value);
                    if (maxInputTokens < 1) {
                        problems.add(key + ": must be a positive integer");
                    }
                } catch (NumberFormatException e) {
                    problems.add(key + ": must be a positive integer");
                }
            } else if (name.equals("price.input-per-million")) {
                priceIn = price(key, value, problems);
            } else if (name.equals("price.output-per-million")) {
                priceOut = price(key, value, problems);
            } else {
                problems.add(key + ": unknown adapter key");
            }
        }
        return new AdapterSettings(threshold, Map.copyOf(byKind), maxInputTokens, priceIn, priceOut);
    }

    private static Optional<Double> threshold(String key, String value, List<String> problems) {
        try {
            double threshold = Double.parseDouble(value);
            if (threshold >= 0 && threshold <= 1) {
                return Optional.of(threshold);
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        problems.add(key + ": a threshold is a number in [0, 1], got '" + value + "'");
        return Optional.empty();
    }

    private static Optional<Double> temperature(String key, String value, List<String> problems) {
        try {
            double t = Double.parseDouble(value);
            if (t > 0 && Double.isFinite(t)) {
                return Optional.of(t);
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        problems.add(key + ": a temperature is a finite number above 0, got '" + value + "'");
        return Optional.empty();
    }

    private static BigDecimal price(String key, String value, List<String> problems) {
        try {
            BigDecimal price = new BigDecimal(value);
            if (price.signum() >= 0) {
                return price;
            }
        } catch (NumberFormatException e) {
            // reported below
        }
        problems.add(key + ": a price is a non-negative decimal, got '" + value + "'");
        return null;
    }

    private static Optional<DecisionKind> kind(String name) {
        for (DecisionKind kind : DecisionKind.values()) {
            if (kind.wireName().equals(name.toLowerCase(Locale.ROOT))) {
                return Optional.of(kind);
            }
        }
        return Optional.empty();
    }
}
