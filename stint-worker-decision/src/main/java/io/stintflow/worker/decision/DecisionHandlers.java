package io.stintflow.worker.decision;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;

/**
 * Builds a {@link DecisionHandler} for every route of a {@link DecisionConfig} (SDD 3a.2, 4.2): the routing key of a
 * {@code call: decision} ({@code with.task}) names the decision, and the worker's configuration says which adapter
 * answers it. The workflow never names an adapter, a threshold or a credential.
 * <pre>{@code
 * DecisionHandlers.builder(DecisionConfig.fromProperties(props))
 *         .adapter(llmAdapter)
 *         .adapter(jevAdapter)
 *         .build()
 *         .registerAll(registry);
 * }</pre>
 * {@link Builder#build()} fails when a route names an adapter that isn't there, or the configuration has settings for
 * one that isn't — so a typo stops the worker from starting.
 */
public final class DecisionHandlers {

    private final Map<String, DecisionHandler> byRoutingKey;

    private DecisionHandlers(Map<String, DecisionHandler> byRoutingKey) {
        this.byRoutingKey = Map.copyOf(byRoutingKey);
    }

    public static Builder builder(DecisionConfig config) {
        return new Builder(config);
    }

    public Map<String, ? extends TaskHandler> handlers() {
        return byRoutingKey;
    }

    public TaskHandlerRegistry registerAll(TaskHandlerRegistry registry) {
        byRoutingKey.forEach(registry::register);
        return registry;
    }

    public static final class Builder {
        private final DecisionConfig config;
        private final Map<String, DecisionAdapter> adapters = new LinkedHashMap<>();
        private final Map<String, ConfidenceCalibration> calibrations = new HashMap<>();
        private DecisionPolicy policy = DecisionPolicy.ALLOW;
        private DecisionUsageSink usage = DecisionUsageSink.LOG;

        private Builder(DecisionConfig config) {
            this.config = Objects.requireNonNull(config, "config");
        }

        /**
         * An adapter calibrated by its configuration: {@link TemperatureScaling} with its
         * {@code calibration.temperature}, or {@link ConfidenceCalibration#IDENTITY} without one.
         */
        public Builder adapter(DecisionAdapter adapter) {
            add(adapter);
            return this;
        }

        /**
         * An adapter whose raw confidence is mapped by {@code calibration} before its threshold. Its configuration
         * then must not have a {@code calibration.temperature} too.
         */
        public Builder adapter(DecisionAdapter adapter, ConfidenceCalibration calibration) {
            calibrations.put(add(adapter), Objects.requireNonNull(calibration, "calibration"));
            return this;
        }

        private String add(DecisionAdapter adapter) {
            String id = adapter.descriptor().id();
            if (adapters.putIfAbsent(id, adapter) != null) {
                throw new IllegalArgumentException("Two decision adapters share the id '" + id + "'");
            }
            return id;
        }

        /** The SDD 3a.5 policy's hook. Default {@link DecisionPolicy#ALLOW}. */
        public Builder policy(DecisionPolicy policy) {
            this.policy = Objects.requireNonNull(policy, "policy");
            return this;
        }

        public Builder usage(DecisionUsageSink usage) {
            this.usage = Objects.requireNonNull(usage, "usage");
            return this;
        }

        public DecisionHandlers build() {
            List<String> problems = new ArrayList<>();
            config.routes().values().stream().filter(r -> !adapters.containsKey(r.adapter())).forEach(r -> problems.add(
                    DecisionConfig.PREFIX + "routes." + r.routingKey() + ".adapter: no adapter '" + r.adapter() + "'"));
            config.configuredAdapters().stream().filter(id -> !adapters.containsKey(id)).forEach(id -> problems.add(
                    DecisionConfig.PREFIX + "adapters." + id + ": no adapter '" + id + "'"));
            calibrations.keySet().stream().filter(id -> config.temperature(id).isPresent()).forEach(id -> problems.add(
                    DecisionConfig.PREFIX + "adapters." + id + ".calibration.temperature: adapter '" + id
                            + "' also has a calibration in code; keep one"));
            if (!problems.isEmpty()) {
                throw new IllegalArgumentException("Invalid decision configuration:\n  - "
                        + String.join("\n  - ", problems) + "\n  (adapters: " + adapters.keySet() + ")");
            }
            Map<String, DecisionHandler> handlers = new HashMap<>();
            config.routes().forEach((key, route) -> handlers.put(key, new DecisionHandler(route,
                    adapters.get(route.adapter()), config.adapter(route.adapter()), calibration(route.adapter()),
                    policy, usage)));
            return new DecisionHandlers(handlers);
        }

        private ConfidenceCalibration calibration(String id) {
            ConfidenceCalibration inCode = calibrations.get(id);
            if (inCode != null) {
                return inCode;
            }
            return config.temperature(id).<ConfidenceCalibration>map(TemperatureScaling::new)
                    .orElse(ConfidenceCalibration.IDENTITY);
        }
    }
}
