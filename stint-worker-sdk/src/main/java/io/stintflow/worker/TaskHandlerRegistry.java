package io.stintflow.worker;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Maps a routing key (the CloudEvent {@code subject}) to the handler that runs that task type. */
public final class TaskHandlerRegistry {

    private final Map<String, TaskHandler> byRoutingKey = new ConcurrentHashMap<>();

    public TaskHandlerRegistry register(String routingKey, TaskHandler handler) {
        byRoutingKey.put(routingKey, handler);
        return this;
    }

    public Optional<TaskHandler> find(String routingKey) {
        return Optional.ofNullable(byRoutingKey.get(routingKey));
    }
}
