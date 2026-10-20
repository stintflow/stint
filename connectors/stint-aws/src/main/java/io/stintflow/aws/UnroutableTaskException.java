package io.stintflow.aws;

import java.util.Set;
import java.util.TreeSet;

/**
 * SDD 3a.9: a task's routing key matches no route of the {@link RoutingSqsTaskTransport} and the transport has no
 * default invoke queue. Names the key and the keys the transport knows — never a queue URL.
 */
public final class UnroutableTaskException extends RuntimeException {

    private final String routingKey;

    public UnroutableTaskException(String routingKey, Set<String> knownKeys) {
        super("No route for routing key '" + routingKey + "' and no default invoke queue; known keys: "
                + new TreeSet<>(knownKeys));
        this.routingKey = routingKey;
    }

    public String routingKey() {
        return routingKey;
    }
}
