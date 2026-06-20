package io.stintflow.spi;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Port: durable checkpoint storage for suspended workflow instances.
 * <p>
 * The {@link #instanceWaitingFor(String)} lookup is what links "a result just arrived" to "the
 * instance that was waiting on it" — so the implementation must index by correlation id
 * (e.g. a DynamoDB GSI or a Postgres index).
 */
public interface StateStore {

    CompletionStage<Void> save(InstanceSnapshot snapshot);

    CompletionStage<Optional<InstanceSnapshot>> load(String instanceId);

    CompletionStage<Void> delete(String instanceId);

    /** @return the instance id currently waiting on {@code correlationId}, if any. */
    CompletionStage<Optional<String>> instanceWaitingFor(String correlationId);
}
