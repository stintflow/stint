package io.stintflow.worker.langchain4j.memory;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Port: the conversation memory of LLM workers (SDD 3a.3, 5.3). It lives with the worker, never in the engine's
 * {@code stint-spi}, and knows nothing of LangChain4j: messages are opaque JSON, so an adapter needs no AI library.
 * <p>
 * Exchanges are only ever added whole, once per correlationId ({@link #commit}), so an at-least-once redelivery can't
 * duplicate the history. Adapters drop exchanges past their {@link Exchange#expiresAt()} from every read.
 */
public interface ConversationStore {

    /** The unexpired exchanges committed under {@code memory}, oldest first (by committedAt, then correlationId). */
    CompletionStage<List<Exchange>> committed(MemoryKey memory);

    /** Records an exchange, atomically: the first commit of an instanceId + correlationId wins, any later one is a no-op. */
    CompletionStage<CommitOutcome> commit(Exchange exchange);

    /** Every exchange of an instance, in every scope — what {@link #deleteInstance} is about to delete. */
    CompletionStage<List<Exchange>> exchangesOf(String instanceId);

    /** Deletes everything an instance remembers, in every scope. Idempotent. */
    CompletionStage<Void> deleteInstance(String instanceId);
}
