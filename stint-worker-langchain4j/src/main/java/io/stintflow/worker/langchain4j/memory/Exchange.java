package io.stintflow.worker.langchain4j.memory;

import java.time.Instant;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * One execution's contribution to a conversation, committed in one act when the execution succeeded (SDD 3a.3, 5.4).
 * Its identity is {@code memory.instanceId} + {@code correlationId}: a redelivery of the same message has the same
 * correlationId, a retry a new one.
 *
 * @param memory        the conversation it belongs to
 * @param correlationId the dispatch that produced it
 * @param taskId        the task's position, for diagnosis
 * @param attempt       the dispatch's attempt, for diagnosis
 * @param committedAt   when the worker committed it, strictly after every exchange its execution read (SDD 3a.3b):
 *                      orders the conversation
 * @param messages      the messages the execution added, as LangChain4j serializes them (a JSON array) — or a
 *                      claim-check pointer to them, see {@link BlobOffloadingConversationStore}
 * @param output        the output the execution returned, so a redelivery returns it without calling the model
 * @param expiresAt     when the exchange may be dropped (retention)
 */
public record Exchange(MemoryKey memory, String correlationId, String taskId, int attempt, Instant committedAt,
        JsonNode messages, JsonNode output, Instant expiresAt) {

    public Exchange withPayload(JsonNode messages, JsonNode output) {
        return new Exchange(memory, correlationId, taskId, attempt, committedAt, messages, output, expiresAt);
    }
}
