package io.stintflow.worker.langchain4j.memory;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;

/**
 * Keeps big exchanges out of the store (SDD 3a.3, 5.3): an exchange whose messages and output together pass
 * {@code maxInlineBytes} goes to the {@link BlobStore}, and the store keeps a claim-check pointer in its place. Reads
 * rehydrate it; {@link #deleteInstance} deletes the blobs too. Works over any adapter.
 */
public final class BlobOffloadingConversationStore implements ConversationStore {

    /** Below a DynamoDB item's 400 KB, with room for the rest of the item. */
    public static final int DEFAULT_MAX_INLINE_BYTES = 64 * 1024;

    private static final String MESSAGES = "messages";
    private static final String OUTPUT = "output";

    private final ConversationStore delegate;
    private final BlobStore blobs;
    private final int maxInlineBytes;
    private final RetentionClass retention;

    public BlobOffloadingConversationStore(ConversationStore delegate, BlobStore blobs) {
        this(delegate, blobs, DEFAULT_MAX_INLINE_BYTES);
    }

    public BlobOffloadingConversationStore(ConversationStore delegate, BlobStore blobs, int maxInlineBytes) {
        this(delegate, blobs, maxInlineBytes, null);
    }

    /**
     * SDD 3a.5, RF6: with a {@code retention} class, blobs go under it (encrypted and expired by the blob store); with
     * {@code maxInlineBytes} 0, every exchange goes by pointer — a restricted instance's memory.
     */
    public BlobOffloadingConversationStore(ConversationStore delegate, BlobStore blobs, int maxInlineBytes,
            RetentionClass retention) {
        this.delegate = delegate;
        this.blobs = blobs;
        this.maxInlineBytes = maxInlineBytes;
        this.retention = retention;
    }

    /** The same memory, every exchange by pointer under {@code retention}. */
    public BlobOffloadingConversationStore allByPointer(RetentionClass retention) {
        return new BlobOffloadingConversationStore(delegate, blobs, -1, retention);
    }

    @Override
    public CompletionStage<List<Exchange>> committed(MemoryKey memory) {
        return delegate.committed(memory).thenCompose(this::rehydrateAll);
    }

    @Override
    public CompletionStage<CommitOutcome> commit(Exchange exchange) {
        ObjectNode payload = Json.obj();
        payload.set(MESSAGES, exchange.messages());
        payload.set(OUTPUT, exchange.output());
        byte[] bytes = Json.bytes(payload);
        if (bytes.length <= maxInlineBytes) {
            return delegate.commit(exchange);
        }
        // A key of its own per commit: a concurrent commit of the same correlationId that loses deletes only its blob.
        String key = "conversations/" + exchange.memory().instanceId() + "/" + exchange.correlationId() + "-"
                + UUID.randomUUID() + ".json";
        return (retention == null ? blobs.put(bytes, key) : blobs.put(bytes, key, retention)).thenCompose(uri -> {
            ObjectNode pointer = Json.obj();
            pointer.put(ClaimCheck.POINTER_FIELD, uri.toString());
            return delegate.commit(exchange.withPayload(pointer, null)).thenCompose(outcome ->
                    outcome == CommitOutcome.COMMITTED ? CompletableFuture.completedFuture(outcome)
                            : blobs.delete(uri).thenApply(v -> outcome));
        });
    }

    @Override
    public CompletionStage<List<Exchange>> exchangesOf(String instanceId) {
        return delegate.exchangesOf(instanceId).thenCompose(this::rehydrateAll);
    }

    /** Deletes the exchanges first, then their blobs: a crash in between leaves orphan blobs, never dangling pointers. */
    @Override
    public CompletionStage<Void> deleteInstance(String instanceId) {
        return delegate.exchangesOf(instanceId).thenCompose(exchanges -> delegate.deleteInstance(instanceId)
                .thenCompose(v -> {
                    CompletionStage<Void> deleted = CompletableFuture.completedFuture(null);
                    for (Exchange exchange : exchanges) {
                        if (isPointer(exchange.messages())) {
                            URI uri = URI.create(
                                    exchange.messages().get(ClaimCheck.POINTER_FIELD).asText());
                            deleted = deleted.thenCompose(x -> blobs.delete(uri));
                        }
                    }
                    return deleted;
                }));
    }

    private CompletionStage<List<Exchange>> rehydrateAll(List<Exchange> exchanges) {
        CompletionStage<List<Exchange>> all = CompletableFuture.completedFuture(new ArrayList<>());
        for (Exchange exchange : exchanges) {
            all = all.thenCompose(list -> rehydrate(exchange).thenApply(e -> {
                list.add(e);
                return list;
            }));
        }
        return all;
    }

    private CompletionStage<Exchange> rehydrate(Exchange exchange) {
        if (!isPointer(exchange.messages())) {
            return CompletableFuture.completedFuture(exchange);
        }
        return ClaimCheck.rehydrate(exchange.messages(), blobs)
                .thenApply(payload -> exchange.withPayload(payload.get(MESSAGES), payload.get(OUTPUT)));
    }

    private static boolean isPointer(JsonNode messages) {
        return messages != null && messages.has(ClaimCheck.POINTER_FIELD);
    }
}
