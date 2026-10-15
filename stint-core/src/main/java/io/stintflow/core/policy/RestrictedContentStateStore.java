package io.stintflow.core.policy;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.net.URI;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.RetryState;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.ClaimCheck;
import io.stintflow.wire.Json;

/**
 * SDD 3a.5, RF6 (decision 8e, E1): a restricted instance's content never rests in the state store. On each save of a
 * {@link Sensitivity#RESTRICTED} snapshot, its {@code context}, {@code listenState} and the try's {@code dispatchInput}
 * go together into one blob of the {@code restricted} retention class — encrypted and expired by the blob store — under
 * a key of its own per version, and the snapshot keeps only a pointer to it. Its {@code input}, which never changes,
 * goes once into a blob of its own, and <b>stays there for the instance's life</b>: a redaction that lowers the level
 * redacts what flows next, never the raw input (sec. 9). Loads bring the content back. Any other snapshot passes
 * through unchanged; so does everything else (waits, outbox, inbox).
 * <p>
 * <b>Fail closed:</b> a restricted snapshot without a blob store is not saved; a content blob that is gone (deleted, or
 * expired before the instance ended) fails the instance — it never resumes on anything else. Superseded versions'
 * blobs are left to their retention.
 */
public final class RestrictedContentStateStore implements StateStore {

    private static final Logger LOG = System.getLogger(RestrictedContentStateStore.class.getName());

    /** The field of the pointer that stands, in a snapshot, for content at rest in a blob. */
    public static final String AT_REST_FIELD = "__stint_at_rest__";
    static final String CONTEXT = "context";
    static final String LISTEN_STATE = "listenState";
    static final String DISPATCH_INPUT = "dispatchInput";

    private final StateStore delegate;
    private final BlobStore blobs;
    private final Function<WorkflowRef, RetentionClass> retention;
    private final InstantSource clock;
    /** Inputs this store keeps at rest (written, or read back), by value, with their blob. */
    private final java.util.Map<JsonNode, URI> inputsAtRest =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    /**
     * @param blobs     where restricted content goes; {@code null} refuses to save restricted snapshots
     * @param retention the {@code restricted} class of a definition (its policy's, else 30 days)
     */
    public RestrictedContentStateStore(StateStore delegate, BlobStore blobs,
            Function<WorkflowRef, RetentionClass> retention, InstantSource clock) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.blobs = blobs;
        this.retention = Objects.requireNonNull(retention, "retention");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /** The store this one decorates. */
    public StateStore delegate() {
        return delegate;
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox) {
        return atRest(snapshot).thenCompose(stored ->
                delegate.save(stored, expectedVersion, addWaits, consumeWaitKeys, addOutbox));
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys, List<OutboxEntry> addOutbox, List<InboxEntry.Key> removeInbox) {
        return atRest(snapshot).thenCompose(stored ->
                delegate.save(stored, expectedVersion, addWaits, consumeWaitKeys, addOutbox, removeInbox));
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion, List<Wait> addWaits,
            List<String> consumeWaitKeys) {
        return atRest(snapshot).thenCompose(stored ->
                delegate.save(stored, expectedVersion, addWaits, consumeWaitKeys));
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        return delegate.load(instanceId).thenCompose(found -> {
            if (found.isEmpty()) {
                return CompletableFuture.completedFuture(found);
            }
            InstanceSnapshot stored = found.get();
            URI bundle = bundleOf(stored);
            URI input = isAtRest(stored.input()) ? uriOf(stored.input()) : null;
            if (bundle == null && input == null) {
                return CompletableFuture.completedFuture(found);
            }
            CompletionStage<JsonNode> bundleRead = bundle == null ? CompletableFuture.completedFuture(Json.obj())
                    : blobs.get(bundle).thenApply(Json::read);
            CompletionStage<JsonNode> inputRead = input == null ? CompletableFuture.completedFuture(null)
                    : blobs.get(input).thenApply(Json::read);
            return bundleRead.thenCombine(inputRead, (content, restoredInput) -> {
                        if (restoredInput != null) {
                            inputsAtRest.put(restoredInput, input);
                        }
                        return Optional.of(restore(stored, content, restoredInput));
                    })
                    .exceptionallyCompose(failure -> BlobNotFoundException.in(failure)
                            ? contentGone(stored)
                            : CompletableFuture.failedFuture(failure));
        });
    }

    @Override
    public CompletionStage<Optional<Wait>> findWait(String waitKey) {
        return delegate.findWait(waitKey);
    }

    /** Deletes the instance, then its current content blob: erasing the blob erases the content. */
    @Override
    public CompletionStage<Void> delete(String instanceId) {
        return delegate.load(instanceId).thenCompose(found -> {
            List<URI> pointers = new java.util.ArrayList<>();
            found.ifPresent(stored -> {
                if (bundleOf(stored) != null) {
                    pointers.add(bundleOf(stored));
                }
                if (isAtRest(stored.input())) {
                    pointers.add(uriOf(stored.input()));
                }
            });
            CompletionStage<Void> deleted = delegate.delete(instanceId);
            for (URI pointer : pointers) {
                deleted = deleted.thenCompose(v -> blobs.delete(pointer));
            }
            return deleted;
        });
    }

    @Override
    public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
        return delegate.pendingOutbox(createdAtOrBefore, limit);
    }

    @Override
    public CompletionStage<Void> removeOutbox(String eventId) {
        return delegate.removeOutbox(eventId);
    }

    @Override
    public CompletionStage<Void> putInbox(InboxEntry entry) {
        return delegate.putInbox(entry);
    }

    @Override
    public CompletionStage<List<InboxEntry>> findInbox(String waitKey) {
        return delegate.findInbox(waitKey);
    }

    @Override
    public CompletionStage<Void> removeInbox(InboxEntry.Key key) {
        return delegate.removeInbox(key);
    }

    private CompletionStage<InstanceSnapshot> atRest(InstanceSnapshot snapshot) {
        URI inputAtRest = snapshot.input() == null ? null : inputsAtRest.get(snapshot.input());
        if (snapshot.sensitivity() != Sensitivity.RESTRICTED) {
            // lowered by a redaction: what flows next is inline again; the raw input stays where it rests
            return CompletableFuture.completedFuture(inputAtRest == null ? snapshot
                    : copy(snapshot, pointer(inputAtRest), snapshot.context(), snapshot.listenState(),
                            snapshot.retryState()));
        }
        if (blobs == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Instance " + snapshot.instanceId()
                    + " is restricted and the engine has no blob store: its content may not rest in the state store"));
        }
        RetryState retry = snapshot.retryState();
        JsonNode dispatchInput = retry == null ? null : retry.dispatchInput();
        boolean keepDispatchInput = dispatchInput == null || ClaimCheck.isPointer(dispatchInput);
        CompletionStage<URI> input = snapshot.input() == null || inputAtRest != null
                ? CompletableFuture.completedFuture(inputAtRest)
                : blobs.put(Json.bytes(snapshot.input()), "instances/" + snapshot.instanceId() + "/input.json",
                        retention.apply(snapshot.definition())).thenApply(uri -> {
                            inputsAtRest.put(snapshot.input(), uri);
                            return uri;
                        });
        ObjectNode bundle = Json.obj();
        putIfSet(bundle, CONTEXT, snapshot.context());
        putIfSet(bundle, LISTEN_STATE, snapshot.listenState());
        if (!keepDispatchInput) {
            bundle.set(DISPATCH_INPUT, dispatchInput);
        }
        String key = "instances/" + snapshot.instanceId() + "/v" + snapshot.version() + ".json";
        CompletionStage<URI> content = bundle.isEmpty() ? CompletableFuture.completedFuture(null)
                : blobs.put(Json.bytes(bundle), key, retention.apply(snapshot.definition()));
        return input.thenCombine(content, (inputUri, contentUri) -> {
            JsonNode pointer = contentUri == null ? null : pointer(contentUri);
            RetryState storedRetry = retry == null || keepDispatchInput ? retry
                    : new RetryState(retry.tryNodePointer(), retry.attempt(), retry.firstAttemptAt(),
                            retry.lastError(), retry.currentCorrelationId(), pointer);
            return copy(snapshot, inputUri == null ? null : pointer(inputUri),
                    snapshot.context() == null ? null : pointer,
                    snapshot.listenState() == null ? null : pointer, storedRetry);
        });
    }

    private static InstanceSnapshot restore(InstanceSnapshot stored, JsonNode bundle, JsonNode input) {
        RetryState retry = stored.retryState();
        RetryState restoredRetry = retry != null && isAtRest(retry.dispatchInput())
                ? new RetryState(retry.tryNodePointer(), retry.attempt(), retry.firstAttemptAt(), retry.lastError(),
                        retry.currentCorrelationId(), bundle.get(DISPATCH_INPUT))
                : retry;
        return copy(stored, input != null ? input : stored.input(), part(stored.context(), bundle, CONTEXT),
                part(stored.listenState(), bundle, LISTEN_STATE), restoredRetry);
    }

    /**
     * The content is gone: the instance fails, with nothing of it left to read — never resumed on anything else. A
     * concurrent writer that got there first wins; its version is read again.
     */
    private CompletionStage<Optional<InstanceSnapshot>> contentGone(InstanceSnapshot stored) {
        if (stored.status() != InstanceStatus.RUNNING && stored.status() != InstanceStatus.WAITING) {
            // an ended instance whose content expired: nothing to fail, nothing to resume
            return CompletableFuture.completedFuture(Optional.of(copy(stored, null, null, null, null)));
        }
        LOG.log(Level.WARNING, "Instance {0}: its restricted content is gone (deleted, or expired by its retention "
                + "class before the instance ended); the instance fails", stored.instanceId());
        InstanceSnapshot failed = new InstanceSnapshot(stored.instanceId(), stored.definition(), stored.position(),
                null, null, InstanceStatus.FAILED, stored.version() + 1, null, clock.instant(), null,
                stored.startedAt(), stored.chainId(), stored.suspendedAt(), null, stored.decisions(),
                stored.sensitivity());
        List<String> consume = stored.waitingKey() == null ? List.of() : List.of(stored.waitingKey());
        return delegate.save(failed, stored.version(), List.of(), consume).thenCompose(outcome ->
                outcome == SaveOutcome.OK ? CompletableFuture.completedFuture(Optional.of(failed))
                        : load(stored.instanceId()));
    }

    /** The blob of a snapshot's context, listen state and dispatch input, or {@code null}. */
    private static URI bundleOf(InstanceSnapshot snapshot) {
        for (JsonNode node : new JsonNode[] {snapshot.context(), snapshot.listenState(),
                snapshot.retryState() == null ? null : snapshot.retryState().dispatchInput()}) {
            if (isAtRest(node)) {
                return uriOf(node);
            }
        }
        return null;
    }

    private static URI uriOf(JsonNode atRest) {
        return URI.create(atRest.get(AT_REST_FIELD).asText());
    }

    private static JsonNode pointer(URI uri) {
        return Json.obj().put(AT_REST_FIELD, uri.toString());
    }

    static boolean isAtRest(JsonNode node) {
        return node != null && node.isObject() && node.size() == 1 && node.hasNonNull(AT_REST_FIELD);
    }

    private static JsonNode part(JsonNode stored, JsonNode bundle, String field) {
        return isAtRest(stored) ? bundle.get(field) : stored;
    }

    private static void putIfSet(ObjectNode bundle, String field, JsonNode value) {
        if (value != null) {
            bundle.set(field, value);
        }
    }

    private static InstanceSnapshot copy(InstanceSnapshot s, JsonNode input, JsonNode context, JsonNode listenState,
            RetryState retry) {
        return new InstanceSnapshot(s.instanceId(), s.definition(), s.position(), s.waitingKey(), context, s.status(),
                s.version(), retry, s.updatedAt(), input, s.startedAt(), s.chainId(), s.suspendedAt(), listenState,
                s.decisions(), s.sensitivity());
    }
}
