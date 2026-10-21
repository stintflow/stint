package io.stintflow.worker.knowledge;

import java.time.InstantSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.AuditLog;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;

/**
 * The task {@code area-key-destroy} (SDD 3b.5b, 4.5): destroys the key of an area, so that every copy of the blobs encrypted
 * under it — wherever it is — can no longer be read. Irreversible; idempotent (the area ends destroyed however many times it is
 * asked).
 * <p>
 * <b>Input.</b> {@code {"requestId": "...", "area": "..."}}. <b>Answer.</b> {@code {"requestId", "area", "state"}}, the state
 * being {@code DESTROYED}. <b>Failure.</b> An area this worker does not serve (empty list: none): {@code authorization} 403;
 * a malformed request: {@code validation} 400; a provider that could not be reached, or an area that is not destroyed after the
 * call: {@code communication} 503 — the engine retries it. <b>Audit.</b> A record of kind {@value #AUDIT_KIND}: the area and the
 * state; nothing of the blobs, and never a key.
 */
public final class AreaKeyDestroyHandler implements TaskHandler {

    public static final String AUDIT_KIND = "area-key-destroy";

    private static final Logger LOG = LoggerFactory.getLogger(AreaKeyDestroyHandler.class);

    private final AreaKeys keys;
    private final Set<String> areas;
    private final AuditLog audit;
    private final InstantSource clock;

    public AreaKeyDestroyHandler(AreaKeys keys, Set<String> areas, AuditLog audit, InstantSource clock) {
        this.keys = Objects.requireNonNull(keys, "keys");
        this.areas = Set.copyOf(areas);
        this.audit = audit == null ? AuditLog.NONE : audit;
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        try {
            return CompletableFuture.completedFuture(destroy(ctx));
        } catch (TaskFailure failure) {
            return CompletableFuture.failedFuture(failure);
        } catch (RuntimeException e) {
            LOG.warn("Destroying the key of an area in task {} of instance {} failed ({})", ctx.taskId(), ctx.instanceId(),
                    e.getClass().getName());
            return CompletableFuture.failedFuture(Failures.unavailable("The key of the area could not be destroyed ("
                    + e.getClass().getSimpleName() + ")"));
        }
    }

    private JsonNode destroy(TaskContext ctx) {
        JsonNode in = ctx.input();
        String requestId = in.path("requestId").asText("");
        String area = in.path("area").asText("");
        if (requestId.isBlank() || !AreaKeys.AREA.matcher(area).matches()) {
            throw Failures.validation("requestId and a valid area are required");
        }
        if (!areas.contains(area)) {
            throw Failures.refused("This worker does not serve that area");
        }
        AreaKeys.State state;
        try {
            keys.destroy(area).toCompletableFuture().join();
            state = keys.state(area).toCompletableFuture().join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw Failures.unavailable("The key of the area could not be destroyed (" + cause.getClass().getSimpleName() + ")");
        }
        if (state != AreaKeys.State.DESTROYED) {
            throw Failures.unavailable("The key of the area is not destroyed yet");
        }
        LOG.info("The key of area {} was destroyed (task {}, instance {})", area, ctx.taskId(), ctx.instanceId());
        Map<String, String> detail = new LinkedHashMap<>();
        detail.put("area", area);
        detail.put("state", state.name());
        audit.append(new AuditLog.AuditRecord(ctx.instanceId(), ctx.chainId(), clock.instant(), AUDIT_KIND, ctx.taskId(),
                ctx.sensitivity(), "area-key-destroy", "answered", detail)).toCompletableFuture().join();
        ObjectNode out = Json.obj();
        out.put("requestId", requestId).put("area", area).put("state", state.name());
        return out;
    }
}
