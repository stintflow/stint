package io.stintflow.worker.knowledge;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.InstantSource;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.inmemory.InMemoryAreaKeys;
import io.stintflow.inmemory.InMemoryAuditLog;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.DataKey;
import io.stintflow.spi.ErrorInfo.StandardType;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskFailure;

/** SDD 3b.5b (CA6, CA8): the task that destroys the key of an area — idempotent, only for the areas served, and what it leaves in the audit log. */
class AreaKeyDestroyHandlerTest {

    private final InMemoryAreaKeys keys = new InMemoryAreaKeys();
    private final InMemoryAuditLog audit = new InMemoryAuditLog();
    private final AreaKeyDestroyHandler handler = new AreaKeyDestroyHandler(keys, Set.of("examples"), audit, InstantSource.system());

    private static ObjectNode request(String requestId, String area) {
        ObjectNode in = Json.obj();
        if (requestId != null) {
            in.put("requestId", requestId);
        }
        if (area != null) {
            in.put("area", area);
        }
        return in;
    }

    private JsonNode run(AreaKeyDestroyHandler which, ObjectNode input) {
        return which.execute(KnowledgeFixture.task("i-ak", input, Sensitivity.RESTRICTED)).toCompletableFuture().join();
    }

    private TaskFailure failure(AreaKeyDestroyHandler which, ObjectNode input) {
        try {
            run(which, input);
        } catch (CompletionException e) {
            return (TaskFailure) e.getCause();
        }
        throw new AssertionError("no failure");
    }

    @Test
    void it_destroys_the_key_of_an_area_it_serves_and_asking_again_ends_the_same() {
        keys.newDataKey("examples").toCompletableFuture().join();

        JsonNode first = run(handler, request("r-1", "examples"));
        JsonNode again = run(handler, request("r-1", "examples"));

        assertThat(first.path("state").asText()).isEqualTo("DESTROYED");
        assertThat(again).isEqualTo(first);
        assertThat(keys.state("examples").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
    }

    @Test
    void it_leaves_the_area_and_the_state_in_the_audit_log_and_nothing_else() {
        keys.newDataKey("examples").toCompletableFuture().join();

        run(handler, request("r-2", "examples"));

        var records = audit.all();
        assertThat(records).hasSize(1);
        assertThat(records.get(0).kind()).isEqualTo(AreaKeyDestroyHandler.AUDIT_KIND);
        assertThat(records.get(0).detail()).containsOnlyKeys("area", "state");
        assertThat(records.toString()).doesNotContain("requestId");
    }

    @Test
    void an_area_it_does_not_serve_and_a_malformed_request_are_refused_before_anything_is_destroyed() {
        keys.newDataKey("other").toCompletableFuture().join();

        assertThat(failure(handler, request("r-3", "other")).toErrorInfo().status()).isEqualTo(403);
        assertThat(keys.state("other").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ACTIVE);
        assertThat(failure(handler, request(null, "examples")).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(handler, request("r-3", "Bad Area")).toErrorInfo().status()).isEqualTo(400);
        assertThat(failure(handler, request("r-3", null)).toErrorInfo().status()).isEqualTo(400);
        AreaKeyDestroyHandler none = new AreaKeyDestroyHandler(keys, Set.of(), audit, InstantSource.system());
        assertThat(failure(none, request("r-3", "examples")).toErrorInfo().status()).isEqualTo(403);
    }

    @Test
    void a_provider_that_fails_or_does_not_destroy_is_unavailable_so_the_engine_retries() {
        AreaKeys failing = new Delegating(keys) {
            @Override
            public CompletionStage<State> destroy(String area) {
                return CompletableFuture.failedFuture(new IllegalStateException("kms is down: secret-detail"));
            }
        };
        TaskFailure down = failure(new AreaKeyDestroyHandler(failing, Set.of("examples"), audit, InstantSource.system()),
                request("r-4", "examples"));
        assertThat(down.toErrorInfo().type()).isEqualTo(StandardType.COMMUNICATION.uri());
        assertThat(down.toErrorInfo().toString()).doesNotContain("secret-detail");

        AreaKeys lying = new Delegating(keys) {
            @Override
            public CompletionStage<State> destroy(String area) {
                return CompletableFuture.completedFuture(State.DESTROYED);   // says so, and the key is still there
            }
        };
        keys.newDataKey("examples").toCompletableFuture().join();
        TaskFailure notYet = failure(new AreaKeyDestroyHandler(lying, Set.of("examples"), audit, InstantSource.system()),
                request("r-5", "examples"));
        assertThat(notYet.toErrorInfo().type()).isEqualTo(StandardType.COMMUNICATION.uri());
        assertThat(audit.all()).isEmpty();
    }

    private static class Delegating implements AreaKeys {
        private final AreaKeys inner;

        Delegating(AreaKeys inner) {
            this.inner = inner;
        }

        @Override
        public CompletionStage<DataKey> newDataKey(String area) {
            return inner.newDataKey(area);
        }

        @Override
        public CompletionStage<byte[]> unwrap(String area, byte[] wrappedKey) {
            return inner.unwrap(area, wrappedKey);
        }

        @Override
        public CompletionStage<State> destroy(String area) {
            return inner.destroy(area);
        }

        @Override
        public CompletionStage<State> state(String area) {
            return inner.state(area);
        }
    }
}
