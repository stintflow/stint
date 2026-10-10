package io.stintflow.inmemory;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.StateStore;

/**
 * Shared wiring for the SDD 2.2 tests: an engine with an {@link InMemoryEventPublisher} as its domain
 * channel, a filesystem facts store, a capturing task transport (no worker) and a controllable clock —
 * everything synchronous, so a {@code start}/{@code deliverResult} stage covers save + publish.
 */
final class EmitFixture {

    final MutableClock clock = new MutableClock();
    final WorkflowRegistry registry = new WorkflowRegistry();
    final StateStore state;
    final InMemoryEventPublisher publisher;
    final DomainEventFixture.CapturingTransport transport = new DomainEventFixture.CapturingTransport();
    final InMemoryTimerService timer = new InMemoryTimerService(clock);
    final BlobStore facts;
    final WorkflowEngine engine;

    EmitFixture(WorkflowDefinition... defs) throws Exception {
        this(new InMemoryStateStore(), new InMemoryEventPublisher(), defs);
    }

    EmitFixture(StateStore state, InMemoryEventPublisher publisher, WorkflowDefinition... defs) throws Exception {
        for (WorkflowDefinition def : defs) {
            registry.register(def);
        }
        this.state = state;
        this.publisher = publisher;
        this.facts = synchronous(new FilesystemBlobStore(Files.createTempDirectory("stint-sdd22-facts")));
        this.engine = new WorkflowEngine(registry, transport, state, timer,
                new FilesystemBlobStore(Files.createTempDirectory("stint-sdd22-blobs")),
                new TreeInterpreter(new JqExpressionEvaluator()), clock, publisher, facts);
    }

    InstanceSnapshot load(String instanceId) throws Exception {
        return state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
    }

    /** CA6: everything the outbox ever held, whatever its age. */
    List<OutboxEntry> outbox() throws Exception {
        return state.pendingOutbox(clock.instant().plus(Duration.ofDays(3650)), 1000)
                .toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    /** Completes {@code put} before returning, so a {@code start} stage stays fully synchronous in tests. */
    static BlobStore synchronous(BlobStore delegate) {
        return new BlobStore() {
            @Override
            public CompletionStage<URI> put(byte[] data, String key) {
                return CompletableFuture.completedFuture(delegate.put(data, key).toCompletableFuture().join());
            }

            @Override
            public CompletionStage<byte[]> get(URI ref) {
                return delegate.get(ref);
            }

            @Override
            public CompletionStage<Void> delete(URI ref) {
                return delegate.delete(ref);
            }
        };
    }

    static final class MutableClock implements InstantSource {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.parse("2026-01-01T00:00:00Z"));

        @Override
        public Instant instant() {
            return now.get();
        }

        void advance(Duration duration) {
            now.updateAndGet(i -> i.plus(duration));
        }
    }
}
