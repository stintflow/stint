package io.stintflow.inmemory;

import java.net.URI;
import java.nio.file.Files;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.EventCorrelation;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.core.expr.Expr;
import io.stintflow.core.expr.JqExpressionEvaluator;
import io.stintflow.core.interpreter.TreeInterpreter;
import io.stintflow.core.model.ListenNode;
import io.stintflow.core.trigger.DomainEventRouter;
import io.stintflow.core.trigger.ResumeReaction;
import io.stintflow.spi.InboxEntry;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TimerFire;
import io.stintflow.spi.TimerFireHandler;
import io.stintflow.spi.TimerRequest;
import io.stintflow.spi.TimerService;
import io.stintflow.wire.Json;

/**
 * Shared wiring for the SDD 2.3 tests: an engine whose domain consumer runs {@link ResumeReaction} on an
 * {@link InMemoryDomainEventBus}, a capturing transport (no worker), an {@link InMemoryEventPublisher} (not
 * looped back; tests subscribe responders) and a timer driven by a controllable clock — no sleeps.
 */
final class ListenFixture {

    static final String REVIEWED = "io.acme.review.completed.v1";
    static final String APPROVED = "io.acme.billing.approved.v1";
    static final URI SOURCE = URI.create("https://acme.example/review");

    final EmitFixture.MutableClock clock = new EmitFixture.MutableClock();
    final WorkflowRegistry registry = new WorkflowRegistry();
    final StateStore state;
    final InMemoryEventPublisher publisher = new InMemoryEventPublisher();
    final DomainEventFixture.CapturingTransport transport = new DomainEventFixture.CapturingTransport();
    final ControllableTimer timer = new ControllableTimer(new InMemoryTimerService(clock));
    final InMemoryDomainEventBus bus = new InMemoryDomainEventBus();
    final WorkflowEngine engine;

    ListenFixture(WorkflowDefinition... defs) throws Exception {
        this(new InMemoryStateStore(), defs);
    }

    ListenFixture(StateStore state, WorkflowDefinition... defs) throws Exception {
        for (WorkflowDefinition def : defs) {
            registry.register(def);
        }
        this.state = state;
        this.engine = new WorkflowEngine(registry, transport, state, timer,
                new FilesystemBlobStore(Files.createTempDirectory("stint-sdd23-blobs")),
                new TreeInterpreter(new JqExpressionEvaluator()), clock, publisher, null);
        new DomainEventRouter(List.of(new ResumeReaction(engine))).subscribeTo(bus);
    }

    /** A filter on {@code type} correlated by {@code .data.instanceId == $workflow.id}. */
    static ListenNode.Filter byInstance(String type) {
        return new ListenNode.Filter(type, Map.of(), new TreeMap<>(Map.of("instance",
                new ListenNode.Correlation(Expr.jq(".data.instanceId"), Expr.jq("$workflow.id")))));
    }

    /** An event of {@code type} for {@code instanceId}, its data carrying {@code note}. */
    static CloudEvent event(String type, String eventId, String instanceId, String note) {
        ObjectNode data = Json.obj();
        data.put("instanceId", instanceId);
        data.put("note", note);
        return CloudEventBuilder.v1().withId(eventId).withSource(SOURCE).withType(type)
                .withDataContentType("application/json").withData(Json.bytes(data)).build();
    }

    void publish(CloudEvent event) throws Exception {
        bus.publish(event).toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    InstanceSnapshot load(String instanceId) throws Exception {
        return state.load(instanceId).toCompletableFuture().get(5, TimeUnit.SECONDS).orElseThrow();
    }

    JsonNode awaitCompleted(String instanceId) throws Exception {
        return engine.awaitCompletion(instanceId, Duration.ofSeconds(10)).toCompletableFuture().get(15, TimeUnit.SECONDS);
    }

    String timerKeyOf(String instanceId) throws Exception {
        return load(instanceId).listenState().get("active").get("timerKey").asText();
    }

    /** The key an instance with id {@code instanceId} waits on for {@code type} (filter {@link #byInstance}). */
    static String keyFor(String type, String instanceId) {
        return EventCorrelation.waitKey(type, EventCorrelation.shapeOf(byInstance(type)),
                new TreeMap<>(Map.of("instance", Json.MAPPER.getNodeFactory().textNode(instanceId))));
    }

    List<InboxEntry> inbox(String key) throws Exception {
        return state.findInbox(key).toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    boolean waits(String key) throws Exception {
        return state.findWait(key).toCompletableFuture().get(5, TimeUnit.SECONDS).isPresent();
    }

    /** An {@link InMemoryTimerService} whose fire can be awaited — the plain {@code tick()} doesn't wait. */
    static final class ControllableTimer implements TimerService {
        private final InMemoryTimerService delegate;
        private volatile TimerFireHandler handler;

        ControllableTimer(InMemoryTimerService delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletionStage<String> schedule(TimerRequest req) {
            return delegate.schedule(req);
        }

        @Override
        public CompletionStage<Void> cancel(String timerId) {
            return delegate.cancel(timerId);
        }

        @Override
        public void onFire(TimerFireHandler handler) {
            this.handler = handler;
            delegate.onFire(handler);
        }

        @Override
        public Duration maxDelay() {
            return delegate.maxDelay();
        }

        void tick() {
            delegate.tick();
        }

        /** Delivers the fire of {@code timerId} and waits until the engine has handled it. */
        void fireAndWait(String timerId, String instanceId) throws Exception {
            handler.handle(new TimerFire(timerId, instanceId)).toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
