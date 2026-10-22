package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.inmemory.InMemoryEventPublisher;
import io.stintflow.spi.AdapterCapabilities;
import io.stintflow.spi.EventPublisher;
import io.stintflow.spi.wire.StintEvents;
import io.stintflow.wire.Json;
import io.stintflow.wire.review.ReviewContract;

/**
 * SDD 3a.4, 4.8 (copied for SDD 3a.6): the example review channel — the minimal stand-in for a portal. It is the engine's
 * {@link EventPublisher}, so it sees the review requests (ids, role, options, items and the material's pointer, never
 * the material), and serves them over HTTP on the loopback interface:
 * <ul>
 *   <li>{@code GET /reviews}: the open requests;</li>
 *   <li>{@code POST /reviews/{reviewId}}, header {@code X-Stint-Role}, body the answer of the review contract
 *       ({@code value} or {@code verdict}/{@code items}, {@code noteRef}, {@code reviewer}): publishes
 *       {@code io.stintflow.review.completed.v1} with this channel's {@code source}.</li>
 * </ul>
 * <b>No authentication:</b> a real channel authenticates the person, checks the role and pseudonymizes the reviewer
 * before publishing; the Stint trusts the {@code source} its policy lists. No UI.
 */
public final class ReviewChannel implements EventPublisher, AutoCloseable {

    public static final String ROLE_HEADER = "X-Stint-Role";

    private final InMemoryEventPublisher delegate = new InMemoryEventPublisher();
    private final Map<String, JsonNode> open = new ConcurrentHashMap<>();
    private final URI source;
    private final HttpServer server;
    private volatile WorkflowEngine engine;

    public ReviewChannel(URI source) throws IOException {
        this.source = source;
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/reviews", this::handle);
        server.start();
    }

    /** The engine the answers go to. */
    public ReviewChannel answersTo(WorkflowEngine engine) {
        this.engine = engine;
        return this;
    }

    public URI baseUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Override
    public CompletionStage<Void> publish(CloudEvent event) {
        if (StintEvents.TYPE_REVIEW_REQUESTED.equals(event.getType()) && event.getData() != null) {
            open.put(event.getSubject(), Json.read(event.getData().toBytes()));
        }
        return delegate.publish(event);
    }

    /** The open requests of one instance (a review id starts with its instance's id). */
    public java.util.List<JsonNode> openFor(String instanceId) {
        return open.entrySet().stream().filter(e -> e.getKey().startsWith(instanceId + ":")).map(Map.Entry::getValue)
                .toList();
    }

    /** Every open request, of any instance (SDD 3c.4: the children of a {@code for} each have their own). */
    public java.util.List<JsonNode> allOpen() {
        return java.util.List.copyOf(open.values());
    }

    /** Every event the engine published through this channel, in order. */
    public java.util.List<CloudEvent> published() {
        return delegate.published();
    }

    @Override
    public AdapterCapabilities capabilities() {
        return delegate.capabilities();
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String path = exchange.getRequestURI().getPath();
            if ("GET".equals(exchange.getRequestMethod()) && path.equals("/reviews")) {
                ArrayNode list = Json.MAPPER.createArrayNode();
                open.values().forEach(list::add);
                respond(exchange, 200, Json.bytes(list));
                return;
            }
            if ("POST".equals(exchange.getRequestMethod()) && path.startsWith("/reviews/")) {
                String reviewId = path.substring("/reviews/".length());
                String role = exchange.getRequestHeaders().getFirst(ROLE_HEADER);
                JsonNode answer;
                try (InputStream body = exchange.getRequestBody()) {
                    answer = Json.read(body.readAllBytes());
                }
                if (!open.containsKey(reviewId) || role == null || !ReviewContract.isContentFree(answer)) {
                    respond(exchange, 400, "{\"error\":\"unknown review, no role, or not the review contract\"}"
                            .getBytes(StandardCharsets.UTF_8));
                    return;
                }
                CloudEvent completed = CloudEventBuilder.v1().withId(UUID.randomUUID().toString())
                        .withSource(source).withType(StintEvents.TYPE_REVIEW_COMPLETED).withSubject(reviewId)
                        .withDataContentType(StintEvents.CONTENT_TYPE_JSON).withData(Json.bytes(answer))
                        .withExtension(StintEvents.EXT_ROLE, role).build();
                engine.onDomainEvent(completed, Duration.ofMinutes(5)).toCompletableFuture().get(10, TimeUnit.SECONDS);
                open.remove(reviewId);
                respond(exchange, 202, "{}".getBytes(StandardCharsets.UTF_8));
                return;
            }
            respond(exchange, 404, "{}".getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private static void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
