package io.stintflow.worker.decision.jev;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;

import io.stintflow.wire.Json;

/**
 * A stand-in for {@code POST /v1/systemone} on 127.0.0.1 — never the real service, no real key. Each call takes the
 * next scripted reply (status, headers, body); it records every request body and its {@code Authorization} header.
 * A reply can be held at a latch, to make the client time out without a sleep.
 */
public final class FakeJevServer implements AutoCloseable {

    /** One scripted reply. */
    public record Reply(int status, Map<String, String> headers, String body, CountDownLatch hold) {

        public static Reply ok(String body) {
            return new Reply(200, Map.of(), body, null);
        }

        public static Reply status(int status, Map<String, String> headers) {
            return new Reply(status, headers, "{\"error\":\"synthetic\"}", null);
        }
    }

    public final List<JsonNode> requests = new CopyOnWriteArrayList<>();
    public final List<String> authorizations = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Reply> replies = new LinkedBlockingQueue<>();
    private final HttpServer server;

    public FakeJevServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> {
            try (exchange) {
                requests.add(Json.read(exchange.getRequestBody().readAllBytes()));
                authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
                Reply reply = replies.poll();
                if (reply == null) {
                    reply = Reply.status(500, Map.of());
                }
                if (reply.hold() != null) {
                    reply.hold().await(10, TimeUnit.SECONDS);
                }
                reply.headers().forEach((name, value) -> exchange.getResponseHeaders().add(name, value));
                byte[] body = reply.body().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(reply.status(), body.length);
                exchange.getResponseBody().write(body);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        server.start();
    }

    public FakeJevServer reply(Reply reply) {
        replies.add(reply);
        return this;
    }

    public URI baseUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
