package io.stintflow.worker.decision.jev;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;

import io.stintflow.wire.Json;

/**
 * A stand-in for {@code POST /v1/systemone} on 127.0.0.1 — never the real service, no real key. Each call takes the
 * next scripted reply (status, headers, body); it records every request body and its {@code Authorization} header
 * ({@code null} when the request had none). Replies in the Jev format or, with {@link Reply#litjev}, in LitJev's.
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

        /**
         * A {@code choice} or {@code score} answer as LitJev gives it (SDD 3a.2b): {@code confidence} is the normalized
         * Gini concentration of the probabilities, {@code model} the loaded checkpoint, and no output tokens.
         */
        public static Reply litjev(String type, Object value, Map<String, Double> probabilities) {
            ObjectNode answer = Json.obj().put("type", type);
            answer.set(type, Json.MAPPER.valueToTree(value));
            answer.set("probabilities", Json.MAPPER.valueToTree(probabilities));
            answer.put("confidence", gini(probabilities.values()));
            return litjev(answer);
        }

        /** A {@code noul} answer as LitJev gives it: P(yes), no confidence. */
        public static Reply litjevNoul(double p) {
            return litjev(Json.obj().put("type", "noul").put("noul", p));
        }

        private static Reply litjev(ObjectNode answer) {
            ObjectNode body = Json.obj().put("model", LITJEV_MODEL);
            body.putObject("answers").set("decision", answer);
            body.putObject("usage").put("input_tokens", 96).put("output_tokens", 0);
            return ok(body.toString());
        }
    }

    /** A synthetic checkpoint id, as a LitJev server reports the model it loaded. */
    public static final String LITJEV_MODEL = "synthetic/qwen-checkpoint";

    /** LitJev's confidence: {@code (K·Σp² − 1)/(K − 1)}, and 1 for a single option. */
    public static double gini(Collection<Double> probabilities) {
        int k = probabilities.size();
        if (k <= 1) {
            return 1;
        }
        double sumOfSquares = probabilities.stream().mapToDouble(p -> p * p).sum();
        return (k * sumOfSquares - 1) / (k - 1);
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
