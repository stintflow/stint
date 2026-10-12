package io.stintflow.example.ai;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;

import io.stintflow.wire.Json;

/**
 * An in-process stand-in for a local LitJev server ({@code POST /v1/systemone}) on 127.0.0.1, so the example runs the
 * Jev adapter's local profile without a model (SDD 3a.2b, 4.7). Like LitJev, it has no authentication — a request
 * with an {@code Authorization} header gets a 400 — and, like LitJev 2cd915a, refuses with 422 a {@code noul} without
 * both {@code criteria} descriptions. It answers in LitJev's format: every yes/no question with {@code noul} 0.2, the
 * model as a checkpoint id, no tokens counted.
 */
final class FakeJevEndpoint implements AutoCloseable {

    static final String MODEL = "synthetic/qwen-checkpoint";

    private final HttpServer server;

    FakeJevEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> {
            try (exchange) {
                JsonNode request = Json.read(exchange.getRequestBody().readAllBytes());
                boolean authenticated = exchange.getRequestHeaders().containsKey("Authorization");
                JsonNode criteria = request.path("questions").path("decision").path("criteria");
                boolean described = criteria.path("true").isTextual() && criteria.path("false").isTextual();
                int status = authenticated ? 400 : described ? 200 : 422;
                byte[] body = (switch (status) {
                    case 400 -> "{\"detail\":\"no authentication here\"}";
                    case 422 -> "{\"detail\":\"sequence item 0: expected str instance, NoneType found\"}";
                    default -> """
                            {"model":"%s","answers":{"decision":{"type":"noul","noul":0.2}},
                             "usage":{"input_tokens":0,"output_tokens":0}}""".formatted(MODEL);
                }).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(status, body.length);
                exchange.getResponseBody().write(body);
            }
        });
        server.start();
    }

    URI baseUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
