package io.stintflow.example.ai;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpServer;

/**
 * An in-process stand-in for a local LitJev server ({@code POST /v1/systemone}) on 127.0.0.1, so the example runs the
 * Jev adapter's local profile without a model (SDD 3a.2b, 4.7). Like LitJev, it has no authentication — a request
 * with an {@code Authorization} header gets a 400 — and answers in its format: every yes/no question with {@code noul}
 * 0.2, the model as a checkpoint id, no output tokens.
 */
final class FakeJevEndpoint implements AutoCloseable {

    static final String MODEL = "synthetic/qwen-checkpoint";

    private final HttpServer server;

    FakeJevEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                boolean authenticated = exchange.getRequestHeaders().containsKey("Authorization");
                byte[] body = (authenticated ? "{\"detail\":\"no authentication here\"}" : """
                        {"model":"%s","answers":{"decision":{"type":"noul","noul":0.2}},
                         "usage":{"input_tokens":40,"output_tokens":0}}""".formatted(MODEL))
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(authenticated ? 400 : 200, body.length);
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
