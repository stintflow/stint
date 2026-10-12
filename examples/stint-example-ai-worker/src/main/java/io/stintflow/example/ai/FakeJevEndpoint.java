package io.stintflow.example.ai;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.sun.net.httpserver.HttpServer;

/**
 * An in-process stand-in for TypeSafe's Jev API ({@code POST /v1/systemone}) on 127.0.0.1, so the example runs the Jev
 * adapter without an account or a key (SDD 3a.2, 8d). It answers every yes/no question with {@code noul} 0.2.
 */
final class FakeJevEndpoint implements AutoCloseable {

    private final HttpServer server;

    FakeJevEndpoint() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/systemone", exchange -> {
            try (exchange) {
                exchange.getRequestBody().readAllBytes();
                byte[] body = """
                        {"model":"jev-fake","answers":{"decision":{"type":"noul","noul":0.2}},
                         "usage":{"input_tokens":40,"output_tokens":1}}""".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, body.length);
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
