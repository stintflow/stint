package io.stintflow.worker.publishing;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.stintflow.wire.Json;

/**
 * A local stand-in for Confluence (SDD 3a.6, 4.5), on loopback: the subset of the REST API that
 * {@link ConfluencePagePublisher} uses, over one or more spaces, with basic authentication against fixed, fake
 * credentials. For tests and the example's local run — never a real Confluence, never a real token. It records every
 * request it answered, so a test can prove none was made.
 */
public final class FakeConfluenceServer implements AutoCloseable {

    /** A page as the fake keeps it. */
    public record StoredPage(String id, String spaceKey, String title, String body, List<String> labels) {
    }

    private static final Pattern CQL = Pattern.compile("space=\"([^\"]+)\" and label=\"([^\"]+)\"");

    private final HttpServer server;
    private final String authorization;
    private final Map<String, String> spaceIds = new ConcurrentHashMap<>();
    private final Map<String, StoredPage> pages = new ConcurrentHashMap<>();
    private final List<String> requests = new CopyOnWriteArrayList<>();
    private final AtomicInteger ids = new AtomicInteger(10_000);

    public FakeConfluenceServer(String email, String apiToken, String... spaces) throws IOException {
        this.authorization = "Basic " + Base64.getEncoder().encodeToString((email + ":" + apiToken)
                .getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < spaces.length; i++) {
            spaceIds.put(spaces[i], String.valueOf(100 + i));
        }
        server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        server.createContext("/wiki", this::handle);
        server.start();
    }

    public URI baseUri() {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort());
    }

    public List<StoredPage> pages() {
        return List.copyOf(pages.values());
    }

    /** Every request answered, as {@code METHOD path}. */
    public List<String> requests() {
        return List.copyOf(requests);
    }

    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            URI uri = exchange.getRequestURI();
            requests.add(method + " " + uri.getPath());
            if (!authorization.equals(exchange.getRequestHeaders().getFirst("Authorization"))) {
                respond(exchange, 401, Json.obj().put("message", "unauthorized"));
                return;
            }
            String path = uri.getPath();
            String query = uri.getRawQuery() == null ? "" : URLDecoder.decode(uri.getRawQuery(), StandardCharsets.UTF_8);
            if ("GET".equals(method) && path.equals("/wiki/api/v2/spaces")) {
                ArrayNode results = Json.MAPPER.createArrayNode();
                String key = query.replaceFirst("^keys=", "");
                if (spaceIds.containsKey(key)) {
                    results.addObject().put("id", spaceIds.get(key)).put("key", key);
                }
                ObjectNode body = Json.obj();
                body.set("results", results);
                respond(exchange, 200, body);
            } else if ("GET".equals(method) && path.equals("/wiki/rest/api/content/search")) {
                Matcher cql = CQL.matcher(query);
                ArrayNode results = Json.MAPPER.createArrayNode();
                if (cql.find()) {
                    for (StoredPage page : pages.values()) {
                        if (page.spaceKey().equals(cql.group(1)) && page.labels().contains(cql.group(2))) {
                            results.add(json(page));
                        }
                    }
                }
                ObjectNode body = Json.obj();
                body.set("results", results);
                body.putObject("_links").put("base", baseUri() + "/wiki");
                respond(exchange, 200, body);
            } else if ("POST".equals(method) && path.equals("/wiki/api/v2/pages")) {
                JsonNode request = Json.read(exchange.getRequestBody().readAllBytes());
                String spaceKey = spaceIds.entrySet().stream()
                        .filter(e -> e.getValue().equals(request.path("spaceId").asText())).map(Map.Entry::getKey)
                        .findFirst().orElse(null);
                if (spaceKey == null) {
                    respond(exchange, 404, Json.obj().put("message", "space not found"));
                    return;
                }
                StoredPage page = new StoredPage(String.valueOf(ids.incrementAndGet()), spaceKey,
                        request.path("title").asText(), request.path("body").path("value").asText(),
                        new CopyOnWriteArrayList<>());
                pages.put(page.id(), page);
                ObjectNode body = json(page);
                body.with("_links").put("base", baseUri() + "/wiki");
                respond(exchange, 200, body);
            } else if ("POST".equals(method) && path.startsWith("/wiki/rest/api/content/") && path.endsWith("/label")) {
                String id = path.substring("/wiki/rest/api/content/".length(), path.length() - "/label".length());
                StoredPage page = pages.get(id);
                if (page == null) {
                    respond(exchange, 404, Json.obj().put("message", "page not found"));
                    return;
                }
                List<String> added = new ArrayList<>();
                Json.read(exchange.getRequestBody().readAllBytes()).forEach(l -> added.add(l.path("name").asText()));
                page.labels().addAll(added);
                respond(exchange, 200, Json.obj());
            } else {
                respond(exchange, 404, Json.obj().put("message", "not part of the fake"));
            }
        }
    }

    private static ObjectNode json(StoredPage page) {
        ObjectNode node = Json.obj();
        node.put("id", page.id());
        node.put("title", page.title());
        node.putObject("version").put("number", 1);
        node.putObject("_links").put("webui", "/spaces/" + page.spaceKey() + "/pages/" + page.id());
        return node;
    }

    private static void respond(HttpExchange exchange, int status, JsonNode body) throws IOException {
        byte[] bytes = Json.bytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
