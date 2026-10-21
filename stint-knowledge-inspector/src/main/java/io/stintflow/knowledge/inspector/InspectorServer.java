package io.stintflow.knowledge.inspector;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import io.stintflow.knowledge.KnowledgeScope;

/**
 * SDD 3b.6, 4.4: the API and the page, on the JDK's own HTTP server — no framework. Every route reads and none writes (anything but
 * {@code GET} is 405). For a base, the caller is asked of {@link InspectorAccess} before anything else, and the answer to "not
 * allowed" and to "no such base" is the same 403. Errors say nothing of the cause. Authentication is the host's: the caller is read
 * by a function the host gives (by default the header {@code X-Stint-Operator}, which only means something behind a gateway that sets
 * it).
 * <p>
 * Routes: {@code /} (the page), {@code /graph/bases}, and for a base {@code /graph/{namespace}/{base}/} {@code view},
 * {@code snapshot?seq=|at=}, {@code changes?after=&limit=&chain=&instance=&type=&document=}, {@code metrics}, {@code duplicates},
 * {@code export.graphml}.
 */
public final class InspectorServer implements AutoCloseable {

    public static final String OPERATOR_HEADER = "X-Stint-Operator";

    private static final Logger LOG = LoggerFactory.getLogger(InspectorServer.class);
    private static final ObjectMapper JSON = new ObjectMapper();

    private final InspectorService service;
    private final List<KnowledgeScope> offered;
    private final Function<HttpExchange, String> callerOf;
    private final HttpServer http;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    /** @param offered the bases this server knows (the page lists those the caller may look at) */
    public InspectorServer(InspectorService service, List<KnowledgeScope> offered, Function<HttpExchange, String> callerOf,
            int port) throws IOException {
        this.service = Objects.requireNonNull(service, "service");
        this.offered = List.copyOf(offered);
        this.callerOf = Objects.requireNonNull(callerOf, "callerOf");
        this.http = HttpServer.create(new InetSocketAddress(java.net.InetAddress.getLoopbackAddress(), port), 0);
        http.createContext("/", this::handle);
        http.setExecutor(executor);
    }

    public static Function<HttpExchange, String> operatorHeader() {
        return exchange -> exchange.getRequestHeaders().getFirst(OPERATOR_HEADER);
    }

    public InspectorServer start() {
        http.start();
        return this;
    }

    public int port() {
        return http.getAddress().getPort();
    }

    @Override
    public void close() {
        http.stop(0);
        executor.shutdownNow();
    }

    // --- routing ----------------------------------------------------------------------------------------------------

    private void handle(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                send(exchange, 405, "application/json", "{\"error\":\"method not allowed\"}");
                return;
            }
            String path = exchange.getRequestURI().getPath();
            if (path.equals("/") || path.equals("/index.html")) {
                sendResource(exchange, "index.html", "text/html; charset=utf-8");
            } else if (path.equals("/app.js")) {
                sendResource(exchange, "app.js", "text/javascript; charset=utf-8");
            } else if (path.equals("/app.css")) {
                sendResource(exchange, "app.css", "text/css; charset=utf-8");
            } else if (path.equals("/graph/bases")) {
                String caller = callerOf.apply(exchange);
                send(exchange, 200, "application/json", JSON.writeValueAsString(offered.stream()
                        .filter(scope -> service.mayInspect(caller, scope))
                        .map(scope -> Map.of("namespace", scope.namespace(), "base", scope.base())).toList()));
            } else if (path.startsWith("/graph/")) {
                graph(exchange, path.substring("/graph/".length()));
            } else {
                send(exchange, 404, "application/json", "{\"error\":\"not found\"}");
            }
        } catch (InspectorAccessDeniedException e) {
            send(exchange, 403, "application/json", "{\"error\":\"not allowed\"}");
        } catch (BadRequest e) {
            send(exchange, 400, "application/json", "{\"error\":\"bad request\"}");
        } catch (RuntimeException e) {
            LOG.warn("The inspector failed ({})", e.getClass().getName());   // the class, never the message
            send(exchange, 500, "application/json", "{\"error\":\"internal error\"}");
        } finally {
            exchange.close();
        }
    }

    private void graph(HttpExchange exchange, String rest) throws IOException {
        String[] parts = rest.split("/", 3);
        if (parts.length < 3 || parts[0].isBlank() || parts[1].isBlank()) {
            throw new BadRequest();
        }
        KnowledgeScope scope = KnowledgeScope.named(decode(parts[0]), decode(parts[1]));
        String caller = callerOf.apply(exchange);
        if (!service.mayInspect(caller, scope)) {
            throw new InspectorAccessDeniedException();   // the same for a base that does not exist
        }
        Map<String, String> query = query(exchange.getRequestURI().getRawQuery());
        switch (parts[2]) {
            case "view" -> json(exchange, service.current(caller, scope));
            case "snapshot" -> json(exchange, service.snapshot(caller, scope, longOrNull(query.get("seq")), instantOrNull(query.get("at"))));
            case "changes" -> json(exchange, service.changes(caller, scope, longOr(query.get("after"), 0),
                    (int) longOr(query.get("limit"), 500), new InspectorService.ChangeFilter(query.get("chain"),
                            query.get("instance"), query.get("type"), query.get("document"))));
            case "metrics" -> json(exchange, service.metrics(caller, scope));
            case "duplicates" -> json(exchange, service.duplicates(caller, scope));
            case "export.graphml" -> {
                Headers headers = exchange.getResponseHeaders();
                headers.set("Content-Disposition", "attachment; filename=\"graph.graphml\"");
                send(exchange, 200, "application/graphml+xml; charset=utf-8", service.graphMl(caller, scope));
            }
            default -> send(exchange, 404, "application/json", "{\"error\":\"not found\"}");
        }
    }

    // --- helpers ----------------------------------------------------------------------------------------------------

    private static final class BadRequest extends RuntimeException {
        BadRequest() {
            super(null, null, false, false);
        }
    }

    private static String decode(String s) {
        return URLDecoder.decode(s, StandardCharsets.UTF_8);
    }

    private static Map<String, String> query(String raw) {
        Map<String, String> out = new HashMap<>();
        if (raw != null) {
            for (String pair : raw.split("&")) {
                int eq = pair.indexOf('=');
                if (eq > 0) {
                    out.put(decode(pair.substring(0, eq)), decode(pair.substring(eq + 1)));
                }
            }
        }
        return out;
    }

    private static Long longOrNull(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new BadRequest();
        }
    }

    private static long longOr(String s, long fallback) {
        Long value = longOrNull(s);
        return value == null ? fallback : value;
    }

    private static Instant instantOrNull(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Instant.parse(s);
        } catch (java.time.format.DateTimeParseException e) {
            throw new BadRequest();
        }
    }

    private void json(HttpExchange exchange, Object body) throws IOException {
        send(exchange, 200, "application/json", JSON.writeValueAsString(body));
    }

    private void sendResource(HttpExchange exchange, String name, String type) throws IOException {
        try (InputStream in = InspectorServer.class.getResourceAsStream("/stint/inspector/" + name)) {
            if (in == null) {
                send(exchange, 404, "application/json", "{\"error\":\"not found\"}");
                return;
            }
            send(exchange, 200, type, new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static void send(HttpExchange exchange, int status, String type, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        Headers headers = exchange.getResponseHeaders();
        headers.set("Content-Type", type);
        headers.set("Cache-Control", "no-store");
        headers.set("X-Content-Type-Options", "nosniff");
        headers.set("Content-Security-Policy", "default-src 'self'; frame-ancestors 'none'");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
