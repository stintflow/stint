package io.stintflow.worker.publishing;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;

/**
 * A {@link PagePublisher} over the Confluence Cloud REST API (SDD 3a.6, 4.5), with the JDK's HttpClient and basic
 * authentication (an e-mail and an API token from the configuration). One space only: a page for another space is
 * refused before any call, as in {@link PublishHandler}. Idempotent by key: the page carries the label
 * {@link PagePublisher#keyLabel}, and a publish first looks for it.
 * <ol>
 *   <li>{@code GET /wiki/api/v2/spaces?keys=<space>}: the space's id;</li>
 *   <li>{@code GET /wiki/rest/api/content/search?cql=space="<space>" and label="<key label>"}: an earlier page;</li>
 *   <li>{@code POST /wiki/api/v2/pages}: the page, in storage format;</li>
 *   <li>{@code POST /wiki/rest/api/content/<id>/label}: its labels.</li>
 * </ol>
 * Tested against {@link FakeConfluenceServer} only; never against a real Confluence, never with a real token.
 */
public final class ConfluencePagePublisher implements PagePublisher {

    private final HttpClient http;
    private final URI baseUri;
    private final String space;
    private final String authorization;
    private final Duration timeout;

    private ConfluencePagePublisher(Builder b) {
        this.baseUri = Objects.requireNonNull(b.baseUri, "baseUri is required");
        this.space = Objects.requireNonNull(b.space, "space is required");
        this.authorization = "Basic " + Base64.getEncoder().encodeToString(
                (Objects.requireNonNull(b.email, "email is required") + ":"
                        + Objects.requireNonNull(b.apiToken, "apiToken is required")).getBytes(StandardCharsets.UTF_8));
        this.timeout = b.timeout;
        this.http = b.http != null ? b.http : HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public PublishedPage publish(Page page) {
        if (!space.equals(page.space())) {
            throw PublishFailures.spaceNotAllowed(page.space(), space);
        }
        String keyLabel = PagePublisher.keyLabel(page.key());
        JsonNode spaces = send("GET", "/wiki/api/v2/spaces?keys=" + encode(space), null, "the space lookup");
        String spaceId = spaces.path("results").path(0).path("id").asText(null);
        if (spaceId == null) {
            throw PublishFailures.http(404, null, "the space lookup");
        }
        JsonNode found = send("GET", "/wiki/rest/api/content/search?cql=" + encode("space=\"" + space
                + "\" and label=\"" + keyLabel + "\""), null, "the page search");
        JsonNode existing = found.path("results").path(0);
        if (!existing.isMissingNode()) {
            return new PublishedPage(existing.path("id").asText(), link(found, existing),
                    existing.path("version").path("number").asInt(1), false);
        }
        ObjectNode create = Json.obj();
        create.put("spaceId", spaceId);
        create.put("status", "current");
        create.put("title", page.title());
        create.putObject("body").put("representation", "storage").put("value", page.body());
        JsonNode created = send("POST", "/wiki/api/v2/pages", create, "the page creation");
        String id = created.path("id").asText();
        List<String> labels = new ArrayList<>(page.labels());
        labels.add(keyLabel);
        labels.add(PagePublisher.levelLabel(page.sensitivity()));
        ArrayNode labelBody = Json.MAPPER.createArrayNode();
        labels.forEach(l -> labelBody.addObject().put("prefix", "global").put("name", l));
        send("POST", "/wiki/rest/api/content/" + encode(id) + "/label", labelBody, "the labels");
        return new PublishedPage(id, link(created, created), created.path("version").path("number").asInt(1), true);
    }

    private URI link(JsonNode response, JsonNode page) {
        String base = response.path("_links").path("base").asText(baseUri.resolve("/wiki").toString());
        String webui = page.path("_links").path("webui").asText("/pages/" + page.path("id").asText());
        return URI.create(base + webui);
    }

    private JsonNode send(String method, String path, JsonNode body, String what) {
        HttpRequest.Builder request = HttpRequest.newBuilder(baseUri.resolve(path)).timeout(timeout)
                .header("Authorization", authorization).header("Accept", "application/json");
        if (body != null) {
            request.header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(Json.bytes(body)));
        } else {
            request.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<byte[]> response;
        try {
            response = http.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (HttpTimeoutException e) {
            throw PublishFailures.timeout("The publishing server did not answer " + what + " within "
                    + timeout.toSeconds() + " s");
        } catch (ConnectException e) {
            throw PublishFailures.unavailable(503, "The publishing server refused the connection", null);
        } catch (IOException e) {
            throw PublishFailures.unavailable(503, "The publishing server could not be reached ("
                    + e.getClass().getSimpleName() + ")", null);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw PublishFailures.unavailable(503, "The publication was interrupted", null);
        }
        if (response.statusCode() / 100 != 2) {
            Duration retryAfter = response.headers().firstValue("Retry-After").map(v -> {
                try {
                    return Duration.ofSeconds(Long.parseLong(v.trim()));
                } catch (NumberFormatException e) {
                    return null;
                }
            }).orElse(null);
            throw PublishFailures.http(response.statusCode(), retryAfter, what);
        }
        try {
            return response.body().length == 0 ? Json.obj() : Json.read(response.body());
        } catch (RuntimeException e) {
            throw PublishFailures.validation("The publishing server answered " + what + " with something that is not "
                    + "JSON");
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    public static final class Builder {
        private URI baseUri;
        private String space;
        private String email;
        private String apiToken;
        private Duration timeout = Duration.ofSeconds(30);
        private HttpClient http;

        private Builder() {
        }

        /** The site, e.g. {@code https://example.atlassian.net}; the API paths start at {@code /wiki}. */
        public Builder baseUri(URI baseUri) {
            this.baseUri = baseUri;
            return this;
        }

        /** The one space this publisher may write to. */
        public Builder space(String space) {
            this.space = space;
            return this;
        }

        public Builder email(String email) {
            this.email = email;
            return this;
        }

        /** The API token, from the configuration; never in code or in a test. */
        public Builder apiToken(String apiToken) {
            this.apiToken = apiToken;
            return this;
        }

        public Builder timeout(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout, "timeout");
            return this;
        }

        public Builder httpClient(HttpClient http) {
            this.http = http;
            return this;
        }

        public ConfluencePagePublisher build() {
            return new ConfluencePagePublisher(this);
        }
    }
}
