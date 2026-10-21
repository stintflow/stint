package io.stintflow.knowledge.inspector;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.stintflow.knowledge.ChangeEntry;
import io.stintflow.knowledge.GraphView;
import io.stintflow.knowledge.KnowledgeInspector;
import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.ViewQuery;

/**
 * SDD 3b.6, 4.4 (CA3, CA7, CA8, CA11 over HTTP): the API only reads, asks the access port first and answers a base that does not
 * exist like a base that is not allowed; its errors say nothing of the cause; the page is served with a policy that lets nothing
 * foreign in; the export is well-formed XML. Synthetic data only.
 */
class InspectorServerTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @TempDir
    Path dir;

    private InspectorServer server;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void start() throws Exception {
        BaseFixture base = BaseFixture.withHistory(dir);
        InspectorService service = new InspectorService(base.store,
                InspectorAccess.allowing(Map.of("op-1", Set.of(BaseFixture.SCOPE))), InspectorService.Settings.DEFAULT);
        server = new InspectorServer(service, List.of(BaseFixture.SCOPE), InspectorServer.operatorHeader(), 0).start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    private HttpResponse<String> get(String path, String operator) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + path)).GET();
        if (operator != null) {
            request.header(InspectorServer.OPERATOR_HEADER, operator);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static final String BASE = "/graph/meetings/notes/";

    @Test
    void it_serves_the_page_with_a_policy_that_lets_nothing_foreign_in() throws Exception {
        HttpResponse<String> page = get("/", null);

        assertThat(page.statusCode()).isEqualTo(200);
        assertThat(page.body()).contains("Knowledge inspector");
        assertThat(page.headers().firstValue("Content-Security-Policy").orElseThrow()).startsWith("default-src 'self'");
        assertThat(page.headers().firstValue("X-Content-Type-Options")).hasValue("nosniff");
        assertThat(page.headers().firstValue("Cache-Control")).hasValue("no-store");
        assertThat(get("/app.js", null).body()).contains("textContent");
        assertThat(get("/app.css", null).statusCode()).isEqualTo(200);
        // nothing is loaded from another origin: the page and its style name none, and the script only fetches paths of its own
        assertThat(page.body() + get("/app.css", null).body()).doesNotContain("http://").doesNotContain("https://");
        assertThat(get("/app.js", null).body()).doesNotContain("fetch('http").doesNotContain("fetch(\"http").doesNotContain("innerHTML");
    }

    @Test
    void ca7_without_permission_it_is_403_for_a_base_that_exists_and_for_one_that_does_not_and_the_body_is_the_same()
            throws Exception {
        HttpResponse<String> nobody = get(BASE + "view", null);
        HttpResponse<String> stranger = get(BASE + "view", "op-2");
        HttpResponse<String> missing = get("/graph/nowhere/none/view", "op-1");

        assertThat(nobody.statusCode()).isEqualTo(403);
        assertThat(stranger.statusCode()).isEqualTo(403);
        assertThat(missing.statusCode()).isEqualTo(403);
        assertThat(nobody.body()).isEqualTo(stranger.body()).isEqualTo(missing.body()).isEqualTo("{\"error\":\"not allowed\"}");
        for (String route : List.of("snapshot", "changes", "metrics", "duplicates", "export.graphml")) {
            assertThat(get(BASE + route, "op-2").statusCode()).as(route).isEqualTo(403);
        }
        assertThat(JSON.readTree(get("/graph/bases", "op-2").body())).isEmpty();
        assertThat(JSON.readTree(get("/graph/bases", "op-1").body())).hasSize(1);
    }

    @Test
    void it_only_reads_and_a_malformed_request_or_an_unknown_route_is_refused_without_detail() throws Exception {
        HttpRequest post = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + BASE + "view"))
                .header(InspectorServer.OPERATOR_HEADER, "op-1").POST(HttpRequest.BodyPublishers.ofString("{}")).build();
        HttpRequest delete = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + BASE + "view"))
                .header(InspectorServer.OPERATOR_HEADER, "op-1").DELETE().build();

        assertThat(http.send(post, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(405);
        assertThat(http.send(delete, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(405);
        assertThat(get(BASE + "snapshot?seq=abc", "op-1").statusCode()).isEqualTo(400);
        assertThat(get(BASE + "snapshot?at=yesterday", "op-1").body()).isEqualTo("{\"error\":\"bad request\"}");
        assertThat(get(BASE + "nothing", "op-1").statusCode()).isEqualTo(404);
        assertThat(get("/elsewhere", "op-1").statusCode()).isEqualTo(404);
    }

    @Test
    void it_answers_the_view_the_snapshot_the_timeline_the_metrics_and_the_duplicates() throws Exception {
        JsonNode view = JSON.readTree(get(BASE + "view", "op-1").body());
        assertThat(view.path("aggregated").asBoolean()).isFalse();
        assertThat(view.path("nodes")).hasSize(3);

        JsonNode snapshot = JSON.readTree(get(BASE + "snapshot?at=2026-10-01T23:00:00Z", "op-1").body());
        assertThat(snapshot.path("nodes")).hasSize(3);
        assertThat(snapshot.path("nodes").toString()).doesNotContain(BaseFixture.PERSON_NAME);

        JsonNode changes = JSON.readTree(get(BASE + "changes?chain=chain-B&limit=100", "op-1").body());
        assertThat(changes).isNotEmpty();
        changes.forEach(c -> assertThat(c.path("chainId").asText()).isEqualTo("chain-B"));
        assertThat(JSON.readTree(get(BASE + "changes?type=Person", "op-1").body()).findValuesAsText("elementType"))
                .containsOnly("Person");

        assertThat(JSON.readTree(get(BASE + "metrics", "op-1").body()).path("nodes").asInt()).isEqualTo(3);
        assertThat(JSON.readTree(get(BASE + "duplicates", "op-1").body())).isEmpty();
    }

    @Test
    void ca11_the_export_is_well_formed_graphml_with_the_dates_and_the_mask() throws Exception {
        HttpResponse<String> export = get(BASE + "export.graphml", "op-1");

        assertThat(export.statusCode()).isEqualTo(200);
        assertThat(export.headers().firstValue("Content-Type").orElseThrow()).startsWith("application/graphml+xml");
        assertThat(export.body()).doesNotContain(BaseFixture.PERSON_NAME);
        var factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        var doc = factory.newDocumentBuilder().parse(new ByteArrayInputStream(export.body().getBytes(StandardCharsets.UTF_8)));
        assertThat(doc.getElementsByTagName("node").getLength()).isEqualTo(4);   // the three alive and the person who left
        assertThat(doc.getElementsByTagName("edge").getLength()).isEqualTo(3);   // the two that stay and the erased one
        assertThat(export.body()).contains("<data key=\"n_removed\">").contains("<data key=\"n_added\">2026-10-01T10:00:00Z</data>")
                .contains("<data key=\"n_masked\">true</data>");
    }

    @Test
    void a_failure_underneath_is_a_500_that_says_nothing_of_the_cause() throws Exception {
        KnowledgeInspector failing = new KnowledgeInspector() {
            @Override
            public CompletionStage<GraphView> view(KnowledgeScope scope, ViewQuery query) {
                return CompletableFuture.failedFuture(new IllegalStateException("secret-detail SYNTH-PII-0999"));
            }

            @Override
            public CompletionStage<List<ChangeEntry>> changes(KnowledgeScope scope, long afterSequence, int limit) {
                return CompletableFuture.failedFuture(new IllegalStateException("secret-detail SYNTH-PII-0999"));
            }
        };
        try (InspectorServer broken = new InspectorServer(new InspectorService(failing,
                InspectorAccess.allowing(Map.of("op-1", Set.of(BaseFixture.SCOPE))), InspectorService.Settings.DEFAULT),
                List.of(BaseFixture.SCOPE), InspectorServer.operatorHeader(), 0).start()) {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + broken.port() + BASE + "view"))
                    .header(InspectorServer.OPERATOR_HEADER, "op-1").GET().build();

            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(500);
            assertThat(response.body()).isEqualTo("{\"error\":\"internal error\"}").doesNotContain("secret-detail");
        }
    }
}
