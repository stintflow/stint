package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.cloudevents.CloudEvent;
import io.stintflow.core.WorkflowDefinition;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.dsl.DslLoader;
import io.stintflow.dsl.LoadOptions;
import io.stintflow.example.meeting.Corpus.ArchitectureMeeting;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.knowledge.GraphPointer;
import io.stintflow.knowledge.GraphPointers;
import io.stintflow.knowledge.NodeView;
import io.stintflow.knowledge.ViewQuery;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.StateStore;
import io.stintflow.wire.Json;

/**
 * The scenario of the milestone, end to end (SDD 3c.6): the product meetings that take decisions run {@code product-meeting},
 * their published minutes are ingested, announced and linked; the architecture meetings are drafted into decisions that answer
 * them, and each draft runs {@code architecture-adr} — the pointers resolved by the product area, the conflict judged, a person
 * on every conflict and every pair the machine cannot settle, the ADR published. The engine, the store and the transport are the
 * caller's (the process, or floci); a scripted reviewer answers by the answer key.
 */
public final class ScenarioRun {

    private static final String INDEXED = "io.stintflow.knowledge.document-indexed.v1";
    private static final String NODES = "io.stintflow.knowledge.nodes-published.v1";

    /** The definitions the scenario starts or calls, by file name under {@code stint/workflows}. */
    public static final List<String> DEFINITIONS = List.of("product-meeting.yaml", "knowledge-ingest.yaml", "knowledge-announce.yaml",
            "architecture-links.yaml", "architecture-adr.yaml", "architecture-conflict-check.yaml");

    /** Where the scenario runs. */
    public record Env(WorkflowEngine engine, StateStore state, ReviewChannel channel, BlobStore blobs, RunMetrics metrics,
            List<WorkflowDefinition> definitions, ScenarioWorkers workers, Duration patience) {
    }

    /** One architecture meeting's run: what the key says, what the system did. */
    public record PairRun(String meetingId, String draftId, String instanceId, InstanceStatus status, String expectedVerdict,
            boolean expectedReviewed, String verdict, boolean reviewed, String auto, String resolution, String pageStatus,
            boolean originDown, int reviewsAnswered, Duration total, Map<String, Duration> steps) {
    }

    /** What the scenario did. */
    public record Result(List<ProductMeetingPipeline.MeetingRun> product, List<PairRun> architecture, List<String> announced,
            Set<String> pointersHeld) {
    }

    private ScenarioRun() {
    }

    /** The scenario's definitions, loaded strictly from the classpath. */
    public static List<WorkflowDefinition> definitions() throws IOException {
        List<WorkflowDefinition> all = new ArrayList<>();
        for (String name : DEFINITIONS) {
            try (InputStream yaml = ScenarioRun.class.getClassLoader().getResourceAsStream("stint/workflows/" + name)) {
                all.add(new DslLoader().load(yaml, name, LoadOptions.STRICT));
            }
        }
        return List.copyOf(all);
    }

    public static Result execute(Env env, List<Meeting> corpus) throws Exception {
        HttpClient http = HttpClient.newHttpClient();
        Set<String> decided = new LinkedHashSet<>();
        Corpus.loadProductDecisions().forEach(d -> decided.add(d.meetingId()));
        List<ProductMeetingPipeline.MeetingRun> product = new ArrayList<>();
        List<String> announced = new ArrayList<>();
        for (Meeting meeting : corpus.stream().filter(m -> decided.contains(m.id())).toList()) {
            product.add(productMeeting(env, http, meeting));
            announced.addAll(publishToTheBase(env, meeting));
        }

        Set<String> held = new LinkedHashSet<>(announced);
        List<PairRun> architecture = new ArrayList<>();
        for (ArchitectureMeeting meeting : Corpus.loadArchitecture()) {
            architecture.add(architectureMeeting(env, http, meeting, held));
        }
        return new Result(List.copyOf(product), List.copyOf(architecture), List.copyOf(announced), Set.copyOf(held));
    }

    /** Where the run does not follow the answer key of the corpus; empty when it follows it. */
    public static List<String> disagreements(Result result) {
        List<String> found = new ArrayList<>();
        result.product().forEach(run -> {
            if (run.status() != InstanceStatus.COMPLETED) {
                found.add("product " + run.meeting().id() + " ended " + run.status());
            }
            if (!run.meeting().expected().docType().equals(run.context().path("docType").asText())) {
                found.add("product " + run.meeting().id() + " document type " + run.context().path("docType").asText());
            }
        });
        result.architecture().forEach(run -> {
            if (run.status() != InstanceStatus.COMPLETED) {
                found.add("architecture " + run.meetingId() + " ended " + run.status());
            }
            if (!run.expectedVerdict().equals(run.verdict())) {
                found.add("architecture " + run.meetingId() + " verdict " + run.verdict() + ", the key says " + run.expectedVerdict());
            }
            if (run.expectedReviewed() != run.reviewed()) {
                found.add("architecture " + run.meetingId() + " reviewed " + run.reviewed() + ", the key says " + run.expectedReviewed());
            }
            if (run.originDown() != "unavailable".equals(run.resolution())) {
                found.add("architecture " + run.meetingId() + " resolution " + run.resolution());
            }
        });
        return found;
    }

    // --- the product area ---------------------------------------------------------------------------------------

    private static ProductMeetingPipeline.MeetingRun productMeeting(Env env, HttpClient http, Meeting meeting) throws Exception {
        WorkflowDefinition definition = definition(env, "product-meeting");
        URI audioRef = env.blobs().put(ProductMeetingPipeline.syntheticAudio(meeting), "audio/" + meeting.id() + ".wav",
                ProductMeetingPipeline.RAW_AUDIO).toCompletableFuture().get(10, TimeUnit.SECONDS);
        Instant started = Instant.now();
        String id = env.engine().startFromEvent(definition.ref(), ProductMeetingPipeline.meetingRecorded(meeting, audioRef, 0))
                .toCompletableFuture().get(30, TimeUnit.SECONDS);
        List<String> answered = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        long until = System.nanoTime() + env.patience().toNanos();
        InstanceSnapshot snapshot = load(env, id);
        while (!ended(snapshot) && System.nanoTime() < until) {
            for (JsonNode request : env.channel().openFor(id)) {
                if (seen.add(request.path("reviewId").asText())) {
                    ProductMeetingPipeline.answerAsTheKey(http, env.channel(), meeting, request);
                    answered.add(request.path("kind").asText());
                }
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            snapshot = load(env, id);
        }
        JsonNode decision = null;
        if (snapshot.decisions() != null) {
            for (var entry : snapshot.decisions().properties()) {
                if (entry.getKey().endsWith("/docType")) {
                    decision = entry.getValue();
                }
            }
        }
        return new ProductMeetingPipeline.MeetingRun(meeting, id, snapshot.status(), snapshot.context(), decision,
                Duration.between(started, Instant.now()), env.metrics().stepLatencies(id), env.metrics().chainIds(id), answered);
    }

    /** Ingests the published minutes, announces what the document added and links the architecture area to it; the pointers announced. */
    private static List<String> publishToTheBase(Env env, Meeting meeting) throws Exception {
        CloudEvent minutes = env.channel().published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                .filter(e -> meeting.id().equals(e.getSubject())).findFirst().orElseThrow();
        int indexedBefore = count(env, INDEXED);
        runAndWait(env, "knowledge-ingest", minutes);
        CloudEvent indexed = awaitNew(env, INDEXED, indexedBefore);
        int nodesBefore = count(env, NODES);
        runAndWait(env, "knowledge-announce", indexed);
        CloudEvent nodes = awaitNew(env, NODES, nodesBefore);
        runAndWait(env, "architecture-links", nodes);
        List<String> pointers = new ArrayList<>();
        Json.read(nodes.getData().toBytes()).path("items").forEach(i -> pointers.add(i.path("pointer").asText()));
        return pointers;
    }

    private static void runAndWait(Env env, String name, CloudEvent fact) throws Exception {
        WorkflowDefinition definition = definition(env, name);
        String id = env.engine().startFromEvent(definition.ref(), fact).toCompletableFuture().get(30, TimeUnit.SECONDS);
        long until = System.nanoTime() + env.patience().toNanos();
        InstanceSnapshot snapshot = load(env, id);
        while (!ended(snapshot) && System.nanoTime() < until) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
            snapshot = load(env, id);
        }
        if (snapshot.status() != InstanceStatus.COMPLETED) {
            throw new IllegalStateException(name + " ended " + snapshot.status() + " at " + snapshot.position());
        }
    }

    // --- the architecture area ----------------------------------------------------------------------------------

    private static PairRun architectureMeeting(Env env, HttpClient http, ArchitectureMeeting meeting, Set<String> held)
            throws Exception {
        Corpus.Draft key = meeting.drafts().getFirst();
        List<FakeArchitectureModel.Drafted> drafted = FakeArchitectureModel.draft(meeting.text());
        if (drafted.size() != 1) {
            throw new IllegalStateException(meeting.id() + " drafted " + drafted.size() + " decisions, the key has one");
        }
        FakeArchitectureModel.Drafted decision = drafted.getFirst();
        ObjectNode draft = Json.obj();
        ArrayNode decisions = draft.putArray("decisions");
        ObjectNode d = decisions.addObject().put("id", decision.id()).put("title", decision.title()).put("summary", decision.summary());
        ArrayNode motivatedBy = d.putArray("motivatedBy");
        for (String label : decision.motivatedBy()) {
            String pointer = pointerOf(env, label);
            if (!held.contains(pointer)) {
                throw new IllegalStateException("The architecture area holds no announced pointer for '" + label + "'");
            }
            motivatedBy.add(pointer);
        }
        URI draftRef = env.blobs().put(Json.bytes(draft), "adr-draft/" + meeting.id() + ".json", ArchitectureAdrPipeline.DRAFT)
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        WorkflowDefinition adr = definition(env, "architecture-adr");
        env.workers().originDown(key.originDown());
        try {
            Instant started = Instant.now();
            String id = env.engine().startFromEvent(adr.ref(), ArchitectureAdrPipeline.drafted(meeting.id(), draftRef,
                    key.originDown() ? "follow" : null)).toCompletableFuture().get(30, TimeUnit.SECONDS);
            Set<String> seen = new HashSet<>();
            long until = System.nanoTime() + env.patience().toNanos();
            InstanceSnapshot snapshot = load(env, id);
            while (!ended(snapshot) && System.nanoTime() < until) {
                for (JsonNode request : env.channel().allOpen()) {
                    String reviewId = request.path("reviewId").asText();
                    if (seen.add(reviewId)) {
                        ObjectNode answer = Json.obj().put("reviewer", "rev-scripted").put("value", key.verdict());
                        http.send(HttpRequest.newBuilder(env.channel().baseUri().resolve("/reviews/" + reviewId))
                                .header(ReviewChannel.ROLE_HEADER, request.path("role").asText())
                                .POST(HttpRequest.BodyPublishers.ofByteArray(Json.bytes(answer))).build(),
                                HttpResponse.BodyHandlers.discarding());
                    }
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(20));
                snapshot = load(env, id);
            }
            Duration total = Duration.between(started, Instant.now());
            JsonNode context = snapshot.context() == null ? Json.obj() : snapshot.context();
            JsonNode result = context.path("results").isArray() && !context.path("results").isEmpty()
                    ? context.path("results").get(0) : Json.obj();
            JsonNode fact = awaitFactOrNull(env, ArchitectureAdrPipeline.PUBLISHED, meeting.id());
            String page = fact == null ? null : fact.path("pages").path(0).path("status").asText(null);
            return new PairRun(meeting.id(), key.id(), id, snapshot.status(), key.verdict(), key.reviewed(),
                    result.path("verdict").asText(null), result.path("reviewed").asBoolean(false), result.path("auto").asText(null),
                    fact == null ? null : fact.path("resolution").asText(null), page, key.originDown(), seen.size(), total,
                    env.metrics().stepLatencies(id));
        } finally {
            env.workers().originDown(false);
        }
    }

    /** The pointer of the product decision named {@code label}, at the version its origin holds. */
    private static String pointerOf(Env env, String label) {
        var scope = KnowledgeIngestPipeline.SCOPE;
        NodeView node = env.workers().product.view(scope, new ViewQuery(Sensitivity.RESTRICTED, 500, null, null))
                .toCompletableFuture().join().nodes().stream()
                .filter(n -> n.type().equals("Decision") && label.equals(n.label())).findFirst()
                .orElseThrow(() -> new IllegalStateException("The product base holds no decision '" + label + "'"));
        var log = env.workers().product.changes(scope, 0, 10_000).toCompletableFuture().join();
        return GraphPointer.of(scope, node.id(), GraphPointers.versionOf(log, node.id()).orElseThrow()).format();
    }

    // --- facts and state ---------------------------------------------------------------------------------------

    /** The last fact of {@code type} about {@code subject}; a fact leaves a moment after its instance is saved as ended. */
    private static CloudEvent awaitFact(Env env, String type, String subject) {
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < until) {
            var found = env.channel().published().stream().filter(e -> e.getType().equals(type))
                    .filter(e -> subject.equals(e.getSubject())).reduce((a, b) -> b);
            if (found.isPresent()) {
                return found.get();
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        throw new IllegalStateException("No fact " + type + " about " + subject + " was published");
    }

    private static int count(Env env, String type) {
        return (int) env.channel().published().stream().filter(e -> e.getType().equals(type)).count();
    }

    /** The last fact of {@code type} once there are more than {@code before} of them. */
    private static CloudEvent awaitNew(Env env, String type, int before) {
        long until = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < until) {
            if (count(env, type) > before) {
                return env.channel().published().stream().filter(e -> e.getType().equals(type)).reduce((a, b) -> b).orElseThrow();
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        throw new IllegalStateException("No new fact " + type + " was published");
    }

    private static JsonNode awaitFactOrNull(Env env, String type, String subject) {
        try {
            return Json.read(awaitFact(env, type, subject).getData().toBytes());
        } catch (IllegalStateException e) {
            return null;
        }
    }

    private static WorkflowDefinition definition(Env env, String name) {
        return env.definitions().stream().filter(d -> d.ref().name().equals(name)).findFirst().orElseThrow();
    }

    private static InstanceSnapshot load(Env env, String id) throws Exception {
        return env.state().load(id).toCompletableFuture().get(10, TimeUnit.SECONDS).orElseThrow();
    }

    private static boolean ended(InstanceSnapshot snapshot) {
        return snapshot.status() == InstanceStatus.COMPLETED || snapshot.status() == InstanceStatus.FAILED
                || snapshot.status() == InstanceStatus.ABORTED;
    }
}
