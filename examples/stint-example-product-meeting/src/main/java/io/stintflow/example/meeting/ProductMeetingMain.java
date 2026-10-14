package io.stintflow.example.meeting;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.config.ConfigProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import dev.langchain4j.model.openai.OpenAiChatModel;
import io.cloudevents.CloudEvent;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.example.meeting.ProductMeetingPipeline.MeetingRun;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.wire.Json;
import io.stintflow.worker.decision.jev.JevDecisionAdapter;
import io.stintflow.worker.transcription.FakeTranscriber;
import io.stintflow.worker.transcription.OpenAiCompatibleTranscriber;

/**
 * SDD 3a.6: the Phase 3a milestone, end to end, on the local engine.
 * <ul>
 *   <li>No argument (the build, CA1): every meeting of the corpus with fake models; each must follow its key —
 *       document type, review branch, urgency, items — be published once to the allowed space and emit its fact.
 *       Writes the metrics report and prints {@code PRODUCT-MEETING OK}.</li>
 *   <li>{@code manual} (CA2): the local stack on loopback — a Whisper server, LitJev and llama.cpp's server, see
 *       {@code LOCAL-STACK.md} — over the generated audio, checked against the manifest. Writes the report.</li>
 * </ul>
 * Synthetic data, fake credentials, loopback only.
 */
@QuarkusMain
public class ProductMeetingMain implements QuarkusApplication {

    public static void main(String... args) {
        Quarkus.run(ProductMeetingMain.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        List<Meeting> meetings = Corpus.load();
        boolean manual = args.length > 0 && args[0].equals("manual");
        RunMetrics metrics = new RunMetrics(Map.of(), manual);
        ProductMeetingPipeline.Stack stack = manual ? localStack(meetings) : ProductMeetingPipeline.fakes(meetings);
        Path work = Files.createTempDirectory("stint-product-meeting");
        List<MeetingRun> runs = new ArrayList<>();
        Map<String, byte[]> audio = new LinkedHashMap<>();
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, stack, metrics, work)) {
            for (Meeting meeting : meetings) {
                audio.put(meeting.id(), stack.audio().apply(meeting));
                MeetingRun run = pipeline.run(meeting, manual ? Duration.ofMinutes(30) : Duration.ofSeconds(60));
                runs.add(run);
                System.out.println("PRODUCT-MEETING " + meeting.id() + " " + run.status() + " docType="
                        + run.context().path("docType").asText() + " urgent=" + run.context().path("urgent").asText()
                        + " items=" + run.context().path("itemIds").size() + " reviewed="
                        + run.reviewsAnswered().contains("choice") + " page=" + run.context().path("pageId").asText()
                        + " ms=" + run.total().toMillis() + " answered=" + run.reviewsAnswered());
            }
            ObjectNode report = Report.build(runs, metrics, audio, manual ? "local" : "fake");
            Path dir = Path.of(setting("report-dir").orElse("target/product-meeting-report"));
            Path written = Report.write(report, dir, (manual ? "manual-" : "ci-")
                    + Instant.now().toString().replace(':', '-'));
            System.out.println("PRODUCT-MEETING report=" + written + " summary=" + report.path("summary"));
            if (manual) {
                return runs.stream().allMatch(r -> r.status() == InstanceStatus.COMPLETED) ? 0 : 1;
            }
            boolean ok = followsTheKey(runs, meetings) && published(pipeline, meetings);
            System.out.println(ok ? "PRODUCT-MEETING OK" : "PRODUCT-MEETING FAILED");
            return ok ? 0 : 1;
        }
    }

    /** Every meeting completed on its key's path, in one chain. */
    static boolean followsTheKey(List<MeetingRun> runs, List<Meeting> meetings) {
        boolean ok = runs.size() == meetings.size();
        for (MeetingRun run : runs) {
            JsonNode context = run.context();
            boolean fine = run.status() == InstanceStatus.COMPLETED
                    && run.meeting().expected().docType().equals(context.path("docType").asText())
                    && run.meeting().expected().urgent() == context.path("urgent").asBoolean()
                    && run.meeting().expected().items().size() == context.path("itemIds").size()
                    && run.meeting().expected().review() == run.reviewsAnswered().contains("choice")
                    && "approved".equals(context.path("verdict").asText())
                    && run.chains().size() == 1;
            if (!fine) {
                System.out.println("PRODUCT-MEETING " + run.meeting().id() + " off the key: status=" + run.status()
                        + " chains=" + run.chains().size());
            }
            ok &= fine;
        }
        return ok;
    }

    /** One page per meeting in the allowed space, and one minutes-published fact each. */
    static boolean published(ProductMeetingPipeline pipeline, List<Meeting> meetings) {
        long facts = pipeline.channel.published().stream()
                .filter(e -> e.getType().equals("io.stintflow.examples.minutes-published.v1")).count();
        boolean onePerMeeting = pipeline.confluence.pages().size() == meetings.size()
                && pipeline.confluence.pages().stream().allMatch(p -> p.spaceKey().equals(ProductMeetingPipeline.SPACE));
        System.out.println("PRODUCT-MEETING pages=" + pipeline.confluence.pages().size() + " facts=" + facts);
        return onePerMeeting && facts == meetings.size();
    }

    /** The manual run's stack: every model on loopback; the audio generated by scripts/generate-audio.py. */
    static ProductMeetingPipeline.Stack localStack(List<Meeting> meetings) throws IOException {
        Path corpus = Path.of(setting("corpus-dir").orElse("examples/stint-example-product-meeting/corpus"));
        JsonNode manifest = Json.read(Files.readAllBytes(corpus.resolve("manifest.json")));
        Map<String, byte[]> audio = new LinkedHashMap<>();
        for (JsonNode entry : manifest.path("meetings")) {
            JsonNode file = entry.path("audio");
            if (file.isMissingNode() || file.isNull()) {
                throw new IllegalStateException("No audio for " + entry.path("id").asText()
                        + ": run scripts/generate-audio.py first");
            }
            byte[] bytes = Files.readAllBytes(corpus.resolve(file.path("file").asText()));
            if (!FakeTranscriber.sha256(bytes).equals(file.path("sha256").asText())) {
                throw new IllegalStateException("The audio of " + entry.path("id").asText()
                        + " does not match the manifest");
            }
            audio.put(entry.path("id").asText(), bytes);
        }
        JevDecisionAdapter litjev = JevDecisionAdapter.builder().server(JevDecisionAdapter.Server.LITJEV)
                .baseUri(URI.create(setting("litjev.base-uri").orElse("http://127.0.0.1:8000")))
                .model(setting("litjev.model").orElse("Qwen/Qwen3.5-2B")).build();
        String llmModel = setting("llm.model").orElse("qwen3-4b-instruct-2507-q4_k_m");
        OpenAiChatModel llm = OpenAiChatModel.builder()
                .baseUrl(setting("llm.base-uri").orElse("http://127.0.0.1:8080") + "/v1")
                .apiKey("local").modelName(llmModel).temperature(0.0).timeout(Duration.ofMinutes(10))
                .maxRetries(0).build();
        return new ProductMeetingPipeline.Stack(
                OpenAiCompatibleTranscriber.builder()
                        .baseUri(URI.create(setting("whisper.base-uri").orElse("http://127.0.0.1:8001")))
                        .model(setting("whisper.model").orElse("Systran/faster-distil-whisper-small.en"))
                        .timeout(Duration.ofMinutes(20)).build(),
                litjev, litjev, llm, "llama.cpp", llmModel, false, m -> audio.get(m.id()));
    }

    private static java.util.Optional<String> setting(String name) {
        return ConfigProvider.getConfig().getOptionalValue("stint.example." + name, String.class);
    }

    static CloudEvent sample(Meeting meeting) {
        return ProductMeetingPipeline.meetingRecorded(meeting, URI.create("file:///synthetic/" + meeting.id()), 0);
    }
}
