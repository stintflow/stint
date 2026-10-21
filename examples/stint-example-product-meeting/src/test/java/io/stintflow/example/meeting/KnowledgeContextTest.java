package io.stintflow.example.meeting;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.model.chat.request.ChatRequest;
import dev.langchain4j.model.chat.response.ChatResponse;
import io.cloudevents.CloudEvent;
import io.stintflow.example.meeting.Corpus.Meeting;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.wire.Json;

/**
 * SDD 3b.4, CA3 — the milestone of the phase: the minutes of a meeting are written with what the base holds from the one before,
 * the model is handed that context with the document it came from, and the minutes end with the documents that were handed over.
 * Fake models, no sleep; synthetic data only.
 */
class KnowledgeContextTest {

    @TempDir
    Path work;

    private final List<Meeting> meetings = Corpus.load();

    /** The minutes model of the build, remembering every prompt it was given. */
    private static final class Recording implements ChatModel {
        final List<String> prompts = new CopyOnWriteArrayList<>();
        private final FakeMinutesModel inner = new FakeMinutesModel();

        @Override
        public ChatResponse doChat(ChatRequest request) {
            for (ChatMessage message : request.messages()) {
                if (message instanceof UserMessage user && user.hasSingleText()) {
                    prompts.add(user.singleText());
                }
            }
            return inner.doChat(request);
        }
    }

    private ProductMeetingPipeline.Stack stackWith(ChatModel minutes) {
        ProductMeetingPipeline.Stack fakes = ProductMeetingPipeline.fakes(meetings);
        return new ProductMeetingPipeline.Stack(fakes.transcriber(), fakes.docType(), fakes.urgency(), minutes,
                fakes.minutesProvider(), fakes.minutesModelName(), false, fakes.audio());
    }

    private static CloudEvent publishedFact(ProductMeetingPipeline pipeline, String meetingId) {
        return pipeline.channel.published().stream().filter(e -> e.getType().endsWith("minutes-published.v1"))
                .filter(e -> meetingId.equals(e.getSubject())).findFirst().orElseThrow();
    }

    private String minutesOf(ProductMeetingPipeline pipeline, String meetingId) {
        JsonNode fact = Json.read(publishedFact(pipeline, meetingId).getData().toBytes());
        return new String(pipeline.blobs.get(java.net.URI.create(fact.path("minutesRef").asText())).toCompletableFuture().join(),
                StandardCharsets.UTF_8);
    }

    @Test
    void ca3_the_minutes_of_the_second_meeting_are_written_with_the_first_and_cite_it() throws Exception {
        Recording model = new Recording();
        Meeting first = meetings.get(0);
        Meeting second = meetings.get(1);
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, stackWith(model),
                new RunMetrics(Map.of(), false), work);
                KnowledgeIngestPipeline ingest = new KnowledgeIngestPipeline(pipeline.blobs,
                        new FakeKnowledgeModel(Corpus.loadKnowledge().base().glossary()))) {
            pipeline.useKnowledge(ingest.store);

            pipeline.run(first, Duration.ofSeconds(60));
            ingest.ingest(publishedFact(pipeline, first.id()), Duration.ofSeconds(60));
            String before = model.prompts.getLast();
            pipeline.run(second, Duration.ofSeconds(60));
            String prompt = model.prompts.getLast();

            // the first meeting had nothing before it: its prompt is the prompt of before, and its minutes have no such section
            assertThat(before).doesNotContain("Earlier context");
            assertThat(minutesOf(pipeline, first.id())).doesNotContain("Context used");
            // the second is written knowing the first, each item with the document it came from
            assertThat(prompt).contains("Earlier context from the knowledge base").contains("[source: meeting:" + first.id() + "]");
            assertThat(prompt.indexOf("Earlier context")).isLessThan(prompt.indexOf("Transcript:"));
            String minutes = minutesOf(pipeline, second.id());
            assertThat(minutes).contains("## Context used").contains("- meeting:" + first.id());
            assertThat(minutes).doesNotContain("meeting:" + second.id());   // never itself
            // the read is in the audit trail with counts, no question, no content
            List<AuditRecord> reads = new ArrayList<>();
            for (String instance : List.of(pipeline.current)) {
                pipeline.audit.of(instance).toCompletableFuture().join().stream().filter(r -> r.kind().equals("knowledge-read"))
                        .forEach(reads::add);
            }
            assertThat(reads).isNotEmpty().allSatisfy(r -> {
                assertThat(r.detail()).containsKeys("mode", "hops", "trimmed", "ceiling");
                assertThat(r.toString()).doesNotContain("Transcript", "[source");
            });
            // and the planted personal data is nowhere in what was handed over
            Corpus.allMarkers(meetings).forEach(marker -> assertThat(prompt).doesNotContain(marker));
        }
    }

    @Test
    void without_a_base_the_prompt_and_the_minutes_are_what_they_were() throws Exception {
        Recording model = new Recording();
        try (ProductMeetingPipeline pipeline = new ProductMeetingPipeline(meetings, stackWith(model),
                new RunMetrics(Map.of(), false), work)) {
            pipeline.run(meetings.get(0), Duration.ofSeconds(60));

            assertThat(model.prompts).isNotEmpty().noneMatch(p -> p.contains("Earlier context"));
            assertThat(minutesOf(pipeline, meetings.get(0).id())).doesNotContain("Context used");
        }
    }
}
