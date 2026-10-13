package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.run;
import static io.stintflow.worker.langchain4j.Fakes.task;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.wire.Json;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.Logs;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * SDD 3a.3b, CA1 and CA2: an exchange is committed strictly after every exchange its execution read, so the memory
 * keeps the causal order whatever the clock says — a fixed clock, or one behind the history. The correlationIds are
 * picked so that their tie-break would put the steps the wrong way round.
 */
class ConversationOrderTest {

    private final InMemoryConversationStore store = new InMemoryConversationStore();

    private List<Exchange> memoryOf(String instanceId) {
        return store.committed(MemoryKey.instance(instanceId)).toCompletableFuture().join();
    }

    @Test
    void with_a_fixed_clock_the_second_step_is_committed_one_millisecond_after_the_first() {
        Fakes.Clock clock = new Fakes.Clock();
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).clock(clock)
                .build();
        TaskHandler step = worker.handler(Chat.class, (ai, t) -> ai.reply(t.input().path("text").asText()));

        run(step, task("i-A", "/do/0/first", "c-9", 1, Json.obj().put("text", "first")));
        run(step, task("i-A", "/do/1/second", "c-1", 1, Json.obj().put("text", "second")));
        JsonNode third = run(step, task("i-A", "/do/2/third", "c-5", 1, Json.obj().put("text", "third")));

        assertThat(memoryOf("i-A")).extracting(Exchange::taskId)
                .containsExactly("/do/0/first", "/do/1/second", "/do/2/third");
        assertThat(memoryOf("i-A")).extracting(Exchange::committedAt)
                .containsExactly(Fakes.T0, Fakes.T0.plusMillis(1), Fakes.T0.plusMillis(2));
        assertThat(memoryOf("i-A")).extracting(Exchange::expiresAt).containsExactly(Fakes.T0.plus(Duration.ofDays(30)),
                Fakes.T0.plusMillis(1).plus(Duration.ofDays(30)), Fakes.T0.plusMillis(2).plus(Duration.ofDays(30)));
        // the third step saw the first two in their order: question, answer, question, answer, its own question
        assertThat(third.asText()).isEqualTo("5:third");
        assertThat(LangChain4jWorkerTest.texts(model.lastMessages()))
                .containsExactly("first", "1:first", "second", "3:second", "third");
    }

    @Test
    void a_clock_ahead_of_the_history_is_used_as_is() {
        Fakes.Clock clock = new Fakes.Clock();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(clock).build();
        TaskHandler step = worker.handler(Chat.class, (ai, t) -> ai.reply("step"));

        run(step, task("i-A", "/do/0/first", "c-9"));
        clock.advance(Duration.ofSeconds(5));
        run(step, task("i-A", "/do/1/second", "c-1"));

        assertThat(memoryOf("i-A")).extracting(Exchange::committedAt)
                .containsExactly(Fakes.T0, Fakes.T0.plusSeconds(5));
    }

    @Test
    void a_clock_behind_the_history_still_commits_after_it_and_warns_with_ids_only() {
        Fakes.Clock ahead = new Fakes.Clock();
        ahead.advance(Duration.ofSeconds(10));
        Fakes.Clock behind = new Fakes.Clock();   // another worker, its clock 10 s late
        TaskHandler first = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(ahead).build().handler(Chat.class, (ai, t) -> ai.reply("SECRET-ORDER-MARKER first"));
        TaskHandler second = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(behind).build().handler(Chat.class, (ai, t) -> ai.reply("SECRET-ORDER-MARKER second"));

        run(first, task("i-A", "/do/0/first", "c-9"));
        try (Logs logs = new Logs()) {
            run(second, task("i-A", "/do/1/second", "c-1"));

            Instant read = Fakes.T0.plusSeconds(10);
            assertThat(memoryOf("i-A")).extracting(Exchange::committedAt).containsExactly(read, read.plusMillis(1));
            assertThat(logs.lines).anySatisfy(line -> assertThat(line).startsWith("WARN")
                    .contains("Task /do/1/second of instance i-A")
                    .contains("the worker's clock is 10000 ms behind the conversation it continues"));
            assertThat(logs.joined()).doesNotContain("SECRET-ORDER-MARKER");
        }
    }

    @Test
    void a_clock_slightly_behind_does_not_warn() {
        Fakes.Clock ahead = new Fakes.Clock();
        ahead.advance(Duration.ofMillis(500));
        Fakes.Clock behind = new Fakes.Clock();
        TaskHandler first = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(ahead).build().handler(Chat.class, (ai, t) -> ai.reply("first"));
        TaskHandler second = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(behind).build().handler(Chat.class, (ai, t) -> ai.reply("second"));

        run(first, task("i-A", "/do/0/first", "c-9"));
        try (Logs logs = new Logs()) {
            run(second, task("i-A", "/do/1/second", "c-1"));

            assertThat(memoryOf("i-A")).extracting(Exchange::taskId).containsExactly("/do/0/first", "/do/1/second");
            assertThat(logs.lines).noneSatisfy(line -> assertThat(line).contains("behind the conversation"));
        }
    }
}
