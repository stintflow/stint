package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.run;
import static io.stintflow.worker.langchain4j.Fakes.task;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.UserMessage;
import dev.langchain4j.service.Result;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.ChatWithSystem;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;
import io.stintflow.worker.langchain4j.Fakes.Summarizer;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/** SDD 3a.3: the handler (CA1) and the conversation memory (CA2), with a scripted model and synthetic data. */
class LangChain4jWorkerTest {

    private final InMemoryConversationStore store = new InMemoryConversationStore();

    @Test
    void a_handler_returns_the_structured_type_as_json() {
        ScriptedModel model = ScriptedModel.replying("{\"topic\":\"synthetic planning\",\"items\":3}");
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).build();
        TaskHandler handler = worker.handler(Summarizer.class,
                (ai, t) -> ai.summarize(t.input().path("text").asText()));

        JsonNode output = run(handler, task("i-1", "/do/0/summarize", "c-1"));

        assertThat(output.path("topic").asText()).isEqualTo("synthetic planning");
        assertThat(output.path("items").asInt()).isEqualTo(3);
        assertThat(output.size()).isEqualTo(2);
    }

    @Test
    void strings_and_results_are_outputs_too() {
        ScriptedModel model = ScriptedModel.replying("plain text");
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).build();

        assertThat(run(worker.handler(Chat.class, (ai, t) -> ai.reply("hi")), task("i-1", "/do/0/a", "c-1"))
                .asText()).isEqualTo("plain text");
        assertThat(run(worker.handler(Chat.class, (ai, t) -> Result.builder().content(ai.reply("hi")).build()),
                task("i-1", "/do/0/b", "c-2")).asText()).isEqualTo("plain text");
    }

    @Test
    void two_steps_of_an_instance_share_its_memory_and_other_instances_do_not() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).build();
        TaskHandler first = worker.handler(Chat.class, (ai, t) -> ai.reply("first step of " + t.task().instanceId()));
        TaskHandler second = worker.handler(Chat.class, (ai, t) -> ai.reply("second step"));

        run(first, task("i-A", "/do/0/first", "c-1"));
        run(first, task("i-B", "/do/0/first", "c-2"));
        JsonNode secondA = run(second, task("i-A", "/do/1/second", "c-3"));

        // step 2 of A saw step 1 of A (its question and the answer), never anything of B
        assertThat(secondA.asText()).isEqualTo("3:second step");
        assertThat(texts(model.lastMessages())).containsExactly("first step of i-A", "1:first step of i-A",
                "second step");
        assertThat(store.committed(MemoryKey.instance("i-A")).toCompletableFuture().join())
                .extracting(Exchange::correlationId).containsExactly("c-1", "c-3");
        assertThat(store.committed(MemoryKey.instance("i-B")).toCompletableFuture().join())
                .extracting(Exchange::correlationId).containsExactly("c-2");
    }

    @Test
    void a_task_scope_isolates_two_steps_of_the_same_instance() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).build();
        TaskHandler own = worker.service(Chat.class).memory(MemoryScope.TASK).handler((ai, t) -> ai.reply("own"));

        run(own, task("i-A", "/do/0/left", "c-1"));
        JsonNode right = run(own, task("i-A", "/do/1/right", "c-2"));
        JsonNode leftAgain = run(own, task("i-A", "/do/0/left", "c-3"));

        assertThat(right.asText()).isEqualTo("1:own");       // nothing of /do/0/left
        assertThat(leftAgain.asText()).isEqualTo("3:own");   // its own earlier exchange
        assertThat(store.committed(MemoryKey.task("i-A", "/do/0/left")).toCompletableFuture().join()).hasSize(2);
        assertThat(store.committed(MemoryKey.instance("i-A")).toCompletableFuture().join()).isEmpty();
    }

    @Test
    void without_a_store_or_with_scope_none_there_is_no_memory() {
        ScriptedModel model = ScriptedModel.echoing();
        TaskHandler stateless = LangChain4jWorker.builder().chatModel(model).build()
                .handler(Chat.class, (ai, t) -> ai.reply("again"));
        TaskHandler none = LangChain4jWorker.builder().chatModel(model).conversations(store).build()
                .service(Chat.class).memory(MemoryScope.NONE).handler((ai, t) -> ai.reply("again"));

        run(stateless, task("i-A", "/do/0/a", "c-1"));
        assertThat(run(stateless, task("i-A", "/do/0/a", "c-2")).asText()).isEqualTo("1:again");
        run(none, task("i-A", "/do/0/a", "c-3"));
        assertThat(run(none, task("i-A", "/do/0/a", "c-4")).asText()).isEqualTo("1:again");
        assertThat(store.exchangesOf("i-A").toCompletableFuture().join()).isEmpty();
    }

    @Test
    void the_model_sees_a_window_of_the_conversation_and_the_store_keeps_it_all() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).memoryWindow(3)
                .build();
        TaskHandler step = worker.handler(Chat.class, (ai, t) -> ai.reply("step " + t.task().correlationId()));

        for (int i = 1; i <= 4; i++) {
            run(step, task("i-A", "/do/" + i + "/step", "c-" + i));
        }

        // LangChain4j 1.11.7: the window applies to the conversation, and the new question comes on top of it
        assertThat(texts(model.lastMessages())).containsExactly("3:step c-2", "step c-3", "4:step c-3", "step c-4");
        assertThat(store.committed(MemoryKey.instance("i-A")).toCompletableFuture().join()).hasSize(4)
                .allSatisfy(e -> assertThat(e.messages()).hasSize(2));
    }

    @Test
    void the_system_message_reaches_the_model_but_is_never_committed() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).build();
        TaskHandler step = worker.handler(ChatWithSystem.class, (ai, t) -> ai.reply("hello"));

        run(step, task("i-A", "/do/0/a", "c-1"));
        run(step, task("i-A", "/do/1/b", "c-2"));

        assertThat(model.lastMessages()).hasSize(4).first().isInstanceOf(SystemMessage.class);
        assertThat(model.lastMessages()).filteredOn(SystemMessage.class::isInstance).hasSize(1);
        assertThat(store.committed(MemoryKey.instance("i-A")).toCompletableFuture().join())
                .allSatisfy(e -> assertThat(e.messages().toString()).doesNotContain("synthetic test assistant"));
    }

    @Test
    void forget_deletes_every_scope_of_an_instance() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).build();
        run(worker.handler(Chat.class, (ai, t) -> ai.reply("x")), task("i-A", "/do/0/a", "c-1"));
        run(worker.service(Chat.class).memory(MemoryScope.TASK).handler((ai, t) -> ai.reply("y")),
                task("i-A", "/do/1/b", "c-2"));
        run(worker.handler(Chat.class, (ai, t) -> ai.reply("z")), task("i-B", "/do/0/a", "c-3"));

        worker.forget("i-A").toCompletableFuture().join();

        assertThat(store.exchangesOf("i-A").toCompletableFuture().join()).isEmpty();
        assertThat(store.exchangesOf("i-B").toCompletableFuture().join()).hasSize(1);
    }

    @Test
    void each_exchange_expires_after_the_retention() {
        Fakes.Clock clock = new Fakes.Clock();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(clock).retention(Duration.ofDays(7)).build();

        run(worker.handler(Chat.class, (ai, t) -> ai.reply("x")), task("i-A", "/do/0/a", "c-1"));

        assertThat(store.committed(MemoryKey.instance("i-A")).toCompletableFuture().join()).singleElement()
                .satisfies(e -> {
                    assertThat(e.committedAt()).isEqualTo(Fakes.T0);
                    assertThat(e.expiresAt()).isEqualTo(Fakes.T0.plus(Duration.ofDays(7)));
                    assertThat(e.taskId()).isEqualTo("/do/0/a");
                    assertThat(e.attempt()).isEqualTo(1);
                });
    }

    static List<String> texts(List<ChatMessage> messages) {
        return messages.stream().map(m -> switch (m) {
            case UserMessage u -> u.singleText();
            case AiMessage a -> a.text();
            default -> m.type().name();
        }).toList();
    }
}
