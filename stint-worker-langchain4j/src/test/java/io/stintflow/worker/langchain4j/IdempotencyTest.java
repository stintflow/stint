package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.answer;
import static io.stintflow.worker.langchain4j.Fakes.failure;
import static io.stintflow.worker.langchain4j.Fakes.run;
import static io.stintflow.worker.langchain4j.Fakes.task;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;
import io.stintflow.worker.langchain4j.Fakes.Summarizer;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * SDD 3a.3, CA5 (RF4): at-least-once delivery never duplicates the memory — a redelivery returns the committed output
 * without calling the model, concurrent deliveries commit once, and a failed execution commits nothing.
 */
class IdempotencyTest {

    private final InMemoryConversationStore store = new InMemoryConversationStore();
    private final AtomicInteger answers = new AtomicInteger();

    private List<Exchange> committed(String instanceId) {
        return store.committed(MemoryKey.instance(instanceId)).toCompletableFuture().join();
    }

    @Test
    void a_redelivery_returns_the_committed_output_without_calling_the_model() {
        ScriptedModel model = new ScriptedModel(request -> answer("answer-" + answers.incrementAndGet()));
        TaskHandler handler = LangChain4jWorker.builder().chatModel(model).conversations(store).build()
                .handler(Chat.class, (ai, t) -> ai.reply("question"));

        JsonNode first = run(handler, task("i-A", "/do/0/a", "c-1"));
        JsonNode again = run(handler, task("i-A", "/do/0/a", "c-1"));

        assertThat(first.asText()).isEqualTo("answer-1");
        assertThat(again).isEqualTo(first);
        assertThat(model.requests).hasSize(1);
        assertThat(committed("i-A")).singleElement().satisfies(e -> {
            assertThat(e.correlationId()).isEqualTo("c-1");
            assertThat(e.messages()).hasSize(2);
        });
    }

    @Test
    void concurrent_deliveries_commit_once_and_both_return_the_winner_s_output() throws Exception {
        CyclicBarrier bothInside = new CyclicBarrier(2);
        ScriptedModel model = new ScriptedModel(request -> {
            try {
                bothInside.await(10, TimeUnit.SECONDS);   // neither commits before both called the model
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return answer("answer-" + answers.incrementAndGet());
        });
        TaskHandler handler = LangChain4jWorker.builder().chatModel(model).conversations(store).build()
                .handler(Chat.class, (ai, t) -> ai.reply("question"));
        ExecutorService threads = Executors.newFixedThreadPool(2);
        try {
            CompletableFuture<JsonNode> one = CompletableFuture.supplyAsync(
                    () -> run(handler, task("i-A", "/do/0/a", "c-1")), threads);
            CompletableFuture<JsonNode> two = CompletableFuture.supplyAsync(
                    () -> run(handler, task("i-A", "/do/0/a", "c-1")), threads);

            assertThat(one.get(10, TimeUnit.SECONDS)).isEqualTo(two.get(10, TimeUnit.SECONDS));
        } finally {
            threads.shutdownNow();
        }
        assertThat(model.requests).hasSize(2);
        assertThat(committed("i-A")).singleElement().satisfies(e -> assertThat(e.output()).isEqualTo(
                run(handler, task("i-A", "/do/0/a", "c-1"))));
    }

    @Test
    void a_failed_attempt_commits_nothing_and_the_retry_sees_only_committed_exchanges() {
        ScriptedModel model = new ScriptedModel(request -> {
            if (Fakes.lastUserText(request).contains("flaky") && answers.incrementAndGet() == 1) {   // attempt 1
                throw new IllegalStateException("provider hiccup");
            }
            return answer(request.messages().size() + ":" + Fakes.lastUserText(request));
        });
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).build();
        run(worker.handler(Chat.class, (ai, t) -> ai.reply("first")), task("i-A", "/do/0/a", "c-1"));
        TaskHandler flaky = worker.handler(Chat.class, (ai, t) -> ai.reply("flaky"));

        TaskFailure failed = failure(flaky, task("i-A", "/do/1/b", "c-2", 1, null));
        JsonNode retried = run(flaky, task("i-A", "/do/1/b", "c-3", 2, null));

        assertThat(failed.toErrorInfo().type()).isEqualTo(ErrorInfo.TYPE_RUNTIME);
        assertThat(retried.asText()).isEqualTo("3:flaky");   // step 1's two messages and its own question
        assertThat(committed("i-A")).extracting(Exchange::correlationId).containsExactly("c-1", "c-3");
    }

    @Test
    void an_output_out_of_its_contract_commits_nothing() {
        ScriptedModel model = ScriptedModel.replying("{\"topic\":\"\",\"items\":-1}");
        TaskHandler handler = LangChain4jWorker.builder().chatModel(model).conversations(store).build()
                .service(Summarizer.class)
                .validate(output -> output.path("items").asInt() < 0 ? List.of("items must not be negative") : List.of())
                .handler((ai, t) -> ai.summarize("synthetic"));

        ErrorInfo error = failure(handler, task("i-A", "/do/0/a", "c-1")).toErrorInfo();

        assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
        assertThat(error.status()).isEqualTo(400);
        assertThat(error.detail()).isEqualTo("The LLM output is out of its contract: items must not be negative");
        assertThat(committed("i-A")).isEmpty();
    }

    @Test
    void an_answer_that_does_not_fit_the_output_type_is_a_validation_error() {
        ScriptedModel model = ScriptedModel.replying("this is not the JSON of a Summary");
        TaskHandler handler = LangChain4jWorker.builder().chatModel(model).conversations(store).build()
                .handler(Summarizer.class, (ai, t) -> ai.summarize("synthetic"));

        ErrorInfo error = failure(handler, task("i-A", "/do/0/a", "c-1")).toErrorInfo();

        assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_VALIDATION);
        assertThat(error.status()).isEqualTo(400);
        assertThat(error.detail()).startsWith("The model's answer does not fit the output type (")
                .doesNotContain("this is not the JSON");
        assertThat(committed("i-A")).isEmpty();
    }
}
