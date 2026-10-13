package io.stintflow.worker.langchain4j;

import static io.stintflow.worker.langchain4j.Fakes.failure;
import static io.stintflow.worker.langchain4j.Fakes.run;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskFailure;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.Fakes.Chat;
import io.stintflow.worker.langchain4j.Fakes.Logs;
import io.stintflow.worker.langchain4j.Fakes.ScriptedModel;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.InMemoryConversationStore;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * SDD 3a.3c, CA3, CA4 and CA6: past its deadline an execution neither calls the model nor commits its exchange, and
 * the conversation a step reads leaves out the attempts a later attempt of the same pass superseded. Controllable
 * clock, no sleep, synthetic data.
 */
class DeadlineTest {

    static final Instant DEADLINE = Fakes.T0.plusSeconds(60);

    private final InMemoryConversationStore store = new InMemoryConversationStore();
    private final Fakes.Clock clock = new Fakes.Clock();

    static TaskContext task(String taskId, String correlationId, int attempt, String text, Instant deadline) {
        return new TaskContext(taskId, "i-A", correlationId, attempt, Json.obj().put("text", text),
                new WorkflowRef("test", "ai", "1.0.0"), "chn-i-A", Lineage.NONE, deadline);
    }

    private List<Exchange> memory() {
        return store.committed(MemoryKey.instance("i-A")).toCompletableFuture().join();
    }

    /** A step whose execution takes {@code took} on the controllable clock. */
    private TaskHandler step(LangChain4jWorker worker, Duration took) {
        return worker.handler(Chat.class, (ai, t) -> {
            clock.advance(took);
            return ai.reply(t.input().path("text").asText());
        });
    }

    @Test
    void an_execution_ending_inside_the_commit_margin_does_not_commit_and_warns_with_ids_only() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).clock(clock)
                .build();   // default margin: 5 s

        TaskFailure late;
        try (Logs logs = new Logs()) {
            late = failure(step(worker, Duration.ofSeconds(56)),
                    task("/do/0/ask", "c-1", 1, "SECRET-DEADLINE-MARKER", DEADLINE));

            assertThat(logs.lines).anySatisfy(line -> assertThat(line).startsWith("WARN")
                    .contains("Task /do/0/ask of instance i-A (correlation c-1) ended 4000 ms before its deadline, "
                            + "inside the commit margin of 5000 ms"));
            assertThat(logs.joined()).doesNotContain("SECRET-DEADLINE-MARKER");
        }
        ErrorInfo error = late.toErrorInfo();
        assertThat(error.type()).isEqualTo(ErrorInfo.TYPE_TIMEOUT);
        assertThat(error.status()).isEqualTo(408);
        assertThat(error.detail()).isEqualTo("The execution ended past its deadline; its exchange was not committed");
        assertThat(model.requests).hasSize(1);   // the model was called; only the commit was skipped
        assertThat(memory()).isEmpty();
    }

    @Test
    void an_execution_ending_before_the_margin_commits() {
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(clock).build();

        JsonNode output = run(step(worker, Duration.ofSeconds(54)), task("/do/0/ask", "c-1", 1, "question", DEADLINE));

        assertThat(output.asText()).isEqualTo("1:question");
        assertThat(memory()).extracting(Exchange::correlationId).containsExactly("c-1");
    }

    @Test
    void a_delivery_past_its_deadline_does_not_call_the_model_but_a_committed_one_still_returns_its_output() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).clock(clock)
                .build();
        TaskHandler step = step(worker, Duration.ZERO);
        run(step, task("/do/0/ask", "c-1", 1, "question", DEADLINE));   // committed in time
        clock.advance(Duration.ofSeconds(60));                          // now at the deadline

        TaskFailure tooLate = failure(step, task("/do/1/next", "c-2", 1, "next", DEADLINE));
        JsonNode again = run(step, task("/do/0/ask", "c-1", 1, "question", DEADLINE));   // a redelivery of c-1

        assertThat(tooLate.toErrorInfo().type()).isEqualTo(ErrorInfo.TYPE_TIMEOUT);
        assertThat(tooLate.toErrorInfo().detail()).isEqualTo("The task's deadline passed before its execution started");
        assertThat(again.asText()).isEqualTo("1:question");
        assertThat(model.requests).hasSize(1);   // neither the late delivery nor the redelivery called the model
    }

    @Test
    void without_a_deadline_nothing_changes() {
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(ScriptedModel.echoing()).conversations(store)
                .clock(clock).build();

        run(step(worker, Duration.ofDays(1)), task("/do/0/ask", "c-1", 1, "question", null));

        assertThat(memory()).extracting(Exchange::correlationId).containsExactly("c-1");
    }

    @Test
    void a_step_reads_only_the_latest_attempt_of_each_pass() {
        ScriptedModel model = ScriptedModel.echoing();
        LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store).clock(clock)
                .commitMargin(Duration.ZERO).build();   // lets the abandoned attempt commit, as in the residual race
        TaskHandler step = step(worker, Duration.ofSeconds(1));

        run(step, task("/do/0/ask", "c-1", 1, "abandoned attempt", null));
        run(step, task("/do/0/ask", "c-2", 2, "accepted attempt", null));
        // the retry itself didn't read the attempt it replaces
        assertThat(LangChain4jWorkerTest.texts(model.lastMessages())).containsExactly("accepted attempt");
        run(step, task("/do/1/next", "c-3", 1, "next step", null));

        // the next step saw only the accepted attempt: its question and answer, then its own question
        assertThat(LangChain4jWorkerTest.texts(model.lastMessages()))
                .containsExactly("accepted attempt", "1:accepted attempt", "next step");

        // a new pass through /do/0/ask (attempt 1 again) doesn't hide the earlier pass
        run(step, task("/do/0/ask", "c-4", 1, "second pass", null));
        run(step, task("/do/2/last", "c-5", 1, "last step", null));
        assertThat(LangChain4jWorkerTest.texts(model.lastMessages())).containsExactly("accepted attempt",
                "1:accepted attempt", "next step", "3:next step", "second pass", "5:second pass", "last step");

        // nothing is deleted: the superseded exchange is still stored, and forgotten with the instance
        assertThat(store.exchangesOf("i-A").toCompletableFuture().join()).extracting(Exchange::correlationId)
                .containsExactly("c-1", "c-2", "c-3", "c-4", "c-5");
        worker.forget("i-A").toCompletableFuture().join();
        assertThat(store.exchangesOf("i-A").toCompletableFuture().join()).isEmpty();
    }

    @Test
    void the_superseded_rule_by_itself() {
        Exchange a1 = exchange("/do/0/ask", "c-1", 1);
        Exchange a2 = exchange("/do/0/ask", "c-2", 2);
        Exchange a3 = exchange("/do/0/ask", "c-3", 3);
        Exchange other = exchange("/do/1/next", "c-4", 1);
        Exchange again = exchange("/do/0/ask", "c-5", 1);
        Exchange againRetry = exchange("/do/0/ask", "c-6", 2);

        assertThat(LangChain4jWorker.withoutSupersededAttempts(List.of(a1, a2, a3, other, again, againRetry),
                "/do/2/last", 1)).containsExactly(a3, other, againRetry);
        assertThat(LangChain4jWorker.withoutSupersededAttempts(List.of(a1, other, again), "/do/2/last", 1))
                .containsExactly(a1, other, again);
        // the reader as the latest attempt: attempt 3 of the second pass hides that pass's earlier attempts only
        assertThat(LangChain4jWorker.withoutSupersededAttempts(List.of(a1, a2, other, again, againRetry),
                "/do/0/ask", 3)).containsExactly(a2, other);
        // attempt 1 of a new pass hides nothing
        assertThat(LangChain4jWorker.withoutSupersededAttempts(List.of(a1, other), "/do/0/ask", 1))
                .containsExactly(a1, other);
    }

    private static Exchange exchange(String taskId, String correlationId, int attempt) {
        return new Exchange(MemoryKey.instance("i-A"), correlationId, taskId, attempt, Fakes.T0,
                Json.MAPPER.createArrayNode(), null, null);
    }
}
