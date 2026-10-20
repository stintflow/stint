package io.stintflow.it;

import static io.stintflow.it.AiFixture.input;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.it.AiFixture.Chat;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.spi.TimerRequest;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * SDD 3a.3c, CA5, end to end: the first attempt of an LLM step outlives its timeout. The engine gives up on it — and
 * retries, or takes the {@code catch} — while the attempt is still running; when it ends, past its deadline, its
 * exchange is not committed, so the next step's conversation holds only what the workflow used. Latches and the
 * fixture's clock, never a sleep.
 */
class AbandonedAttemptTest {

    static final String TIMEOUT = "https://serverlessworkflow.io/spec/1.0.0/errors/timeout";

    /** {@code slow} in a try with a 30 s timeout; {@code %s} is the catch's body; then a step asking what was asked. */
    static final String WORKFLOW = """
            do:
              - guarded:
                  try:
                    - first:
                        call: remote
                        with:
                          task: slow
                          input:
                            question: ${ "first about " + .text }
                        timeout: { after: { seconds: 30 } }
                  catch:
                    errors:
                      with:
                        type: %s
            %s
              - second:
                  call: remote
                  with:
                    task: ask
                    input:
                      question: what did I ask?
                  export: { as: '${ {second: .} }' }
            """;

    static final String RETRY = """
            retry:
              delay: { seconds: 1 }
              limit:
                attempt: { count: 2 }""";

    static final String FALLBACK = """
            do:
              - fallback:
                  set: '${ {fallback: true} }'""";

    private final ExecutorService threads = Executors.newVirtualThreadPerTaskExecutor();
    private final CountDownLatch attempt1Started = new CountDownLatch(1);
    private final CountDownLatch releaseAttempt1 = new CountDownLatch(1);
    private final CompletableFuture<Void> attempt1Ended = new CompletableFuture<>();

    @AfterEach
    void stop() {
        releaseAttempt1.countDown();
        threads.shutdownNow();
    }

    /** The fixture, with {@code slow}: an LLM step whose first attempt waits for the test, off the dispatch thread. */
    private AiFixture fixture(String catchBody, RuleModel model) throws Exception {
        AtomicReference<TaskHandlerRegistry> registry = new AtomicReference<>();
        AiFixture f = new AiFixture(WORKFLOW.formatted(TIMEOUT, catchBody.indent(8).stripTrailing()), model, b -> b,
                io.stintflow.spi.ExecutionTracer.NOOP, registry::set);
        TaskHandler slow = LangChain4jWorker.builder().chatModel(model).conversations(f.conversations)
                .clock(f.now::get).build().handler(Chat.class, (ai, t) -> {
                    if (t.task().attempt() == 1) {
                        attempt1Started.countDown();
                        releaseAttempt1.await(10, TimeUnit.SECONDS);
                    }
                    return ai.reply(t.task().input().path("question").asText() + " (attempt "
                            + t.task().attempt() + ")");
                });
        registry.get().register("slow", ctx -> CompletableFuture.supplyAsync(() -> {
            try {
                return slow.execute(ctx).toCompletableFuture().join();
            } finally {
                if (ctx.attempt() == 1) {
                    attempt1Ended.complete(null);
                }
            }
        }, threads));
        return f;
    }

    private static TimerRequest awaitTaskTimer(AiFixture f) throws InterruptedException {
        TimerRequest timer = f.armed.poll(10, TimeUnit.SECONDS);
        assertThat(timer).as("the task timeout was armed").isNotNull();
        assertThat(timer.timerId()).startsWith("timer:");
        return timer;
    }

    /** Attempt 1 runs on until its deadline, ends there — before the engine's timer fires — and the timer fires. */
    private void outliveTheTimeout(AiFixture f) throws Exception {
        TimerRequest timeout = awaitTaskTimer(f);
        assertThat(attempt1Started.await(10, TimeUnit.SECONDS)).isTrue();
        f.now.set(timeout.fireAt());
        releaseAttempt1.countDown();
        attempt1Ended.get(10, TimeUnit.SECONDS);
        f.fire(timeout);
    }

    private static List<Exchange> memory(AiFixture f, String id) {
        return f.conversations.exchangesOf(id).toCompletableFuture().join();
    }

    @Test
    void with_a_retry_the_next_step_sees_only_the_accepted_attempt() throws Exception {
        RuleModel model = RuleModel.echoing();
        AiFixture f = fixture(RETRY, model);
        String id = f.start(input("text", "apples"));

        outliveTheTimeout(f);
        f.fire(f.awaitRetryTimer());
        JsonNode done = f.awaitCompleted(id);

        assertThat(memory(f, id)).extracting(Exchange::attempt).containsExactly(2, 1);   // first (attempt 2), second
        assertThat(memory(f, id)).extracting(Exchange::taskId)
                .containsExactly("/do/0/guarded", "/do/1/second");   // a task in a try is dispatched as the try
        // the second step saw one question and one answer before its own: attempt 2's
        assertThat(done.path("second").asText()).isEqualTo("3:what did I ask?");
        assertThat(model.requests.getLast().messages().toString()).contains("first about apples (attempt 2)")
                .doesNotContain("(attempt 1)");
        assertThat(model.requests).hasSize(3);   // attempt 1 did call the model; it only didn't commit
    }

    @Test
    void with_a_catch_the_next_step_sees_nothing_of_the_abandoned_attempt() throws Exception {
        RuleModel model = RuleModel.echoing();
        AiFixture f = fixture(FALLBACK, model);
        String id = f.start(input("text", "pears"));

        outliveTheTimeout(f);
        JsonNode done = f.awaitCompleted(id);

        assertThat(memory(f, id)).extracting(Exchange::taskId).containsExactly("/do/1/second");
        assertThat(done.path("second").asText()).isEqualTo("1:what did I ask?");
        assertThat(model.requests.getLast().messages().toString()).doesNotContain("pears");
        assertThat(f.conversations.committed(MemoryKey.instance(id)).toCompletableFuture().join()).hasSize(1);
    }
}
