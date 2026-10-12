package io.stintflow.it;

import static io.stintflow.it.AiFixture.input;
import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import dev.langchain4j.exception.AuthenticationException;
import dev.langchain4j.exception.RateLimitException;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.spi.ExecutionTracer;
import io.stintflow.spi.TimerRequest;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.MemoryKey;

/**
 * SDD 3a.3, end to end on a real engine with a fake model: the structured output reaches the workflow (CA1), the steps
 * of an instance share its memory and instances don't (CA2), an answer out of its type is a validation error the
 * workflow catches (CA6), and a rate limit is retried no sooner than its {@code retryAfter} while a credential error
 * is not retried (CA7).
 */
class AiWorkflowTest {

    static final String VALIDATION = "https://serverlessworkflow.io/spec/1.0.0/errors/validation";
    static final String COMMUNICATION = "https://serverlessworkflow.io/spec/1.0.0/errors/communication";

    static final String SUMMARIZE = """
            do:
              - summarize:
                  call: remote
                  with:
                    task: summarize
                    input:
                      text: ${ .text }
                  export: { as: '${ {summary: .} }' }
            """;

    static final String TWO_STEPS = """
            do:
              - first:
                  call: remote
                  with:
                    task: ask
                    input:
                      question: ${ "first about " + .text }
                  export: { as: '${ {first: .} }' }
              - second:
                  call: remote
                  with:
                    task: ask
                    input:
                      question: what did I ask?
                  export: { as: '${ $context + {second: .} }' }
            """;

    /** The summarize task in a {@code try}; {@code %s} is the catch's body, after its error filter. */
    static final String GUARDED = """
            do:
              - guarded:
                  try:
                    - summarize:
                        call: remote
                        with:
                          task: summarize
                          input:
                            text: ${ .text }
                  catch:
                    errors:
                      with:
                        type: %s
            %s
              - outcome:
                  set: '${ . }'
                  export:
                    as: '${ {result: .} }'
            """;

    static final String FALLBACK = """
            do:
              - fallback:
                  set: '${ {outcome: "fallback", status: .status} }'""";

    static final String RETRY = """
            retry:
              delay: { seconds: 1 }
              limit:
                attempt: { count: 3 }""";

    /** {@link #GUARDED} filtering {@code type}, with {@code body} as the catch's body (indented under it). */
    static String guarded(String type, String body) {
        return GUARDED.formatted(type, body.indent(8).stripTrailing());
    }

    @Test
    void the_structured_output_reaches_the_workflow() throws Exception {
        AiFixture f = new AiFixture(SUMMARIZE, new RuleModel(r -> "{\"topic\":\"synthetic planning\",\"items\":2}"));

        JsonNode context = f.awaitCompleted(f.start(input("text", "synthetic meeting")));

        assertThat(context.path("summary").path("topic").asText()).isEqualTo("synthetic planning");
        assertThat(context.path("summary").path("items").asInt()).isEqualTo(2);
    }

    @Test
    void the_steps_of_an_instance_share_its_memory_and_instances_do_not() throws Exception {
        RuleModel model = RuleModel.echoing();
        AiFixture f = new AiFixture(TWO_STEPS, model);

        String a = f.start(input("text", "apples"));
        JsonNode contextA = f.awaitCompleted(a);
        String b = f.start(input("text", "pears"));
        JsonNode contextB = f.awaitCompleted(b);

        // the second step saw the first step's question and answer of its own instance: 3 messages, never 5
        assertThat(contextA.path("second").asText()).isEqualTo("3:what did I ask?");
        assertThat(contextB.path("second").asText()).isEqualTo("3:what did I ask?");
        assertThat(AiFixture.lastUserText(model.requests.get(1).messages())).isEqualTo("what did I ask?");
        assertThat(model.requests.get(1).messages().toString()).contains("first about apples")
                .doesNotContain("pears");
        assertThat(model.requests.get(3).messages().toString()).contains("first about pears")
                .doesNotContain("apples");
        assertThat(f.conversations.committed(MemoryKey.instance(a)).toCompletableFuture().join())
                .extracting(Exchange::taskId).containsExactly("/do/0/first", "/do/1/second");
    }

    @Test
    void an_answer_out_of_its_type_is_a_validation_error_the_workflow_catches() throws Exception {
        AiFixture f = new AiFixture(guarded(VALIDATION, FALLBACK),
                new RuleModel(r -> "no JSON here, only synthetic prose"));

        JsonNode context = f.awaitCompleted(f.start(input("text", "synthetic meeting")));

        assertThat(context.path("result").path("outcome").asText()).isEqualTo("fallback");
        assertThat(context.path("result").path("status").asInt()).isEqualTo(400);
    }

    @Test
    void uncaught_the_validation_error_fails_the_instance_without_the_answer() throws Exception {
        AiFixture f = new AiFixture(SUMMARIZE, new RuleModel(r -> "no JSON here, only synthetic prose"));

        String failure = f.startFailing(input("text", "synthetic meeting"));

        assertThat(failure).contains("type=" + VALIDATION).contains("status=400").contains("instance=/do/0/summarize")
                .doesNotContain("synthetic prose").doesNotContain("synthetic meeting");
    }

    @Test
    void a_rate_limit_is_retried_no_sooner_than_its_retry_after() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RuleModel model = new RuleModel(r -> {
            if (calls.incrementAndGet() == 1) {
                throw new RateLimitException("synthetic rate limit");
            }
            return "{\"topic\":\"after the limit\",\"items\":1}";
        });
        AiFixture f = new AiFixture(guarded(COMMUNICATION, RETRY), model,
                b -> b.rateLimitRetryAfter(Duration.ofSeconds(30)), ExecutionTracer.NOOP);

        String id = f.start(input("text", "synthetic meeting"));
        TimerRequest retryTimer = f.awaitRetryTimer();

        assertThat(retryTimer.fireAt()).isEqualTo(AiFixture.T0.plusSeconds(30));   // the 429's floor, not the 1 s delay
        f.fire(retryTimer);
        JsonNode context = f.awaitCompleted(id);
        assertThat(context.path("result").path("topic").asText()).isEqualTo("after the limit");
        assertThat(calls).hasValue(2);
    }

    @Test
    void a_credential_error_is_not_retried() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        RuleModel model = new RuleModel(r -> {
            calls.incrementAndGet();
            throw new AuthenticationException("synthetic bad key");
        });
        AiFixture f = new AiFixture(guarded(COMMUNICATION, RETRY), model);

        String failure = f.startFailing(input("text", "synthetic meeting"));

        assertThat(failure).contains("type=https://serverlessworkflow.io/spec/1.0.0/errors/authentication")
                .contains("status=401").doesNotContain("synthetic bad key");
        assertThat(calls).hasValue(1);
        assertThat(f.armed).allSatisfy(t -> assertThat(t.timerId()).startsWith("timer:"));   // task timeouts only
    }
}
