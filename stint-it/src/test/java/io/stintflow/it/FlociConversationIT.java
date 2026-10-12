package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.aws.conversations.DynamoDbConversationStore;
import io.stintflow.it.AiFixture.Chat;
import io.stintflow.it.AiFixture.RuleModel;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;
import io.stintflow.worker.langchain4j.LangChain4jWorker;
import io.stintflow.worker.langchain4j.MemoryScope;
import io.stintflow.worker.langchain4j.memory.CommitOutcome;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.MemoryKey;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * SDD 3a.3, CA2 and CA5/CA10 over a real DynamoDB (floci, dummy credentials {@code test}/{@code test}): the LLM worker's
 * memory in {@link DynamoDbConversationStore} — shared by the steps of an instance, isolated between instances and
 * scopes, committed once per correlationId, read in order, dropped when expired, deleted with its instance.
 */
class FlociConversationIT {

    private static final String TABLE = "stint-conversations";
    private static final Instant T0 = Instant.parse("2026-10-12T12:00:00Z");

    private static <T> T join(CompletionStage<T> stage) {
        return stage.toCompletableFuture().join();
    }

    private static TaskContext task(String instanceId, String taskId, String correlationId) {
        return new TaskContext(taskId, instanceId, correlationId, 1, Json.obj(), new WorkflowRef("ai", "notes", "1.0.0"),
                "chn-" + instanceId, Lineage.NONE);
    }

    private static JsonNode run(TaskHandler handler, TaskContext ctx) {
        return handler.execute(ctx).toCompletableFuture().join();
    }

    @Test
    void the_llm_worker_memory_lives_in_dynamodb() {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                DynamoDbConversationStore.createTable(ddb, TABLE);
                AtomicReference<Instant> now = new AtomicReference<>(T0);
                DynamoDbConversationStore store = new DynamoDbConversationStore(ddb, TABLE, now::get);
                RuleModel model = RuleModel.echoing();
                LangChain4jWorker worker = LangChain4jWorker.builder().chatModel(model).conversations(store)
                        .clock(now::get).retention(Duration.ofDays(30)).build();
                TaskHandler ask = worker.handler(Chat.class, (ai, t) -> ai.reply("step " + t.task().taskId() + " of " + t.task().instanceId()));
                TaskHandler own = worker.service(Chat.class).memory(MemoryScope.TASK)
                        .handler((ai, t) -> ai.reply("own " + t.task().taskId()));

                // CA2: two steps of i-A share the memory; i-B has its own; a task scope sees only itself
                run(ask, task("i-A", "/do/0/first", "c-1"));
                now.set(T0.plusSeconds(1));
                run(ask, task("i-B", "/do/0/first", "c-2"));
                now.set(T0.plusSeconds(2));
                assertThat(run(ask, task("i-A", "/do/1/second", "c-3")).asText()).isEqualTo("3:step /do/1/second of i-A");
                assertThat(model.requests.getLast().messages().toString()).contains("step /do/0/first of i-A")
                        .doesNotContain("of i-B");
                assertThat(run(own, task("i-A", "/do/2/own", "c-4")).asText()).isEqualTo("1:own /do/2/own");

                // CA5: a redelivery is answered from DynamoDB, without the model; a second commit is a no-op
                int calls = model.requests.size();
                assertThat(run(ask, task("i-A", "/do/1/second", "c-3")).asText()).isEqualTo("3:step /do/1/second of i-A");
                assertThat(model.requests).hasSize(calls);
                Exchange again = join(store.committed(MemoryKey.instance("i-A"))).getLast();
                assertThat(join(store.commit(again))).isEqualTo(CommitOutcome.ALREADY_COMMITTED);
                assertThat(join(store.committed(MemoryKey.instance("i-A")))).extracting(Exchange::correlationId)
                        .containsExactly("c-1", "c-3");

                // CA10: retention — expiresAt is the TTL attribute; an expired exchange is no longer read
                assertThat(again.expiresAt()).isEqualTo(T0.plusSeconds(2).plus(Duration.ofDays(30)));
                now.set(T0.plus(Duration.ofDays(30)).plusSeconds(1));
                assertThat(join(store.committed(MemoryKey.instance("i-A")))).extracting(Exchange::correlationId)
                        .containsExactly("c-3");
                now.set(T0.plusSeconds(3));

                // CA10: forgetting i-A deletes every scope of it, and nothing of i-B
                assertThat(join(store.exchangesOf("i-A"))).hasSize(3);
                join(worker.forget("i-A"));
                assertThat(join(store.exchangesOf("i-A"))).isEmpty();
                assertThat(join(store.committed(MemoryKey.instance("i-A")))).isEmpty();
                assertThat(join(store.committed(MemoryKey.task("i-A", "/do/2/own")))).isEmpty();
                assertThat(join(store.committed(MemoryKey.instance("i-B")))).hasSize(1);
            }
        }
    }
}
