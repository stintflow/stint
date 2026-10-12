package io.stintflow.aws.conversations;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.wire.Json;
import io.stintflow.worker.langchain4j.memory.CommitOutcome;
import io.stintflow.worker.langchain4j.memory.ConversationStore;
import io.stintflow.worker.langchain4j.memory.Exchange;
import io.stintflow.worker.langchain4j.memory.MemoryKey;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * The DynamoDB {@link ConversationStore} (SDD 3a.3, 5.3). One table:
 * <ul>
 *   <li>partition key {@code memory} (S): {@code instanceId#scope} — {@code instanceId#} for the whole instance;</li>
 *   <li>sort key {@code item} (S): {@code x#<committedAt, epoch millis, 20 digits>#<correlationId>} for an exchange,
 *       {@code c#<correlationId>} for the marker that makes a correlationId's commit happen once;</li>
 *   <li>{@code instanceId} (S) on every item, the key of the global secondary index {@value #BY_INSTANCE} that finds
 *       every scope of an instance;</li>
 *   <li>{@code expiresAt} (N, epoch seconds), the table's native TTL attribute (like the inbox of SDD 2.3). DynamoDB
 *       deletes expired items lazily, so reads drop them too.</li>
 * </ul>
 * A commit is one transaction: the marker, conditional on not existing, and the exchange. Synchronous calls on the
 * caller's thread, like {@code DynamoDbStateStore}.
 */
public final class DynamoDbConversationStore implements ConversationStore {

    public static final String BY_INSTANCE = "by-instance";

    private static final String MEMORY = "memory";
    private static final String ITEM = "item";
    private static final String INSTANCE_ID = "instanceId";
    private static final String SCOPE = "scope";
    private static final String CORRELATION_ID = "correlationId";
    private static final String TASK_ID = "taskId";
    private static final String ATTEMPT = "attempt";
    private static final String COMMITTED_AT = "committedAt";
    private static final String MESSAGES = "messages";
    private static final String OUTPUT = "output";
    private static final String EXPIRES_AT = "expiresAt";
    private static final String EXCHANGE_PREFIX = "x#";
    private static final String MARKER_PREFIX = "c#";

    private final DynamoDbClient ddb;
    private final String table;
    private final InstantSource clock;

    public DynamoDbConversationStore(DynamoDbClient ddb, String table) {
        this(ddb, table, InstantSource.system());
    }

    public DynamoDbConversationStore(DynamoDbClient ddb, String table, InstantSource clock) {
        this.ddb = ddb;
        this.table = table;
        this.clock = clock;
    }

    /** Creates the table, its index and its TTL — for tests and local setups; production provisions it as code. */
    public static void createTable(DynamoDbClient ddb, String table) {
        ddb.createTable(CreateTableRequest.builder()
                .tableName(table)
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName(MEMORY).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(ITEM).attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName(INSTANCE_ID).attributeType(ScalarAttributeType.S)
                                .build())
                .keySchema(
                        KeySchemaElement.builder().attributeName(MEMORY).keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName(ITEM).keyType(KeyType.RANGE).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName(BY_INSTANCE)
                        .keySchema(KeySchemaElement.builder().attributeName(INSTANCE_ID).keyType(KeyType.HASH).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build())
                .build());
        ddb.updateTimeToLive(r -> r.tableName(table).timeToLiveSpecification(
                TimeToLiveSpecification.builder().enabled(true).attributeName(EXPIRES_AT).build()));
    }

    @Override
    public CompletionStage<List<Exchange>> committed(MemoryKey memory) {
        return supply(() -> {
            List<Exchange> exchanges = new ArrayList<>();
            for (Map<String, AttributeValue> item : query(QueryRequest.builder()
                    .tableName(table)
                    .consistentRead(true)
                    .keyConditionExpression("#m = :m AND begins_with(#i, :x)")
                    .expressionAttributeNames(Map.of("#m", MEMORY, "#i", ITEM))
                    .expressionAttributeValues(Map.of(":m", s(partition(memory)), ":x", s(EXCHANGE_PREFIX)))
                    .build())) {
                Exchange exchange = exchange(item);
                if (exchange.expiresAt() == null || exchange.expiresAt().isAfter(clock.instant())) {
                    exchanges.add(exchange);
                }
            }
            return exchanges;   // the sort key orders them by committedAt, then correlationId
        });
    }

    @Override
    public CompletionStage<CommitOutcome> commit(Exchange exchange) {
        return supply(() -> {
            String partition = partition(exchange.memory());
            Map<String, AttributeValue> marker = new HashMap<>();
            marker.put(MEMORY, s(partition));
            marker.put(ITEM, s(MARKER_PREFIX + exchange.correlationId()));
            marker.put(INSTANCE_ID, s(exchange.memory().instanceId()));
            if (exchange.expiresAt() != null) {
                marker.put(EXPIRES_AT, n(exchange.expiresAt().getEpochSecond()));
            }
            Map<String, AttributeValue> item = new HashMap<>(marker);
            item.put(ITEM, s(String.format("%s%020d#%s", EXCHANGE_PREFIX, exchange.committedAt().toEpochMilli(),
                    exchange.correlationId())));
            item.put(SCOPE, s(exchange.memory().scope()));
            item.put(CORRELATION_ID, s(exchange.correlationId()));
            item.put(TASK_ID, s(exchange.taskId()));
            item.put(ATTEMPT, n(exchange.attempt()));
            item.put(COMMITTED_AT, n(exchange.committedAt().toEpochMilli()));
            item.put(MESSAGES, s(text(exchange.messages())));
            if (exchange.output() != null) {
                item.put(OUTPUT, s(text(exchange.output())));
            }
            try {
                ddb.transactWriteItems(r -> r.transactItems(
                        TransactWriteItem.builder().put(Put.builder().tableName(table).item(marker)
                                .conditionExpression("attribute_not_exists(#i)")
                                .expressionAttributeNames(Map.of("#i", ITEM)).build()).build(),
                        TransactWriteItem.builder().put(Put.builder().tableName(table).item(item).build()).build()));
                return CommitOutcome.COMMITTED;
            } catch (TransactionCanceledException e) {
                if (e.hasCancellationReasons() && e.cancellationReasons().stream().map(CancellationReason::code)
                        .anyMatch("ConditionalCheckFailed"::equals)) {
                    return CommitOutcome.ALREADY_COMMITTED;
                }
                throw e;
            }
        });
    }

    @Override
    public CompletionStage<List<Exchange>> exchangesOf(String instanceId) {
        return supply(() -> {
            List<Exchange> exchanges = new ArrayList<>();
            for (Map<String, AttributeValue> item : byInstance(instanceId)) {
                if (item.get(ITEM).s().startsWith(EXCHANGE_PREFIX)) {
                    exchanges.add(exchange(item));
                }
            }
            exchanges.sort(Comparator.comparing(Exchange::committedAt).thenComparing(Exchange::correlationId));
            return exchanges;
        });
    }

    /** Through the index, which is eventually consistent: an exchange committed a moment ago may survive a call. */
    @Override
    public CompletionStage<Void> deleteInstance(String instanceId) {
        return supply(() -> {
            List<WriteRequest> deletes = new ArrayList<>();
            for (Map<String, AttributeValue> item : byInstance(instanceId)) {
                deletes.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder()
                        .key(Map.of(MEMORY, item.get(MEMORY), ITEM, item.get(ITEM))).build()).build());
            }
            for (int from = 0; from < deletes.size(); from += 25) {
                List<WriteRequest> batch = deletes.subList(from, Math.min(from + 25, deletes.size()));
                Map<String, List<WriteRequest>> pending = Map.of(table, batch);
                while (!pending.isEmpty()) {
                    Map<String, List<WriteRequest>> request = pending;
                    BatchWriteItemResponse response = ddb.batchWriteItem(r -> r.requestItems(request));
                    pending = response.hasUnprocessedItems() ? response.unprocessedItems() : Map.of();
                }
            }
            return null;
        });
    }

    private List<Map<String, AttributeValue>> byInstance(String instanceId) {
        return query(QueryRequest.builder()
                .tableName(table)
                .indexName(BY_INSTANCE)
                .keyConditionExpression("#n = :n")
                .expressionAttributeNames(Map.of("#n", INSTANCE_ID))
                .expressionAttributeValues(Map.of(":n", s(instanceId)))
                .build());
    }

    private List<Map<String, AttributeValue>> query(QueryRequest first) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        QueryRequest request = first;
        while (true) {
            QueryResponse response = ddb.query(request);
            items.addAll(response.items());
            if (!response.hasLastEvaluatedKey() || response.lastEvaluatedKey().isEmpty()) {
                return items;
            }
            request = request.toBuilder().exclusiveStartKey(response.lastEvaluatedKey()).build();
        }
    }

    private static Exchange exchange(Map<String, AttributeValue> item) {
        MemoryKey memory = new MemoryKey(item.get(INSTANCE_ID).s(), item.get(SCOPE).s());
        return new Exchange(memory, item.get(CORRELATION_ID).s(), item.get(TASK_ID).s(),
                Integer.parseInt(item.get(ATTEMPT).n()), Instant.ofEpochMilli(Long.parseLong(item.get(COMMITTED_AT).n())),
                json(item.get(MESSAGES)), json(item.get(OUTPUT)),
                item.containsKey(EXPIRES_AT) ? Instant.ofEpochSecond(Long.parseLong(item.get(EXPIRES_AT).n())) : null);
    }

    private static String partition(MemoryKey memory) {
        return memory.instanceId() + "#" + memory.scope();
    }

    private static JsonNode json(AttributeValue value) {
        return value == null ? null : Json.read(value.s().getBytes(StandardCharsets.UTF_8));
    }

    private static String text(JsonNode node) {
        return new String(Json.bytes(node), StandardCharsets.UTF_8);
    }

    private static AttributeValue s(String value) {
        return AttributeValue.fromS(value);
    }

    private static AttributeValue n(long value) {
        return AttributeValue.fromN(Long.toString(value));
    }

    private static <T> CompletionStage<T> supply(Supplier<T> work) {
        try {
            return CompletableFuture.completedFuture(work.get());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
