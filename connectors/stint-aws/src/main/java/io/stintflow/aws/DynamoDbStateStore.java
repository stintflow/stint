package io.stintflow.aws;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.wire.Json;
import io.stintflow.spi.ErrorInfo;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.Lineage;
import io.stintflow.spi.OutboxEntry;
import io.stintflow.spi.RetryState;
import io.stintflow.spi.SaveOutcome;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.Wait;
import io.stintflow.spi.WorkflowRef;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * {@link StateStore} backed by two DynamoDB tables: {@code stint-instances} (PK {@code instanceId})
 * and {@code stint-waits} (PK {@code waitKey}, plus a GSI by {@code instanceId} for an eventual
 * orphan-cleanup sweep — never read on the resume path, see {@link #findWait}).
 * <p>
 * {@link #save} is one {@code TransactWriteItems}: a conditional {@code Put} on the instance item
 * (condition on {@code version}, or {@code attribute_not_exists} for a brand-new instance) plus a
 * conditional {@code Put} per {@code addWaits} ({@code attribute_not_exists}) and a conditional
 * {@code Delete} per {@code consumeWaitKeys} ({@code attribute_exists}). Any condition failing
 * cancels the whole transaction — confirmed against floci (SDD 1.2 sec. 8c).
 * <p>
 * SDD 2.2, sec. 8c: the outbox is a third table, {@code stint-outbox} (PK {@code eventId}), written by
 * a conditional {@code Put} in the same transaction. Published facts are deleted, so the table — and
 * its GSI {@code pending-by-age} (PK {@code shard}, SK {@code createdAt}) — only ever hold pending
 * facts: {@link #pendingOutbox} is one {@code Query} per shard, never a scan. No TTL: expiring a pending
 * item would lose an unpublished fact.
 */
@ApplicationScoped
public class DynamoDbStateStore implements StateStore {

    @Inject
    DynamoDbClient ddb;

    @ConfigProperty(name = "stint.aws.dynamodb.instances-table", defaultValue = "stint-instances")
    String instancesTable;

    @ConfigProperty(name = "stint.aws.dynamodb.waits-table", defaultValue = "stint-waits")
    String waitsTable;

    @ConfigProperty(name = "stint.aws.dynamodb.outbox-table", defaultValue = "stint-outbox")
    String outboxTable;

    @ConfigProperty(name = "stint.aws.dynamodb.outbox-shards", defaultValue = "4")
    int outboxShards;

    static final String OUTBOX_INDEX = "pending-by-age";

    public DynamoDbStateStore() {
    }

    /** Test/manual wiring outside CDI. */
    public DynamoDbStateStore(DynamoDbClient ddb, String instancesTable, String waitsTable) {
        this(ddb, instancesTable, waitsTable, "stint-outbox", 4);
    }

    /** Test/manual wiring outside CDI, with the SDD 2.2 outbox table. */
    public DynamoDbStateStore(DynamoDbClient ddb, String instancesTable, String waitsTable, String outboxTable,
                              int outboxShards) {
        this.ddb = ddb;
        this.instancesTable = instancesTable;
        this.waitsTable = waitsTable;
        this.outboxTable = outboxTable;
        this.outboxShards = outboxShards;
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                              List<Wait> addWaits, List<String> consumeWaitKeys,
                                              List<OutboxEntry> addOutbox) {
        return CompletableFuture.supplyAsync(() -> {
            List<TransactWriteItem> items = new ArrayList<>();
            items.add(TransactWriteItem.builder().put(instancePut(snapshot, expectedVersion)).build());
            for (Wait toAdd : addWaits) {
                items.add(TransactWriteItem.builder().put(waitPut(toAdd)).build());
            }
            for (String key : consumeWaitKeys) {
                items.add(TransactWriteItem.builder().delete(waitDelete(key)).build());
            }
            for (OutboxEntry entry : addOutbox) {
                items.add(TransactWriteItem.builder().put(outboxPut(entry)).build());
            }
            try {
                ddb.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(items).build());
                return SaveOutcome.OK;
            } catch (TransactionCanceledException e) {
                return SaveOutcome.CONFLICT;
            }
        });
    }

    private Put instancePut(InstanceSnapshot snap, long expectedVersion) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("instanceId", AttributeValue.fromS(snap.instanceId()));
        item.put("definition", AttributeValue.fromS(snap.definition().canonical()));
        item.put("position", AttributeValue.fromS(snap.position()));
        if (snap.waitingKey() != null) {
            item.put("waitingKey", AttributeValue.fromS(snap.waitingKey()));
        }
        item.put("context", AttributeValue.fromS(snap.context().toString()));
        item.put("status", AttributeValue.fromS(snap.status().name()));
        item.put("version", AttributeValue.fromN(Long.toString(snap.version())));
        if (snap.retryState() != null) {
            item.put("retryState", AttributeValue.fromS(retryStateToJson(snap.retryState()).toString()));
        }
        item.put("updatedAt", AttributeValue.fromN(Long.toString(snap.updatedAt().toEpochMilli())));
        if (snap.input() != null) {
            item.put("input", AttributeValue.fromS(snap.input().toString()));
        }
        if (snap.startedAt() != null) {
            item.put("startedAt", AttributeValue.fromN(Long.toString(snap.startedAt().toEpochMilli())));
        }
        // SDD 2.5: the chain and the suspending activation's trace context (optional attributes).
        putIfSet(item, "chainId", snap.chainId());
        if (snap.suspendedAt() != null) {
            putIfSet(item, "suspendedTraceparent", snap.suspendedAt().traceparent());
            putIfSet(item, "suspendedTracestate", snap.suspendedAt().tracestate());
        }

        Put.Builder builder = Put.builder().tableName(instancesTable).item(item);
        if (expectedVersion == 0) {
            builder.conditionExpression("attribute_not_exists(instanceId)");
        } else {
            builder.conditionExpression("version = :expected")
                    .expressionAttributeValues(
                            Map.of(":expected", AttributeValue.fromN(Long.toString(expectedVersion))));
        }
        return builder.build();
    }

    private Put waitPut(Wait toAdd) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("waitKey", AttributeValue.fromS(toAdd.waitKey()));
        item.put("instanceId", AttributeValue.fromS(toAdd.instanceId()));
        item.put("position", AttributeValue.fromS(toAdd.position()));
        item.put("createdAt", AttributeValue.fromN(Long.toString(toAdd.createdAt().toEpochMilli())));
        return Put.builder().tableName(waitsTable).item(item)
                .conditionExpression("attribute_not_exists(waitKey)")
                .build();
    }

    private Put outboxPut(OutboxEntry entry) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("eventId", AttributeValue.fromS(entry.eventId()));
        item.put("instanceId", AttributeValue.fromS(entry.instanceId()));
        item.put("seq", AttributeValue.fromN(Integer.toString(entry.seq())));
        item.put("event", AttributeValue.fromS(entry.event()));
        item.put("createdAt", AttributeValue.fromN(Long.toString(entry.createdAt().toEpochMilli())));
        item.put("shard", AttributeValue.fromS(shardOf(entry.eventId())));
        return Put.builder().tableName(outboxTable).item(item)
                .conditionExpression("attribute_not_exists(eventId)")
                .build();
    }

    private String shardOf(String eventId) {
        return Integer.toString(Math.floorMod(eventId.hashCode(), outboxShards));
    }

    @Override
    public CompletionStage<List<OutboxEntry>> pendingOutbox(Instant createdAtOrBefore, int limit) {
        return CompletableFuture.supplyAsync(() -> {
            List<OutboxEntry> pending = new ArrayList<>();
            for (int shard = 0; shard < outboxShards; shard++) {
                var response = ddb.query(QueryRequest.builder()
                        .tableName(outboxTable)
                        .indexName(OUTBOX_INDEX)
                        .keyConditionExpression("#shard = :shard AND createdAt <= :cutoff")
                        .expressionAttributeNames(Map.of("#shard", "shard"))
                        .expressionAttributeValues(Map.of(
                                ":shard", AttributeValue.fromS(Integer.toString(shard)),
                                ":cutoff", AttributeValue.fromN(Long.toString(createdAtOrBefore.toEpochMilli()))))
                        .limit(limit)
                        .build());
                for (Map<String, AttributeValue> item : response.items()) {
                    pending.add(new OutboxEntry(
                            item.get("eventId").s(),
                            item.get("instanceId").s(),
                            Integer.parseInt(item.get("seq").n()),
                            item.get("event").s(),
                            Instant.ofEpochMilli(Long.parseLong(item.get("createdAt").n()))));
                }
            }
            return pending.stream()
                    .sorted(Comparator.comparing(OutboxEntry::createdAt).thenComparingInt(OutboxEntry::seq)
                            .thenComparing(OutboxEntry::eventId))
                    .limit(limit)
                    .toList();
        });
    }

    @Override
    public CompletionStage<Void> removeOutbox(String eventId) {
        return CompletableFuture.supplyAsync(() -> {
            ddb.deleteItem(DeleteItemRequest.builder()
                    .tableName(outboxTable)
                    .key(Map.of("eventId", AttributeValue.fromS(eventId)))
                    .build());
            return null;
        });
    }

    private Delete waitDelete(String waitKey) {
        return Delete.builder().tableName(waitsTable)
                .key(Map.of("waitKey", AttributeValue.fromS(waitKey)))
                .conditionExpression("attribute_exists(waitKey)")
                .build();
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, AttributeValue> item = ddb.getItem(GetItemRequest.builder()
                    .tableName(instancesTable)
                    .key(Map.of("instanceId", AttributeValue.fromS(instanceId)))
                    .build()).item();
            if (item == null || item.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(toSnapshot(item));
        });
    }

    @Override
    public CompletionStage<Optional<Wait>> findWait(String waitKey) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, AttributeValue> item = ddb.getItem(GetItemRequest.builder()
                    .tableName(waitsTable)
                    .key(Map.of("waitKey", AttributeValue.fromS(waitKey)))
                    .build()).item();
            if (item == null || item.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new Wait(
                    item.get("waitKey").s(),
                    item.get("instanceId").s(),
                    item.get("position").s(),
                    Instant.ofEpochMilli(Long.parseLong(item.get("createdAt").n()))));
        });
    }

    @Override
    public CompletionStage<Void> delete(String instanceId) {
        return CompletableFuture.supplyAsync(() -> {
            ddb.deleteItem(DeleteItemRequest.builder()
                    .tableName(instancesTable)
                    .key(Map.of("instanceId", AttributeValue.fromS(instanceId)))
                    .build());
            return null;
        });
    }

    private static InstanceSnapshot toSnapshot(Map<String, AttributeValue> item) {
        AttributeValue waitingKey = item.get("waitingKey");
        AttributeValue retryState = item.get("retryState");
        // SDD 2.1, RF6: tolerant read — items written before SDD 2.1 have neither attribute.
        AttributeValue input = item.get("input");
        AttributeValue startedAt = item.get("startedAt");
        // SDD 2.5, tolerant read: items written before SDD 2.5 have no chain or trace attributes.
        AttributeValue chainId = item.get("chainId");
        AttributeValue traceparent = item.get("suspendedTraceparent");
        AttributeValue tracestate = item.get("suspendedTracestate");
        return new InstanceSnapshot(
                item.get("instanceId").s(),
                WorkflowRef.parse(item.get("definition").s()),
                item.get("position").s(),
                waitingKey == null ? null : waitingKey.s(),
                Json.read(item.get("context").s().getBytes()),
                InstanceStatus.valueOf(item.get("status").s()),
                Long.parseLong(item.get("version").n()),
                retryState == null ? null : retryStateFromJson(Json.read(retryState.s().getBytes())),
                Instant.ofEpochMilli(Long.parseLong(item.get("updatedAt").n())),
                input == null ? null : Json.read(input.s().getBytes()),
                startedAt == null ? null : Instant.ofEpochMilli(Long.parseLong(startedAt.n())),
                chainId == null ? null : chainId.s(),
                traceparent == null ? null : Lineage.trace(traceparent.s(), tracestate == null ? null : tracestate.s()));
    }

    private static void putIfSet(Map<String, AttributeValue> item, String name, String value) {
        if (value != null && !value.isBlank()) {
            item.put(name, AttributeValue.fromS(value));
        }
    }

    private static ObjectNode retryStateToJson(RetryState state) {
        ObjectNode node = Json.obj();
        node.put("tryNodePointer", state.tryNodePointer());
        node.put("attempt", state.attempt());
        node.put("firstAttemptAt", state.firstAttemptAt().toEpochMilli());
        if (state.currentCorrelationId() != null) {
            node.put("currentCorrelationId", state.currentCorrelationId());
        }
        if (state.lastError() != null) {
            node.set("lastError", errorToJson(state.lastError()));
        }
        return node;
    }

    private static RetryState retryStateFromJson(JsonNode node) {
        ErrorInfo lastError = node.hasNonNull("lastError") ? errorFromJson(node.get("lastError")) : null;
        return new RetryState(
                node.get("tryNodePointer").asText(),
                node.get("attempt").asInt(),
                Instant.ofEpochMilli(node.get("firstAttemptAt").asLong()),
                lastError,
                node.hasNonNull("currentCorrelationId") ? node.get("currentCorrelationId").asText() : null);
    }

    private static ObjectNode errorToJson(ErrorInfo error) {
        ObjectNode node = Json.obj();
        node.put("type", error.type().toString());
        if (error.status() != null) {
            node.put("status", error.status());
        }
        if (error.title() != null) {
            node.put("title", error.title());
        }
        if (error.detail() != null) {
            node.put("detail", error.detail());
        }
        if (error.instance() != null) {
            node.put("instance", error.instance());
        }
        if (error.retryAfter() != null) {
            node.put("retryAfter", error.retryAfter().toString());
        }
        return node;
    }

    private static ErrorInfo errorFromJson(JsonNode node) {
        return new ErrorInfo(
                java.net.URI.create(node.get("type").asText()),
                node.hasNonNull("status") ? node.get("status").asInt() : null,
                node.hasNonNull("title") ? node.get("title").asText() : null,
                node.hasNonNull("detail") ? node.get("detail").asText() : null,
                node.hasNonNull("instance") ? node.get("instance").asText() : null,
                node.hasNonNull("retryAfter") ? java.time.Duration.parse(node.get("retryAfter").asText()) : null);
    }
}
