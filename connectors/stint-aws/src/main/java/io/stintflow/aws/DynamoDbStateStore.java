package io.stintflow.aws;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.stintflow.core.Json;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
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
 */
@ApplicationScoped
public class DynamoDbStateStore implements StateStore {

    @Inject
    DynamoDbClient ddb;

    @ConfigProperty(name = "stint.aws.dynamodb.instances-table", defaultValue = "stint-instances")
    String instancesTable;

    @ConfigProperty(name = "stint.aws.dynamodb.waits-table", defaultValue = "stint-waits")
    String waitsTable;

    public DynamoDbStateStore() {
    }

    /** Test/manual wiring outside CDI. */
    public DynamoDbStateStore(DynamoDbClient ddb, String instancesTable, String waitsTable) {
        this.ddb = ddb;
        this.instancesTable = instancesTable;
        this.waitsTable = waitsTable;
    }

    @Override
    public CompletionStage<SaveOutcome> save(InstanceSnapshot snapshot, long expectedVersion,
                                              List<Wait> addWaits, List<String> consumeWaitKeys) {
        return CompletableFuture.supplyAsync(() -> {
            List<TransactWriteItem> items = new ArrayList<>();
            items.add(TransactWriteItem.builder().put(instancePut(snapshot, expectedVersion)).build());
            for (Wait toAdd : addWaits) {
                items.add(TransactWriteItem.builder().put(waitPut(toAdd)).build());
            }
            for (String key : consumeWaitKeys) {
                items.add(TransactWriteItem.builder().delete(waitDelete(key)).build());
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
        item.put("updatedAt", AttributeValue.fromN(Long.toString(snap.updatedAt().toEpochMilli())));

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
        return new InstanceSnapshot(
                item.get("instanceId").s(),
                WorkflowRef.parse(item.get("definition").s()),
                item.get("position").s(),
                waitingKey == null ? null : waitingKey.s(),
                Json.read(item.get("context").s().getBytes()),
                InstanceStatus.valueOf(item.get("status").s()),
                Long.parseLong(item.get("version").n()),
                Instant.ofEpochMilli(Long.parseLong(item.get("updatedAt").n())));
    }
}
