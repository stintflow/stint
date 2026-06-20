package io.stintflow.aws;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.stintflow.core.Json;
import io.stintflow.spi.InstanceSnapshot;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.WorkflowRef;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * {@link StateStore} backed by DynamoDB. Partition key {@code instanceId}; a GSI on {@code waitingFor}
 * powers {@link #instanceWaitingFor(String)} — the index the orchestrator uses to resume on a result.
 * <p>
 * The table and GSI must exist (created by IaC or, in tests, by the floci integration setup).
 */
@ApplicationScoped
public class DynamoDbStateStore implements StateStore {

    static final String GSI_WAITING_FOR = "waitingFor-index";

    @Inject
    DynamoDbClient ddb;

    @ConfigProperty(name = "stint.aws.dynamodb.table", defaultValue = "stint-instances")
    String table;

    @Override
    public CompletionStage<Void> save(InstanceSnapshot snap) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("instanceId", AttributeValue.fromS(snap.instanceId()));
            item.put("definition", AttributeValue.fromS(snap.definition().canonical()));
            item.put("position", AttributeValue.fromS(snap.position()));
            if (snap.waitingForCorrelationId() != null) {
                item.put("waitingFor", AttributeValue.fromS(snap.waitingForCorrelationId()));
            }
            item.put("context", AttributeValue.fromS(snap.context().toString()));
            item.put("status", AttributeValue.fromS(snap.status().name()));
            item.put("updatedAt", AttributeValue.fromN(Long.toString(snap.updatedAt().toEpochMilli())));
            ddb.putItem(PutItemRequest.builder().tableName(table).item(item).build());
            return null;
        });
    }

    @Override
    public CompletionStage<Optional<InstanceSnapshot>> load(String instanceId) {
        return CompletableFuture.supplyAsync(() -> {
            Map<String, AttributeValue> item = ddb.getItem(GetItemRequest.builder()
                    .tableName(table)
                    .key(Map.of("instanceId", AttributeValue.fromS(instanceId)))
                    .build()).item();
            if (item == null || item.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(toSnapshot(item));
        });
    }

    @Override
    public CompletionStage<Void> delete(String instanceId) {
        return CompletableFuture.supplyAsync(() -> {
            ddb.deleteItem(DeleteItemRequest.builder()
                    .tableName(table)
                    .key(Map.of("instanceId", AttributeValue.fromS(instanceId)))
                    .build());
            return null;
        });
    }

    @Override
    public CompletionStage<Optional<String>> instanceWaitingFor(String correlationId) {
        return CompletableFuture.supplyAsync(() -> {
            QueryResponse resp = ddb.query(QueryRequest.builder()
                    .tableName(table)
                    .indexName(GSI_WAITING_FOR)
                    .keyConditionExpression("waitingFor = :c")
                    .expressionAttributeValues(Map.of(":c", AttributeValue.fromS(correlationId)))
                    .build());
            if (resp.items().isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(resp.items().get(0).get("instanceId").s());
        });
    }

    private static InstanceSnapshot toSnapshot(Map<String, AttributeValue> item) {
        AttributeValue waitingFor = item.get("waitingFor");
        return new InstanceSnapshot(
                item.get("instanceId").s(),
                WorkflowRef.parse(item.get("definition").s()),
                item.get("position").s(),
                waitingFor == null ? null : waitingFor.s(),
                Json.read(item.get("context").s().getBytes()),
                InstanceStatus.valueOf(item.get("status").s()),
                Instant.ofEpochMilli(Long.parseLong(item.get("updatedAt").n())));
    }
}
