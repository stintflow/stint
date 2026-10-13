package io.stintflow.aws;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.spi.CorrectionLog;
import io.stintflow.spi.Sensitivity;
import io.stintflow.spi.WorkflowRef;
import io.stintflow.wire.Json;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * {@link CorrectionLog} backed by the DynamoDB table {@code stint-corrections} (SDD 3a.4, 4.7): partition key
 * {@code definition} ({@code namespace:name:version}), sort key {@code entry} — the answer's epoch milliseconds and a
 * sequence — and a numeric {@code expiresAt} (epoch seconds) for the table's native TTL, 400 days by default. Pointers,
 * declared names and ids only; the decision and the human answer as JSON.
 */
@ApplicationScoped
public class DynamoDbCorrectionLog implements CorrectionLog {

    public static final Duration DEFAULT_TTL = Duration.ofDays(400);

    private static final AtomicLong SEQUENCE = new AtomicLong();

    @Inject
    DynamoDbClient ddb;

    @ConfigProperty(name = "stint.aws.dynamodb.corrections-table", defaultValue = "stint-corrections")
    String table;

    @ConfigProperty(name = "stint.aws.dynamodb.corrections-ttl-days", defaultValue = "400")
    int ttlDays;

    InstantSource clock = InstantSource.system();

    public DynamoDbCorrectionLog() {
    }

    /** Test/manual wiring outside CDI. */
    public DynamoDbCorrectionLog(DynamoDbClient ddb, String table, Duration ttl, InstantSource clock) {
        this.ddb = ddb;
        this.table = table;
        this.ttlDays = (int) ttl.toDays();
        this.clock = clock;
    }

    @Override
    public CompletionStage<Void> append(Correction c) {
        return CompletableFuture.runAsync(() -> {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("definition", s(c.definition().canonical()));
            Instant at = c.reviewedAt() == null ? clock.instant() : c.reviewedAt();
            item.put("entry", s(String.format("%013d#%019d", at.toEpochMilli(), SEQUENCE.incrementAndGet())));
            item.put("expiresAt", AttributeValue.fromN(String.valueOf(
                    clock.instant().plus(Duration.ofDays(ttlDays)).getEpochSecond())));
            item.put("escalated", AttributeValue.fromBool(c.escalated()));
            putIfSet(item, "instanceId", c.instanceId());
            putIfSet(item, "pointer", c.pointer());
            putIfSet(item, "reviewId", c.reviewId());
            putIfSet(item, "kind", c.kind());
            putIfSet(item, "sensitivity", c.sensitivity() == null ? null : c.sensitivity().wireName());
            putIfSet(item, "materialRef", c.materialRef());
            putIfSet(item, "decision", c.decision() == null ? null : c.decision().toString());
            putIfSet(item, "human", c.human() == null ? null : c.human().toString());
            putIfSet(item, "role", c.role());
            putIfSet(item, "reviewer", c.reviewer());
            putIfSet(item, "requestedAt", c.requestedAt() == null ? null : c.requestedAt().toString());
            putIfSet(item, "reviewedAt", c.reviewedAt() == null ? null : c.reviewedAt().toString());
            ddb.putItem(r -> r.tableName(table).item(item));
        });
    }

    @Override
    public CompletionStage<List<Correction>> of(WorkflowRef definition) {
        return CompletableFuture.supplyAsync(() -> {
            List<Correction> corrections = new ArrayList<>();
            Map<String, AttributeValue> start = null;
            do {
                QueryResponse page = ddb.query(QueryRequest.builder().tableName(table)
                        .keyConditionExpression("definition = :d")
                        .expressionAttributeValues(Map.of(":d", s(definition.canonical())))
                        .scanIndexForward(true).exclusiveStartKey(start).build());
                page.items().forEach(item -> corrections.add(correction(definition, item)));
                start = page.hasLastEvaluatedKey() && !page.lastEvaluatedKey().isEmpty() ? page.lastEvaluatedKey()
                        : null;
            } while (start != null);
            return corrections;
        });
    }

    private static Correction correction(WorkflowRef definition, Map<String, AttributeValue> item) {
        String level = text(item, "sensitivity");
        return new Correction(definition, text(item, "instanceId"), text(item, "pointer"), text(item, "reviewId"),
                text(item, "kind"), level == null ? null : Sensitivity.parseOrRestricted(level),
                text(item, "materialRef"), json(text(item, "decision")), json(text(item, "human")),
                text(item, "role"), text(item, "reviewer"), instant(text(item, "requestedAt")),
                instant(text(item, "reviewedAt")), item.containsKey("escalated") && item.get("escalated").bool());
    }

    private static JsonNode json(String text) {
        return text == null ? null : Json.read(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static Instant instant(String text) {
        return text == null ? null : Instant.parse(text);
    }

    private static String text(Map<String, AttributeValue> item, String name) {
        AttributeValue value = item.get(name);
        return value == null ? null : value.s();
    }

    private static void putIfSet(Map<String, AttributeValue> item, String name, String value) {
        if (value != null) {
            item.put(name, s(value));
        }
    }

    private static AttributeValue s(String value) {
        return AttributeValue.fromS(value);
    }
}
