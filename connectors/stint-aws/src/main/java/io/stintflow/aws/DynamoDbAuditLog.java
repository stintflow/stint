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

import io.stintflow.spi.AuditLog;
import io.stintflow.spi.Sensitivity;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

/**
 * {@link AuditLog} backed by the DynamoDB table {@code stint-audit} (SDD 3a.5, 4.8): partition key {@code instanceId},
 * sort key {@code entry} — the record's epoch milliseconds and a sequence, so an instance's records read back in
 * order — and a numeric {@code expiresAt} (epoch seconds) for the table's native TTL, 400 days by default. The audit
 * trail outlives the instance: it holds ids, levels and counts only, never a value of the data.
 */
@ApplicationScoped
public class DynamoDbAuditLog implements AuditLog {

    public static final Duration DEFAULT_TTL = Duration.ofDays(400);

    private static final AtomicLong SEQUENCE = new AtomicLong();

    @Inject
    DynamoDbClient ddb;

    @ConfigProperty(name = "stint.aws.dynamodb.audit-table", defaultValue = "stint-audit")
    String table;

    @ConfigProperty(name = "stint.aws.dynamodb.audit-ttl-days", defaultValue = "400")
    int ttlDays;

    InstantSource clock = InstantSource.system();

    public DynamoDbAuditLog() {
    }

    /** Test/manual wiring outside CDI. */
    public DynamoDbAuditLog(DynamoDbClient ddb, String table, Duration ttl, InstantSource clock) {
        this.ddb = ddb;
        this.table = table;
        this.ttlDays = (int) ttl.toDays();
        this.clock = clock;
    }

    @Override
    public CompletionStage<Void> append(AuditRecord record) {
        return CompletableFuture.runAsync(() -> {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("instanceId", s(record.instanceId()));
            item.put("entry", s(String.format("%013d#%019d", record.at().toEpochMilli(),
                    SEQUENCE.incrementAndGet())));
            item.put("at", s(record.at().toString()));
            item.put("kind", s(record.kind()));
            item.put("expiresAt", AttributeValue.fromN(String.valueOf(
                    clock.instant().plus(Duration.ofDays(ttlDays)).getEpochSecond())));
            putIfSet(item, "chainId", record.chainId());
            putIfSet(item, "pointer", record.pointer());
            putIfSet(item, "sensitivity", record.sensitivity() == null ? null : record.sensitivity().wireName());
            putIfSet(item, "route", record.route());
            putIfSet(item, "outcome", record.outcome());
            if (!record.detail().isEmpty()) {
                Map<String, AttributeValue> detail = new HashMap<>();
                record.detail().forEach((k, v) -> detail.put(k, s(v)));
                item.put("detail", AttributeValue.fromM(detail));
            }
            ddb.putItem(r -> r.tableName(table).item(item));
        });
    }

    @Override
    public CompletionStage<List<AuditRecord>> of(String instanceId) {
        return CompletableFuture.supplyAsync(() -> {
            List<AuditRecord> records = new ArrayList<>();
            Map<String, AttributeValue> start = null;
            do {
                QueryResponse page = ddb.query(QueryRequest.builder().tableName(table)
                        .keyConditionExpression("instanceId = :id")
                        .expressionAttributeValues(Map.of(":id", s(instanceId)))
                        .scanIndexForward(true).exclusiveStartKey(start).build());
                page.items().forEach(item -> records.add(record(item)));
                start = page.hasLastEvaluatedKey() && !page.lastEvaluatedKey().isEmpty() ? page.lastEvaluatedKey()
                        : null;
            } while (start != null);
            return records;
        });
    }

    private static AuditRecord record(Map<String, AttributeValue> item) {
        Map<String, String> detail = new HashMap<>();
        if (item.containsKey("detail")) {
            item.get("detail").m().forEach((k, v) -> detail.put(k, v.s()));
        }
        String level = text(item, "sensitivity");
        return new AuditRecord(text(item, "instanceId"), text(item, "chainId"), Instant.parse(text(item, "at")),
                text(item, "kind"), text(item, "pointer"),
                level == null ? null : Sensitivity.parseOrRestricted(level), text(item, "route"),
                text(item, "outcome"), detail);
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
