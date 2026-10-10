package io.stintflow.it;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * SDD 1.2: provisions the two-table {@code DynamoDbStateStore} schema on floci for integration
 * tests — {@code stint-instances} (PK {@code instanceId}) and {@code stint-waits} (PK {@code waitKey},
 * plus a GSI by {@code instanceId} reserved for a future orphan-cleanup sweep; never read on the
 * resume path).
 */
final class DynamoDbTestTables {

    private DynamoDbTestTables() {
    }

    static void create(DynamoDbClient ddb, String instancesTable, String waitsTable) {
        ddb.createTable(CreateTableRequest.builder()
                .tableName(instancesTable)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("instanceId")
                                .attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName("instanceId").keyType(KeyType.HASH).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        ddb.waiter().waitUntilTableExists(r -> r.tableName(instancesTable));

        ddb.createTable(CreateTableRequest.builder()
                .tableName(waitsTable)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("waitKey")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("instanceId")
                                .attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName("waitKey").keyType(KeyType.HASH).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName("byInstance-index")
                        .keySchema(KeySchemaElement.builder().attributeName("instanceId").keyType(KeyType.HASH).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        ddb.waiter().waitUntilTableExists(r -> r.tableName(waitsTable));
    }

    /**
     * SDD 2.2, sec. 8c: {@code stint-outbox} (PK {@code eventId}) with the {@code pending-by-age} GSI
     * (PK {@code shard}, SK {@code createdAt}) the sweep queries — never scanned.
     */
    static void createOutbox(DynamoDbClient ddb, String outboxTable) {
        ddb.createTable(CreateTableRequest.builder()
                .tableName(outboxTable)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("eventId").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("shard").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("createdAt").attributeType(ScalarAttributeType.N).build())
                .keySchema(KeySchemaElement.builder().attributeName("eventId").keyType(KeyType.HASH).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName("pending-by-age")
                        .keySchema(
                                KeySchemaElement.builder().attributeName("shard").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("createdAt").keyType(KeyType.RANGE).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        ddb.waiter().waitUntilTableExists(r -> r.tableName(outboxTable));
    }

    /** SDD 2.3, sec. 8c: {@code stint-inbox} (PK {@code waitKey}, SK {@code eventId}) for early events. */
    static void createInbox(DynamoDbClient ddb, String inboxTable) {
        ddb.createTable(CreateTableRequest.builder()
                .tableName(inboxTable)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("waitKey").attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("eventId").attributeType(ScalarAttributeType.S).build())
                .keySchema(
                        KeySchemaElement.builder().attributeName("waitKey").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("eventId").keyType(KeyType.RANGE).build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        ddb.waiter().waitUntilTableExists(r -> r.tableName(inboxTable));
    }
}
