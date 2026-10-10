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
}
