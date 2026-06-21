package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.aws.SqsTaskTransport;
import io.stintflow.core.Json;
import io.stintflow.core.WorkflowEngine;
import io.stintflow.core.WorkflowRegistry;
import io.stintflow.example.BuildReport;
import io.stintflow.example.FormatFileHandler;
import io.stintflow.example.QueryAndStageHandler;
import io.stintflow.inmemory.InMemoryTimerService;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.StateStore;
import io.stintflow.spi.TimerService;
import io.stintflow.worker.TaskHandlerRegistry;
import io.stintflow.worker.WorkerRuntime;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

/**
 * The build-report workflow, run end-to-end against floci with the REAL AWS connectors:
 * S3 (blob/claim-check), DynamoDB (state + GSI), SQS (transport invoke + result queues). This is the
 * "reference running end-to-end" the horizontal-scaling proposal points to — only the broker is local.
 */
class FlociBuildReportIT {

    private static final String BUCKET = "stint-blobs";
    private static final String TABLE = "stint-instances";

    @Test
    void build_report_runs_end_to_end_against_floci() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));

            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create())
                    .forcePathStyle(true).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                // --- provision AWS resources on floci ---
                s3.createBucket(b -> b.bucket(BUCKET));
                createInstancesTable(ddb);
                String invokeUrl = sqs.createQueue(q -> q.queueName("stint-invoke")).queueUrl();
                String resultUrl = sqs.createQueue(q -> q.queueName("stint-result")).queueUrl();

                // --- wire the real AWS connectors ---
                BlobStore blob = new S3BlobStore(s3, BUCKET);
                StateStore state = new DynamoDbStateStore(ddb, TABLE);
                SqsTaskTransport transport = new SqsTaskTransport(sqs, invokeUrl, resultUrl);
                TimerService timer = new InMemoryTimerService();

                // --- worker side (SQS worker binding, prototyped in-process) ---
                TaskHandlerRegistry handlers = new TaskHandlerRegistry()
                        .register(BuildReport.ROUTE_QUERY, new QueryAndStageHandler(blob))
                        .register(BuildReport.ROUTE_FORMAT, new FormatFileHandler(blob));
                WorkerRuntime worker = new WorkerRuntime(handlers, blob);

                try (SqsWorkerRunner ignored = new SqsWorkerRunner(sqs, invokeUrl, resultUrl, worker)) {
                    WorkflowRegistry registry = new WorkflowRegistry();
                    registry.register(BuildReport.definition());
                    WorkflowEngine engine = new WorkflowEngine(registry, transport, state, timer, blob);

                    ObjectNode input = Json.obj();
                    input.put("reportQuery", "SELECT * FROM orders");
                    input.put("outputFormat", "xlsx");

                    JsonNode result = engine.startAndWait(BuildReport.REF, input)
                            .toCompletableFuture().get(90, TimeUnit.SECONDS);

                    assertThat(result.get("file").asText()).contains("final.xlsx");
                    assertThat(result.get("rows").asInt()).isEqualTo(12_000);
                    assertThat(result.get("pointer").asText()).startsWith("s3://" + BUCKET);
                }
            }
        }
    }

    private static void createInstancesTable(DynamoDbClient ddb) {
        ddb.createTable(CreateTableRequest.builder()
                .tableName(TABLE)
                .attributeDefinitions(
                        AttributeDefinition.builder().attributeName("instanceId")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("waitingFor")
                                .attributeType(ScalarAttributeType.S).build())
                .keySchema(KeySchemaElement.builder().attributeName("instanceId").keyType(KeyType.HASH).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder()
                        .indexName("waitingFor-index")
                        .keySchema(KeySchemaElement.builder().attributeName("waitingFor").keyType(KeyType.HASH).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build());
        ddb.waiter().waitUntilTableExists(r -> r.tableName(TABLE));
    }
}
