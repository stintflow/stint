package io.stintflow.example.smoke;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.Target;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

/**
 * Starts floci, creates what the smoke app needs, and points every Quarkus AWS client at it (SDD 2.7). The same
 * configuration reaches the JVM jar and the native binary that {@link AwsSmokeIT} runs.
 */
public class FlociResource implements QuarkusTestResourceLifecycleManager {

    static volatile URI endpoint;
    static volatile String factsQueue;

    private GenericContainer<?> floci;

    @Override
    public Map<String, String> start() {
        floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest")).withExposedPorts(4566);
        floci.start();
        endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));

        Map<String, String> config = new HashMap<>();
        // The EventBridge Scheduler extension's prefix is "eventbridge-scheduler" ("scheduler" is Quarkus's own
        // scheduling extension): with the wrong prefix its client would silently fall back to dev services/defaults.
        for (String service : List.of("sqs", "dynamodb", "s3", "eventbridge", "eventbridge-scheduler")) {
            config.put("quarkus." + service + ".endpoint-override", endpoint.toString());
            config.put("quarkus." + service + ".aws.region", "us-east-1");
            config.put("quarkus." + service + ".aws.credentials.type", "static");
            config.put("quarkus." + service + ".aws.credentials.static-provider.access-key-id", "test");
            config.put("quarkus." + service + ".aws.credentials.static-provider.secret-access-key", "test");
        }
        config.put("quarkus.s3.path-style-access", "true");
        // Fail-safe: a client whose configuration is ignored must never reach a real account through the machine's
        // default credentials chain (~/.aws), nor start its own dev services container. System properties win in the
        // SDK's default chain, so these fake values are what any misconfigured client would use.
        config.put("quarkus.devservices.enabled", "false");
        config.put("aws.accessKeyId", "test");
        config.put("aws.secretAccessKey", "test");
        config.put("aws.region", "us-east-1");

        try (SqsClient sqs = SqsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                .credentialsProvider(creds()).httpClient(UrlConnectionHttpClient.create()).build();
             DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                     .credentialsProvider(creds()).httpClient(UrlConnectionHttpClient.create()).build();
             S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1).forcePathStyle(true)
                     .credentialsProvider(creds()).httpClient(UrlConnectionHttpClient.create()).build();
             EventBridgeClient events = EventBridgeClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                     .credentialsProvider(creds()).httpClient(UrlConnectionHttpClient.create()).build()) {

            config.put("smoke.sqs.invoke-queue-url", queue(sqs, "smoke-invoke"));
            config.put("smoke.sqs.result-queue-url", queue(sqs, "smoke-result"));
            String timers = queue(sqs, "stint-timers");
            String schedules = queue(sqs, "stint-schedules");
            factsQueue = queue(sqs, "smoke-facts");
            config.put("smoke.sqs.timer-queue-url", timers);
            config.put("smoke.sqs.schedule-queue-url", schedules);
            config.put("smoke.scheduler.timer-target-arn", arn(sqs, timers));
            config.put("smoke.scheduler.schedule-target-arn", arn(sqs, schedules));

            events.createEventBus(b -> b.name("stint-domain"));
            events.putRule(r -> r.name("smoke-facts").eventBusName("stint-domain")
                    .eventPattern("{\"detail-type\":[\"io.stintflow.smoke.done.v1\"]}"));
            events.putTargets(t -> t.rule("smoke-facts").eventBusName("stint-domain")
                    .targets(Target.builder().id("facts").arn(arn(sqs, factsQueue)).inputPath("$.detail").build()));

            s3.createBucket(b -> b.bucket("stint-smoke"));

            table(ddb, "stint-instances", "instanceId", null);
            table(ddb, "stint-waits", "waitKey", null);
            table(ddb, "stint-inbox", "waitKey", "eventId");
            ddb.createTable(r -> r.tableName("stint-outbox").billingMode(BillingMode.PAY_PER_REQUEST)
                    .attributeDefinitions(attr("eventId", ScalarAttributeType.S), attr("shard", ScalarAttributeType.S),
                            attr("createdAt", ScalarAttributeType.N))
                    .keySchema(key("eventId", KeyType.HASH))
                    .globalSecondaryIndexes(GlobalSecondaryIndex.builder().indexName("pending-by-age")
                            .keySchema(key("shard", KeyType.HASH), key("createdAt", KeyType.RANGE))
                            .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build()));
        }
        return config;
    }

    @Override
    public void stop() {
        if (floci != null) {
            floci.stop();
        }
    }

    static StaticCredentialsProvider creds() {
        return StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
    }

    private static String queue(SqsClient sqs, String name) {
        return sqs.createQueue(q -> q.queueName(name)).queueUrl();
    }

    private static String arn(SqsClient sqs, String url) {
        return sqs.getQueueAttributes(r -> r.queueUrl(url).attributeNames(QueueAttributeName.QUEUE_ARN))
                .attributes().get(QueueAttributeName.QUEUE_ARN);
    }

    private static void table(DynamoDbClient ddb, String name, String hash, String range) {
        ddb.createTable(r -> {
            r.tableName(name).billingMode(BillingMode.PAY_PER_REQUEST);
            if (range == null) {
                r.attributeDefinitions(attr(hash, ScalarAttributeType.S)).keySchema(key(hash, KeyType.HASH));
            } else {
                r.attributeDefinitions(attr(hash, ScalarAttributeType.S), attr(range, ScalarAttributeType.S))
                        .keySchema(key(hash, KeyType.HASH), key(range, KeyType.RANGE));
            }
        });
    }

    private static AttributeDefinition attr(String name, ScalarAttributeType type) {
        return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }

    private static KeySchemaElement key(String name, KeyType type) {
        return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
}
