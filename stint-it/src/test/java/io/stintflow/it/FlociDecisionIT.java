package io.stintflow.it;

import java.net.URI;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbStateStore;
import io.stintflow.spi.InstanceSnapshot.InstanceStatus;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * SDD 3a.1, CA3 over a real DynamoDB (floci, dummy credentials {@code test}/{@code test}): the decision record is
 * written by the engine through {@link DynamoDbStateStore} and read back from the table.
 */
class FlociDecisionIT {

    private static final String INSTANCES_TABLE = "stint-instances";
    private static final String WAITS_TABLE = "stint-waits";

    @Test
    void the_decision_record_is_persisted_by_the_dynamodb_state_store() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                DynamoDbTestTables.create(ddb, INSTANCES_TABLE, WAITS_TABLE);
                DynamoDbStateStore store = new DynamoDbStateStore(ddb, INSTANCES_TABLE, WAITS_TABLE);
                DecisionFixture f = new DecisionFixture(DecisionProvenanceTest.REPLACES_CONTEXT, store);

                String id = f.start("item: aprovado");
                f.awaitSaved(s -> s.instanceId().equals(id) && s.status() == InstanceStatus.WAITING
                        && s.position().equals("/do/1/wait"));

                // Read straight from the table, through a store that shares nothing with the engine's.
                var reloaded = new DynamoDbStateStore(ddb, INSTANCES_TABLE, WAITS_TABLE).load(id)
                        .toCompletableFuture().get().orElseThrow();
                DecisionProvenanceTest.assertProvenance(reloaded.decisions().get("/do/0/classify"),
                        f.dispatched.peek().correlationId());
            }
        }
    }
}
