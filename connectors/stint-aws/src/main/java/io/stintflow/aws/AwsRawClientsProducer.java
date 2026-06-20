package io.stintflow.aws;

import java.net.URI;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;

/**
 * CDI producers for AWS clients that have no Quarkus extension (currently EventBridge).
 * <p>
 * The {@code stint.aws.endpoint-override} property points these at floci ({@code http://localhost:4566}).
 */
@ApplicationScoped
public class AwsRawClientsProducer {

    @ConfigProperty(name = "stint.aws.endpoint-override", defaultValue = "")
    String endpointOverride;

    @ConfigProperty(name = "stint.aws.region", defaultValue = "us-east-1")
    String region;

    @ConfigProperty(name = "stint.aws.access-key", defaultValue = "test")
    String accessKey;

    @ConfigProperty(name = "stint.aws.secret-key", defaultValue = "test")
    String secretKey;

    @Produces
    @ApplicationScoped
    public EventBridgeClient eventBridgeClient() {
        var builder = EventBridgeClient.builder()
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)));
        if (!endpointOverride.isBlank()) {
            builder.endpointOverride(URI.create(endpointOverride));
        }
        return builder.build();
    }
}
