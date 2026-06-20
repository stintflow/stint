package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Smoke test proving the floci wiring: boot the emulator, round-trip an object through S3.
 * <p>
 * Disabled by default (needs Docker). It is the foundation for the full build-report-over-floci IT —
 * the next concrete increment wires the AWS connectors (S3 + DynamoDB + SQS) and runs the real engine.
 */
@Disabled("requires Docker + floci image; remove @Disabled to run")
class FlociSmokeIT {

    @Test
    void s3_round_trip_against_floci() {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            String endpoint = "http://" + floci.getHost() + ":" + floci.getMappedPort(4566);

            try (S3Client s3 = S3Client.builder()
                    .endpointOverride(URI.create(endpoint))
                    .region(Region.US_EAST_1)
                    .credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create("test", "test")))
                    .httpClient(UrlConnectionHttpClient.create())
                    .forcePathStyle(true)
                    .build()) {

                s3.createBucket(CreateBucketRequest.builder().bucket("stint-blobs").build());
                s3.putObject(PutObjectRequest.builder().bucket("stint-blobs").key("hello.txt").build(),
                        RequestBody.fromString("ciao", StandardCharsets.UTF_8));

                String body = s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket("stint-blobs").key("hello.txt").build()).asUtf8String();

                assertThat(body).isEqualTo("ciao");
            }
        }
    }
}
