package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.KmsAreaKeys;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.spi.AreaKeyDestroyedException;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.EnvelopeBlobStore;
import io.stintflow.spi.RetentionClass;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * SDD 3b.5b, CA9, on floci: the blobs of an area go to S3 encrypted under a data key wrapped by a KMS key of the area. What the
 * bucket holds is not the text; once the area's key is scheduled for deletion, {@code Decrypt} fails and the blob can no longer be
 * read — the bytes in the bucket are the same — while the other area's blob is read as before.
 */
class FlociAreaKeysIT {

    private static final String TEXT = "SYNTH-PII-0901 owns the rollback plan";

    @Test
    void destroying_the_key_of_an_area_makes_its_objects_in_s3_unreadable_and_leaves_the_other_area() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));
            try (S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1).forcePathStyle(true)
                    .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 KmsClient kms = KmsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {
                s3.createBucket(b -> b.bucket("stint-blobs"));
                S3BlobStore bucket = new S3BlobStore(s3, "stint-blobs");
                KmsAreaKeys keys = new KmsAreaKeys(kms);
                EnvelopeBlobStore sales = EnvelopeBlobStore.forArea(bucket, keys, "sales");
                EnvelopeBlobStore hr = EnvelopeBlobStore.forArea(bucket, keys, "hr");

                URI inSales = sales.put(TEXT.getBytes(StandardCharsets.UTF_8), "minutes/m-1.md",
                        new RetentionClass("minutes", Duration.ofDays(90))).toCompletableFuture().join();
                URI inHr = hr.put("another area".getBytes(StandardCharsets.UTF_8), "minutes/h-1.md").toCompletableFuture().join();

                assertThat(keys.state("sales").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ACTIVE);
                assertThat(keys.state("never-used").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ABSENT);
                byte[] inTheBucket = raw(s3, inSales);
                assertThat(new String(inTheBucket, StandardCharsets.ISO_8859_1)).doesNotContain("SYNTH-PII").startsWith("STE1");
                assertThat(sales.get(inSales).toCompletableFuture().join()).isEqualTo(TEXT.getBytes(StandardCharsets.UTF_8));
                assertThat(inSales.toString()).contains("retention/minutes/minutes/m-1.md");   // the lifecycle prefix still applies

                assertThat(keys.destroy("sales").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
                assertThat(keys.destroy("sales").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);   // idempotent

                assertThat(keys.state("sales").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
                assertThatThrownBy(() -> sales.get(inSales).toCompletableFuture().join())
                        .satisfies(e -> assertThat(e instanceof CompletionException ? e.getCause() : e)
                                .isInstanceOf(AreaKeyDestroyedException.class));
                assertThatThrownBy(() -> sales.put(new byte[] {1}, "minutes/late").toCompletableFuture().join())
                        .satisfies(e -> assertThat(e instanceof CompletionException ? e.getCause() : e)
                                .isInstanceOf(AreaKeyDestroyedException.class));
                assertThat(raw(s3, inSales)).isEqualTo(inTheBucket);   // the object is untouched; the key went
                assertThat(hr.get(inHr).toCompletableFuture().join()).isEqualTo("another area".getBytes(StandardCharsets.UTF_8));

                // an area that never had a key is closed for writing by destroying it
                assertThat(keys.destroy("never-used").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
                assertThatThrownBy(() -> EnvelopeBlobStore.forArea(bucket, keys, "never-used")
                        .put(new byte[] {1}, "x").toCompletableFuture().join())
                        .satisfies(e -> assertThat(e instanceof CompletionException ? e.getCause() : e)
                                .isInstanceOf(AreaKeyDestroyedException.class));
            }
        }
    }

    private static byte[] raw(S3Client s3, URI ref) {
        String key = ref.getPath().substring(1);
        return s3.getObject(b -> b.bucket(ref.getHost()).key(key), ResponseTransformer.toBytes()).asByteArray();
    }
}
