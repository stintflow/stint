package io.stintflow.aws;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.stintflow.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/** {@link BlobStore} backed by S3. References are {@code s3://bucket/key} URIs. */
@ApplicationScoped
public class S3BlobStore implements BlobStore {

    @Inject
    S3Client s3;

    @ConfigProperty(name = "stint.aws.s3.bucket", defaultValue = "stint-blobs")
    String bucket;

    @Override
    public CompletionStage<URI> put(byte[] data, String key) {
        return CompletableFuture.supplyAsync(() -> {
            s3.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
                    RequestBody.fromBytes(data));
            return URI.create("s3://" + bucket + "/" + key);
        });
    }

    @Override
    public CompletionStage<byte[]> get(URI ref) {
        return CompletableFuture.supplyAsync(() ->
                s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(ref.getHost()).key(stripLeadingSlash(ref.getPath())).build()).asByteArray());
    }

    @Override
    public CompletionStage<Void> delete(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(ref.getHost()).key(stripLeadingSlash(ref.getPath())).build());
            return null;
        });
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
