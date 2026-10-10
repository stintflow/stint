package io.stintflow.aws;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.stintflow.spi.BlobStore;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * The facts store (SDD 2.2, sec. 8d): where a fact's payload goes when it's too large for the domain
 * channel, referenced by the CloudEvents {@code dataref} extension. A <strong>dedicated bucket</strong>
 * ({@code stint.aws.s3.facts-bucket}), separate from the internal claim-check bucket, because readers are
 * consumers the engine doesn't know:
 * <ul>
 *   <li>the engine never deletes from it — retention is a lifecycle rule on the bucket (≥ 30 days
 *       recommended), never tied to an instance's own blobs;</li>
 *   <li>consumers read it through the bucket policy; the internal bucket stays private.</li>
 * </ul>
 * {@code @Typed} to itself so it never competes with {@link S3BlobStore} for {@code BlobStore} injection.
 */
@ApplicationScoped
@Typed(S3FactBlobStore.class)
public class S3FactBlobStore implements BlobStore {

    @Inject
    S3Client s3;

    @ConfigProperty(name = "stint.aws.s3.facts-bucket", defaultValue = "")
    String factsBucket;

    public S3FactBlobStore() {
    }

    /** Test/manual wiring outside CDI. */
    public S3FactBlobStore(S3Client s3, String factsBucket) {
        this.s3 = s3;
        this.factsBucket = factsBucket;
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key) {
        if (factsBucket == null || factsBucket.isBlank()) {
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "stint.aws.s3.facts-bucket is not configured: a fact above the domain channel's limit needs it"));
        }
        return delegate().put(data, key);
    }

    @Override
    public CompletionStage<byte[]> get(URI ref) {
        return delegate().get(ref);
    }

    /** Facts are never deleted by the engine (sec. 8d); kept for the port's contract only. */
    @Override
    public CompletionStage<Void> delete(URI ref) {
        return delegate().delete(ref);
    }

    private S3BlobStore delegate() {
        return new S3BlobStore(s3, factsBucket);
    }
}
