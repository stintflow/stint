package io.stintflow.aws;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

/**
 * {@link BlobStore} backed by S3. References are {@code s3://bucket/key} URIs.
 * <p>
 * SDD 3a.5, RF7: every object is encrypted at rest — SSE-KMS with {@code stint.aws.s3.kms-key-id} when one is
 * configured, SSE-S3 otherwise. A blob put with a {@link RetentionClass} goes under {@code retention/<class>/} and is
 * tagged {@code stint-retention=<class>}; {@link #lifecycleRules} turns the classes into the bucket's expiration rules
 * by prefix, and {@link #applyLifecycle} sends them, for a deployment allowed to configure the bucket.
 */
@ApplicationScoped
public class S3BlobStore implements BlobStore {

    public static final String RETENTION_TAG = "stint-retention";
    /** The id prefix of the lifecycle rules this store owns; others on the bucket are kept. */
    public static final String RULE_PREFIX = "stint-retention-";

    @Inject
    S3Client s3;

    @ConfigProperty(name = "stint.aws.s3.bucket", defaultValue = "stint-blobs")
    String bucket;

    @ConfigProperty(name = "stint.aws.s3.kms-key-id")
    Optional<String> kmsKeyId = Optional.empty();

    public S3BlobStore() {
    }

    /** Test/manual wiring outside CDI: SSE-S3. */
    public S3BlobStore(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    /** Test/manual wiring outside CDI: SSE-KMS with {@code kmsKeyId}. */
    public S3BlobStore(S3Client s3, String bucket, String kmsKeyId) {
        this(s3, bucket);
        this.kmsKeyId = Optional.of(kmsKeyId);
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key) {
        return CompletableFuture.supplyAsync(() -> putObject(data, key, null));
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key, RetentionClass cls) {
        return CompletableFuture.supplyAsync(() -> putObject(data, cls.prefixed(key), cls));
    }

    private URI putObject(byte[] data, String key, RetentionClass cls) {
        PutObjectRequest.Builder request = PutObjectRequest.builder().bucket(bucket).key(key);
        if (kmsKeyId.isPresent()) {
            request.serverSideEncryption(ServerSideEncryption.AWS_KMS).ssekmsKeyId(kmsKeyId.get());
        } else {
            request.serverSideEncryption(ServerSideEncryption.AES256);
        }
        if (cls != null) {
            request.tagging(RETENTION_TAG + "=" + cls.name());
        }
        s3.putObject(request.build(), RequestBody.fromBytes(data));
        return URI.create("s3://" + bucket + "/" + key);
    }

    @Override
    public CompletionStage<byte[]> get(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return s3.getObjectAsBytes(GetObjectRequest.builder()
                        .bucket(ref.getHost()).key(stripLeadingSlash(ref.getPath())).build()).asByteArray();
            } catch (NoSuchKeyException e) {
                throw new BlobNotFoundException(ref, "no such key (deleted, or expired by a lifecycle rule)");
            }
        });
    }

    @Override
    public CompletionStage<Void> delete(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            s3.deleteObject(DeleteObjectRequest.builder()
                    .bucket(ref.getHost()).key(stripLeadingSlash(ref.getPath())).build());
            return null;
        });
    }

    /** One expiration rule per class, by its prefix; a TTL that isn't whole days rounds up. */
    public static List<LifecycleRule> lifecycleRules(Collection<RetentionClass> classes) {
        return classes.stream().map(cls -> LifecycleRule.builder()
                .id(RULE_PREFIX + cls.name())
                .filter(LifecycleRuleFilter.builder().prefix(cls.prefix()).build())
                .expiration(LifecycleExpiration.builder().days(days(cls.ttl())).build())
                .status(ExpirationStatus.ENABLED)
                .build()).toList();
    }

    /**
     * Sets the bucket's expiration rules for {@code classes}, replacing the rules this store set before and keeping any
     * other rule on the bucket.
     */
    public void applyLifecycle(Collection<RetentionClass> classes) {
        List<LifecycleRule> rules = new ArrayList<>();
        try {
            s3.getBucketLifecycleConfiguration(r -> r.bucket(bucket)).rules().stream()
                    .filter(rule -> rule.id() == null || !rule.id().startsWith(RULE_PREFIX)).forEach(rules::add);
        } catch (S3Exception e) {
            if (e.statusCode() != 404) {
                throw e;
            }
        }
        rules.addAll(lifecycleRules(classes));
        s3.putBucketLifecycleConfiguration(r -> r.bucket(bucket)
                .lifecycleConfiguration(BucketLifecycleConfiguration.builder().rules(rules).build()));
    }

    private static int days(Duration ttl) {
        long days = ttl.toDays() + (ttl.equals(Duration.ofDays(ttl.toDays())) ? 0 : 1);
        return (int) Math.max(1, days);
    }

    private static String stripLeadingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }
}
