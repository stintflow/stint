package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import io.stintflow.aws.DynamoDbAuditLog;
import io.stintflow.aws.S3BlobStore;
import io.stintflow.spi.AuditLog.AuditRecord;
import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.RetentionClass;
import io.stintflow.spi.Sensitivity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.BucketLifecycleConfiguration;
import software.amazon.awssdk.services.s3.model.ExpirationStatus;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.LifecycleExpiration;
import software.amazon.awssdk.services.s3.model.LifecycleRule;
import software.amazon.awssdk.services.s3.model.LifecycleRuleFilter;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.Tag;

/**
 * SDD 3a.5 on floci (dummy {@code test}/{@code test} credentials): CA3's configuration side — every object encrypted
 * (SSE-S3, or SSE-KMS with a configured key), a classed blob under {@code retention/<class>/} and tagged with its
 * class, and the lifecycle rules by prefix sent and read back, other rules kept. floci 2.1 doesn't show a rule's
 * effect ({@code HeadObject} has no {@code Expiration}, and a rule's least is one day): that runs manually on AWS; the
 * local store proves it with a clock. Also the DynamoDB audit trail, read back in order with its TTL. Synthetic data.
 */
class FlociComplianceIT {

    static final RetentionClass RAW_TRANSCRIPT = new RetentionClass("raw-transcript", Duration.ofDays(7));
    static final Instant T0 = Instant.parse("2026-10-13T12:00:00Z");

    @Test
    void blobs_are_encrypted_classed_and_expired_by_lifecycle_rules_and_the_audit_trail_reads_back() throws Exception {
        try (GenericContainer<?> floci = new GenericContainer<>(DockerImageName.parse("floci/floci:latest"))
                .withExposedPorts(4566)) {
            floci.start();
            URI endpoint = URI.create("http://" + floci.getHost() + ":" + floci.getMappedPort(4566));
            var creds = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"));

            try (S3Client s3 = S3Client.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                    .forcePathStyle(true).credentialsProvider(creds)
                    .httpClient(UrlConnectionHttpClient.create()).build();
                 KmsClient kms = KmsClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build();
                 DynamoDbClient ddb = DynamoDbClient.builder().endpointOverride(endpoint).region(Region.US_EAST_1)
                         .credentialsProvider(creds).httpClient(UrlConnectionHttpClient.create()).build()) {

                s3.createBucket(b -> b.bucket("stint-blobs"));
                encryptedAndClassed(s3);
                encryptedWithKms(s3, kms);
                lifecycleBySentAndReadBack(s3);
                auditTrail(ddb);
            }
        }
    }

    private static void encryptedAndClassed(S3Client s3) {
        S3BlobStore store = new S3BlobStore(s3, "stint-blobs");
        URI ref = store.put("synthetic transcript".getBytes(StandardCharsets.UTF_8), "meetings/m-1.txt",
                RAW_TRANSCRIPT).toCompletableFuture().join();
        URI plain = store.put("plain".getBytes(StandardCharsets.UTF_8), "plain.txt").toCompletableFuture().join();

        assertThat(ref).hasToString("s3://stint-blobs/retention/raw-transcript/meetings/m-1.txt");
        HeadObjectResponse head = s3.headObject(h -> h.bucket("stint-blobs")
                .key("retention/raw-transcript/meetings/m-1.txt"));
        assertThat(head.serverSideEncryption()).isEqualTo(ServerSideEncryption.AES256);
        assertThat(s3.headObject(h -> h.bucket("stint-blobs").key("plain.txt")).serverSideEncryption())
                .isEqualTo(ServerSideEncryption.AES256);   // every object, classed or not
        assertThat(s3.getObjectTagging(t -> t.bucket("stint-blobs").key("retention/raw-transcript/meetings/m-1.txt"))
                .tagSet()).containsExactly(Tag.builder().key(S3BlobStore.RETENTION_TAG).value("raw-transcript").build());
        assertThat(store.get(ref).toCompletableFuture().join()).asString(StandardCharsets.UTF_8)
                .isEqualTo("synthetic transcript");
        assertThat(plain).hasToString("s3://stint-blobs/plain.txt");

        store.delete(ref).toCompletableFuture().join();
        assertThatThrownBy(() -> store.get(ref).toCompletableFuture().join())
                .satisfies(e -> assertThat(BlobNotFoundException.in(e)).isTrue());
    }

    private static void encryptedWithKms(S3Client s3, KmsClient kms) {
        String keyId = kms.createKey(k -> k.description("stint sdd-3a.5 synthetic key")).keyMetadata().keyId();
        S3BlobStore store = new S3BlobStore(s3, "stint-blobs", keyId);

        URI ref = store.put("synthetic kms".getBytes(StandardCharsets.UTF_8), "k.txt", RetentionClass.RESTRICTED_DEFAULT)
                .toCompletableFuture().join();

        HeadObjectResponse head = s3.headObject(h -> h.bucket("stint-blobs").key("retention/restricted/k.txt"));
        assertThat(head.serverSideEncryption()).isEqualTo(ServerSideEncryption.AWS_KMS);
        assertThat(head.ssekmsKeyId()).contains(keyId);
        assertThat(store.get(ref).toCompletableFuture().join()).asString(StandardCharsets.UTF_8)
                .isEqualTo("synthetic kms");
    }

    private static void lifecycleBySentAndReadBack(S3Client s3) {
        LifecycleRule someoneElses = LifecycleRule.builder().id("logs-90d")
                .filter(LifecycleRuleFilter.builder().prefix("logs/").build())
                .expiration(LifecycleExpiration.builder().days(90).build()).status(ExpirationStatus.ENABLED).build();
        s3.putBucketLifecycleConfiguration(r -> r.bucket("stint-blobs")
                .lifecycleConfiguration(BucketLifecycleConfiguration.builder().rules(someoneElses).build()));
        S3BlobStore store = new S3BlobStore(s3, "stint-blobs");

        store.applyLifecycle(List.of(RAW_TRANSCRIPT, new RetentionClass("restricted", Duration.ofHours(36))));
        store.applyLifecycle(List.of(RAW_TRANSCRIPT, new RetentionClass("restricted", Duration.ofHours(36))));

        List<LifecycleRule> rules = s3.getBucketLifecycleConfiguration(r -> r.bucket("stint-blobs")).rules();
        assertThat(rules).extracting(LifecycleRule::id)
                .containsExactlyInAnyOrder("logs-90d", "stint-retention-raw-transcript", "stint-retention-restricted");
        assertThat(rules).filteredOn(rule -> rule.id().equals("stint-retention-raw-transcript")).singleElement()
                .satisfies(rule -> {
                    assertThat(rule.filter().prefix()).isEqualTo("retention/raw-transcript/");
                    assertThat(rule.expiration().days()).isEqualTo(7);
                    assertThat(rule.status()).isEqualTo(ExpirationStatus.ENABLED);
                });
        assertThat(rules).filteredOn(rule -> rule.id().equals("stint-retention-restricted")).singleElement()
                .satisfies(rule -> assertThat(rule.expiration().days()).isEqualTo(2));   // 36 h rounds up
    }

    private static void auditTrail(DynamoDbClient ddb) throws Exception {
        ddb.createTable(t -> t.tableName("stint-audit")
                .attributeDefinitions(
                        a -> a.attributeName("instanceId").attributeType(ScalarAttributeType.S),
                        a -> a.attributeName("entry").attributeType(ScalarAttributeType.S))
                .keySchema(k -> k.attributeName("instanceId").keyType(KeyType.HASH),
                        k -> k.attributeName("entry").keyType(KeyType.RANGE))
                .billingMode("PAY_PER_REQUEST"));
        DynamoDbAuditLog audit = new DynamoDbAuditLog(ddb, "stint-audit", DynamoDbAuditLog.DEFAULT_TTL, () -> T0);

        audit.append(new AuditRecord("i-1", "chn-1", T0, AuditRecord.POLICY, "/do/0/ask", Sensitivity.RESTRICTED,
                "summarize-remote", "refused", Map.of("reason", "route 'summarize-remote' is not allowed")))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        audit.append(new AuditRecord("i-1", "chn-1", T0.plusMillis(1), AuditRecord.REDACTION, "/do/1/redact",
                Sensitivity.CONFIDENTIAL, "redactor", "lowered", Map.of("counts.email", "1", "from", "restricted")))
                .toCompletableFuture().get(10, TimeUnit.SECONDS);
        audit.append(new AuditRecord("i-2", null, T0, AuditRecord.AI_CALL, "/do/0/ask", null, "summarize-local",
                "answered", Map.of())).toCompletableFuture().get(10, TimeUnit.SECONDS);

        List<AuditRecord> mine = audit.of("i-1").toCompletableFuture().get(10, TimeUnit.SECONDS);
        assertThat(mine).extracting(AuditRecord::kind).containsExactly(AuditRecord.POLICY, AuditRecord.REDACTION);
        assertThat(mine.get(1).detail()).containsEntry("counts.email", "1").containsEntry("from", "restricted");
        assertThat(mine.get(1).sensitivity()).isEqualTo(Sensitivity.CONFIDENTIAL);
        assertThat(audit.of("i-2").toCompletableFuture().get(10, TimeUnit.SECONDS)).singleElement()
                .satisfies(r -> {
                    assertThat(r.sensitivity()).isNull();
                    assertThat(r.chainId()).isNull();
                });
        Map<String, AttributeValue> item = ddb.scan(s -> s.tableName("stint-audit")).items().getFirst();
        assertThat(Long.parseLong(item.get("expiresAt").n()))
                .isEqualTo(T0.plus(Duration.ofDays(400)).getEpochSecond());
    }
}
