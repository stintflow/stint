package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;

/**
 * SDD 3a.5, RF7 (CA3, local effect): a blob put with a retention class goes under {@code retention/<class>/} and, past
 * its TTL on the store's clock, is no longer read — {@code get} fails as for a missing blob and deletes it — and a
 * sweep deletes the rest. A blob without a class never expires. Controllable clock, no sleep, synthetic bytes.
 */
class BlobRetentionTest {

    static final Instant T0 = Instant.parse("2026-10-13T12:00:00Z");
    static final RetentionClass RAW_TRANSCRIPT = new RetentionClass("raw-transcript", Duration.ofDays(7));

    @TempDir
    Path dir;

    private final AtomicReference<Instant> now = new AtomicReference<>(T0);

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void a_classed_blob_is_read_until_its_ttl_and_never_after() {
        FilesystemBlobStore store = new FilesystemBlobStore(dir, now::get);
        URI ref = store.put(bytes("synthetic transcript"), "meetings/m-1.txt", RAW_TRANSCRIPT).toCompletableFuture()
                .join();
        URI plain = store.put(bytes("no class"), "plain.txt").toCompletableFuture().join();

        assertThat(ref.getPath()).contains("retention_raw-transcript_meetings_m-1.txt");
        now.set(T0.plus(Duration.ofDays(7)).minusMillis(1));
        assertThat(store.get(ref).toCompletableFuture().join()).asString(StandardCharsets.UTF_8)
                .isEqualTo("synthetic transcript");

        now.set(T0.plus(Duration.ofDays(7)));
        assertThatThrownBy(() -> store.get(ref).toCompletableFuture().join())
                .isInstanceOf(CompletionException.class)
                .satisfies(e -> assertThat(BlobNotFoundException.in(e)).isTrue())
                .hasMessageContaining("expired by its retention class");
        assertThat(Files.exists(Path.of(ref))).isFalse();   // read past its TTL: deleted, not only hidden

        now.set(T0.plus(Duration.ofDays(3650)));
        assertThat(store.get(plain).toCompletableFuture().join()).asString(StandardCharsets.UTF_8)
                .isEqualTo("no class");
    }

    @Test
    void a_sweep_deletes_every_expired_blob_and_only_those() throws Exception {
        FilesystemBlobStore store = new FilesystemBlobStore(dir, now::get);
        URI week = store.put(bytes("a"), "a", RAW_TRANSCRIPT).toCompletableFuture().join();
        URI month = store.put(bytes("b"), "b", RetentionClass.RESTRICTED_DEFAULT).toCompletableFuture().join();

        now.set(T0.plus(Duration.ofDays(8)));
        assertThat(store.sweep().toCompletableFuture().join()).isEqualTo(1);

        assertThat(Files.exists(Path.of(week))).isFalse();
        assertThat(Files.exists(Path.of(month))).isTrue();
        try (var files = Files.list(dir)) {
            assertThat(files.map(p -> p.getFileName().toString())).noneMatch(name -> name.contains("raw-transcript"));
        }
    }

    @Test
    void a_missing_blob_is_not_found_and_the_default_classed_put_only_prefixes_the_key() {
        FilesystemBlobStore store = new FilesystemBlobStore(dir, now::get);
        assertThatThrownBy(() -> store.get(dir.resolve("never-written").toUri()).toCompletableFuture().join())
                .satisfies(e -> assertThat(BlobNotFoundException.in(e)).isTrue());

        AtomicReference<String> keyed = new AtomicReference<>();
        BlobStore legacy = new BlobStore() {
            @Override
            public java.util.concurrent.CompletionStage<URI> put(byte[] data, String key) {
                keyed.set(key);
                return java.util.concurrent.CompletableFuture.completedFuture(URI.create("mem:" + key));
            }

            @Override
            public java.util.concurrent.CompletionStage<byte[]> get(URI ref) {
                throw new UnsupportedOperationException();
            }

            @Override
            public java.util.concurrent.CompletionStage<Void> delete(URI ref) {
                throw new UnsupportedOperationException();
            }
        };
        legacy.put(bytes("x"), "k/1", RAW_TRANSCRIPT).toCompletableFuture().join();
        assertThat(keyed.get()).isEqualTo("retention/raw-transcript/k/1");
    }

    @Test
    void a_retention_class_is_a_key_safe_name_living_at_least_a_day() {
        assertThatThrownBy(() -> new RetentionClass("Raw Transcript", Duration.ofDays(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RetentionClass("raw", Duration.ofHours(23)))
                .hasMessageContaining("at least a day");
    }
}
