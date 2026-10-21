package io.stintflow.inmemory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.stintflow.spi.AreaKeyDestroyedException;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.DataKey;
import io.stintflow.spi.EnvelopeBlobStore;
import io.stintflow.spi.RetentionClass;

/**
 * SDD 3b.5b (CA1 to CA7): a blob is encrypted with a key of its own, wrapped by the key of its area; destroying the area's key
 * makes the blobs unreadable while the bytes kept do not change, and nothing else is affected.
 */
class EnvelopeBlobStoreTest {

    private static final String TEXT = "SYNTH-PII-0901 owns the rollback plan";

    @TempDir
    Path dir;

    private FilesystemBlobStore raw;
    private InMemoryAreaKeys keys;
    private EnvelopeBlobStore areaA;
    private EnvelopeBlobStore areaB;

    @BeforeEach
    void setUp() {
        raw = new FilesystemBlobStore(dir);
        keys = new InMemoryAreaKeys();
        areaA = EnvelopeBlobStore.forArea(raw, keys, "area-a");
        areaB = EnvelopeBlobStore.forArea(raw, keys, "area-b");
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    private static Throwable cause(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }

    @Test
    void ca1_a_blob_comes_back_whole_and_what_is_kept_holds_neither_the_text_nor_a_key_in_the_clear() {
        URI ref = areaA.put(bytes(TEXT), "minutes/m-1.md").toCompletableFuture().join();

        assertThat(areaA.get(ref).toCompletableFuture().join()).isEqualTo(bytes(TEXT));
        byte[] kept = raw.get(ref).toCompletableFuture().join();
        assertThat(contains(kept, bytes(TEXT))).isFalse();
        assertThat(contains(kept, bytes("SYNTH-PII"))).isFalse();
        assertThat(new String(Arrays.copyOf(kept, 4), StandardCharsets.US_ASCII)).isEqualTo("STE1");
        // the area is in the header; whoever reads does not have to know it
        assertThat(EnvelopeBlobStore.forArea(raw, keys, "area-b").get(ref).toCompletableFuture().join()).isEqualTo(bytes(TEXT));
        // two blobs of the same text are not the same bytes (a key and an iv of their own)
        URI other = areaA.put(bytes(TEXT), "minutes/m-2.md").toCompletableFuture().join();
        assertThat(raw.get(other).toCompletableFuture().join()).isNotEqualTo(kept);
    }

    @Test
    void ca2_destroying_the_key_of_an_area_makes_its_blobs_unreadable_and_leaves_the_other_area_alone() {
        URI inA = areaA.put(bytes(TEXT), "minutes/a.md").toCompletableFuture().join();
        URI inB = areaB.put(bytes("another area"), "minutes/b.md").toCompletableFuture().join();

        assertThat(keys.destroy("area-a").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);

        assertThatThrownBy(() -> areaA.get(inA).toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(AreaKeyDestroyedException.class));
        assertThatThrownBy(() -> areaA.put(bytes("late"), "minutes/late.md").toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(AreaKeyDestroyedException.class));
        assertThat(areaB.get(inB).toCompletableFuture().join()).isEqualTo(bytes("another area"));
        assertThat(keys.state("area-b").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ACTIVE);
    }

    @Test
    void ca3_the_bytes_kept_do_not_change_when_the_key_is_destroyed_it_is_the_key_that_goes() {
        URI ref = areaA.put(bytes(TEXT), "minutes/a.md").toCompletableFuture().join();
        byte[] before = raw.get(ref).toCompletableFuture().join();

        keys.destroy("area-a").toCompletableFuture().join();

        assertThat(raw.get(ref).toCompletableFuture().join()).isEqualTo(before);
        // a copy of the bytes made earlier (a backup) is as unreadable as the blob: nothing else holds the key
        URI copy = raw.put(before, "backup/a.md").toCompletableFuture().join();
        assertThatThrownBy(() -> areaB.get(copy).toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(AreaKeyDestroyedException.class));
    }

    @Test
    void ca4_a_header_edited_to_point_at_another_area_does_not_read() {
        URI ref = EnvelopeBlobStore.forArea(raw, keys, "area-a").put(bytes(TEXT), "minutes/a.md").toCompletableFuture().join();
        byte[] kept = raw.get(ref).toCompletableFuture().join();
        byte[] edited = kept.clone();
        byte[] to = bytes("area-b");
        System.arraycopy(to, 0, edited, 4 + 1, to.length);   // magic, area length, then the area
        URI tampered = raw.put(edited, "tampered/a.md").toCompletableFuture().join();

        assertThatThrownBy(() -> areaB.get(tampered).toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(IllegalStateException.class));
        // a byte of the body changed: authentication fails, no bytes come back
        byte[] flipped = kept.clone();
        flipped[flipped.length - 1] ^= 0x01;
        URI altered = raw.put(flipped, "tampered/b.md").toCompletableFuture().join();
        assertThatThrownBy(() -> areaA.get(altered).toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(IllegalStateException.class));
        // and what this store did not write is not guessed at
        URI plain = raw.put(bytes("not an envelope"), "plain/c.md").toCompletableFuture().join();
        assertThatThrownBy(() -> areaA.get(plain).toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(IllegalStateException.class));
    }

    @Test
    void ca5_destroying_twice_ends_the_same_and_an_area_that_never_existed_is_closed_for_writing() {
        areaA.put(bytes(TEXT), "minutes/a.md").toCompletableFuture().join();

        assertThat(keys.destroy("area-a").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
        assertThat(keys.destroy("area-a").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
        assertThat(keys.state("never-used").toCompletableFuture().join()).isEqualTo(AreaKeys.State.ABSENT);

        assertThat(keys.destroy("never-used").toCompletableFuture().join()).isEqualTo(AreaKeys.State.DESTROYED);
        EnvelopeBlobStore never = EnvelopeBlobStore.forArea(raw, keys, "never-used");
        assertThatThrownBy(() -> never.put(bytes("late"), "x").toCompletableFuture().join())
                .satisfies(e -> assertThat(cause(e)).isInstanceOf(AreaKeyDestroyedException.class));
    }

    @Test
    void ca6_no_key_in_the_clear_in_a_description_or_a_message() {
        DataKey key = keys.newDataKey("area-a").toCompletableFuture().join();
        assertThat(key.toString()).doesNotContain(java.util.Base64.getEncoder().encodeToString(key.plaintext()))
                .doesNotContain(Arrays.toString(key.plaintext()));

        keys.destroy("area-a").toCompletableFuture().join();
        AreaKeyDestroyedException destroyed = new AreaKeyDestroyedException("area-a");
        assertThat(destroyed.getMessage()).isEqualTo("The key of area 'area-a' was destroyed");
        assertThatThrownBy(() -> AreaKeys.requireArea("Bad Area")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void ca7_the_retention_class_and_the_key_of_the_blob_are_those_of_the_store_underneath() {
        RetentionClass cls = new RetentionClass("restricted", Duration.ofDays(30));
        URI ref = areaA.put(bytes(TEXT), "minutes/a.md", cls).toCompletableFuture().join();

        assertThat(ref.toString()).contains("retention_restricted_minutes_a.md");
        assertThat(areaA.get(ref).toCompletableFuture().join()).isEqualTo(bytes(TEXT));
        areaA.delete(ref).toCompletableFuture().join();
        assertThatThrownBy(() -> raw.get(ref).toCompletableFuture().join()).isInstanceOf(CompletionException.class);
    }
}
