package io.stintflow.spi;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * SDD 3b.5b: encrypts every blob on the way to another {@link BlobStore} with a data key of its own, wrapped by the key of the
 * blob's <i>area</i> ({@link AreaKeys}). Destroying the area's key makes every copy of its blobs unreadable.
 * <p>
 * The stored blob is {@code "STE1" | u8 areaLen | area | u16 wrappedLen | wrapped | iv(12) | AES-256-GCM(data)}. The area is in
 * the clear in the header (a name is not personal data) and is also authenticated data, so a header edited to point at another
 * area fails to read. The key a blob is put under, and its retention class, are those of the underlying store: a lifecycle rule
 * by prefix and a tag still apply.
 */
public final class EnvelopeBlobStore implements BlobStore {

    private static final byte[] MAGIC = "STE1".getBytes(StandardCharsets.US_ASCII);
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    private final BlobStore delegate;
    private final AreaKeys keys;
    private final Function<String, String> areaOf;

    /** @param areaOf the area of a blob, from the key it is put under */
    public EnvelopeBlobStore(BlobStore delegate, AreaKeys keys, Function<String, String> areaOf) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.keys = Objects.requireNonNull(keys, "keys");
        this.areaOf = Objects.requireNonNull(areaOf, "areaOf");
    }

    /** Every blob of this store is in {@code area}. */
    public static EnvelopeBlobStore forArea(BlobStore delegate, AreaKeys keys, String area) {
        AreaKeys.requireArea(area);
        return new EnvelopeBlobStore(delegate, keys, key -> area);
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key) {
        return seal(data, key).thenCompose(sealed -> delegate.put(sealed, key));
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key, RetentionClass cls) {
        return seal(data, key).thenCompose(sealed -> delegate.put(sealed, key, cls));
    }

    @Override
    public CompletionStage<byte[]> get(URI ref) {
        return delegate.get(ref).thenCompose(this::open);
    }

    @Override
    public CompletionStage<Void> delete(URI ref) {
        return delegate.delete(ref);
    }

    private CompletionStage<byte[]> seal(byte[] data, String key) {
        String area;
        try {
            area = AreaKeys.requireArea(areaOf.apply(key));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
        return keys.newDataKey(area).thenApply(dataKey -> encrypt(area, dataKey, data));
    }

    private static byte[] encrypt(String area, DataKey dataKey, byte[] data) {
        byte[] clear = dataKey.plaintext();
        try {
            byte[] areaBytes = area.getBytes(StandardCharsets.UTF_8);
            byte[] iv = new byte[IV_BYTES];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(clear, "AES"), new GCMParameterSpec(TAG_BITS, iv));
            cipher.updateAAD(aad(areaBytes));
            byte[] body = cipher.doFinal(data);
            byte[] wrapped = dataKey.wrapped();
            ByteBuffer out = ByteBuffer.allocate(MAGIC.length + 1 + areaBytes.length + 2 + wrapped.length + IV_BYTES + body.length);
            out.put(MAGIC).put((byte) areaBytes.length).put(areaBytes).putShort((short) wrapped.length).put(wrapped).put(iv).put(body);
            return out.array();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("The blob could not be encrypted", e);
        } finally {
            Arrays.fill(clear, (byte) 0);
        }
    }

    private CompletionStage<byte[]> open(byte[] stored) {
        String area;
        byte[] wrapped;
        int bodyStart;
        try {
            ByteBuffer in = ByteBuffer.wrap(stored);
            byte[] magic = new byte[MAGIC.length];
            in.get(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IllegalStateException("The blob is not an envelope this store wrote");
            }
            byte[] areaBytes = new byte[in.get() & 0xFF];
            in.get(areaBytes);
            area = AreaKeys.requireArea(new String(areaBytes, StandardCharsets.UTF_8));
            wrapped = new byte[in.getShort() & 0xFFFF];
            in.get(wrapped);
            bodyStart = in.position();
            if (stored.length - bodyStart < IV_BYTES + TAG_BITS / 8) {
                throw new IllegalStateException("The blob is truncated");
            }
        } catch (java.nio.BufferUnderflowException | IllegalArgumentException e) {
            return CompletableFuture.failedFuture(new IllegalStateException("The blob is not a readable envelope"));
        } catch (IllegalStateException e) {
            return CompletableFuture.failedFuture(e);
        }
        String readArea = area;
        return keys.unwrap(area, wrapped).thenApply(clear -> decrypt(readArea, clear, stored, bodyStart));
    }

    private static byte[] decrypt(String area, byte[] clear, byte[] stored, int bodyStart) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(clear, "AES"),
                    new GCMParameterSpec(TAG_BITS, stored, bodyStart, IV_BYTES));
            cipher.updateAAD(aad(area.getBytes(StandardCharsets.UTF_8)));
            return cipher.doFinal(stored, bodyStart + IV_BYTES, stored.length - bodyStart - IV_BYTES);
        } catch (AEADBadTagException e) {
            throw new IllegalStateException("The blob could not be authenticated");
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("The blob could not be decrypted", e);
        } finally {
            Arrays.fill(clear, (byte) 0);
        }
    }

    private static byte[] aad(byte[] areaBytes) {
        byte[] aad = Arrays.copyOf(MAGIC, MAGIC.length + areaBytes.length);
        System.arraycopy(areaBytes, 0, aad, MAGIC.length, areaBytes.length);
        return aad;
    }
}
