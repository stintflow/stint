package io.stintflow.inmemory;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import io.stintflow.spi.AreaKeyDestroyedException;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.DataKey;

/**
 * {@link AreaKeys} in memory, for tests and the example (SDD 3b.5b): one AES key per area, created on first use; destroying
 * zeroes it, and the area is remembered as destroyed.
 */
public final class InMemoryAreaKeys implements AreaKeys {

    private static final SecureRandom RANDOM = new SecureRandom();
    private static final byte[] DESTROYED = new byte[0];

    private final ConcurrentHashMap<String, byte[]> masters = new ConcurrentHashMap<>();

    @Override
    public CompletionStage<DataKey> newDataKey(String area) {
        try {
            AreaKeys.requireArea(area);
            byte[] master = masters.computeIfAbsent(area, a -> randomKey());
            if (master == DESTROYED) {
                return CompletableFuture.failedFuture(new AreaKeyDestroyedException(area));
            }
            byte[] clear = randomKey();
            return CompletableFuture.completedFuture(new DataKey(clear, wrap(area, master, clear)));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<byte[]> unwrap(String area, byte[] wrappedKey) {
        try {
            AreaKeys.requireArea(area);
            byte[] master = masters.get(area);
            if (master == DESTROYED) {
                return CompletableFuture.failedFuture(new AreaKeyDestroyedException(area));
            }
            if (master == null) {
                return CompletableFuture.failedFuture(new IllegalStateException("No key was ever made for this area"));
            }
            return CompletableFuture.completedFuture(unwrap(area, master, wrappedKey));
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<State> destroy(String area) {
        try {
            AreaKeys.requireArea(area);
            byte[] old = masters.put(area, DESTROYED);
            if (old != null && old != DESTROYED) {
                Arrays.fill(old, (byte) 0);
            }
            return CompletableFuture.completedFuture(State.DESTROYED);
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletionStage<State> state(String area) {
        byte[] master = masters.get(AreaKeys.requireArea(area));
        return CompletableFuture.completedFuture(master == null ? State.ABSENT : master == DESTROYED ? State.DESTROYED : State.ACTIVE);
    }

    private static byte[] randomKey() {
        byte[] key = new byte[32];
        RANDOM.nextBytes(key);
        return key;
    }

    private static byte[] wrap(String area, byte[] master, byte[] clear) {
        try {
            byte[] iv = new byte[12];
            RANDOM.nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(master, "AES"), new GCMParameterSpec(128, iv));
            cipher.updateAAD(area.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] body = cipher.doFinal(clear);
            return ByteBuffer.allocate(iv.length + body.length).put(iv).put(body).array();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("The data key could not be wrapped", e);
        }
    }

    private static byte[] unwrap(String area, byte[] master, byte[] wrapped) {
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(master, "AES"), new GCMParameterSpec(128, wrapped, 0, 12));
            cipher.updateAAD(area.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            return cipher.doFinal(wrapped, 12, wrapped.length - 12);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("The data key could not be unwrapped");
        }
    }
}
