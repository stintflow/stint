package io.stintflow.aws;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.stintflow.spi.AreaKeyDestroyedException;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.DataKey;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.AlreadyExistsException;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DisabledException;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;
import software.amazon.awssdk.services.kms.model.KeyMetadata;
import software.amazon.awssdk.services.kms.model.KeyState;
import software.amazon.awssdk.services.kms.model.KmsInvalidStateException;
import software.amazon.awssdk.services.kms.model.NotFoundException;

/**
 * {@link AreaKeys} on AWS KMS (SDD 3b.5b): one customer key per area, found by the alias {@code alias/stint-area-<area>} and
 * created on first use. Data keys are made by {@code GenerateDataKey}, with the area as encryption context; destroying
 * schedules the key's deletion — from then on {@code Decrypt} fails, though the key is physically deleted only after the window
 * (7 to 30 days).
 * <p>
 * <b>Limit.</b> After the window KMS deletes the key and its alias, and keeps no memory of the area: the name could be used
 * again with a new key. The blobs of the old key stay unreadable; keeping the name from being reused (an allow list of areas
 * that no longer lists it) is the caller's.
 */
public final class KmsAreaKeys implements AreaKeys {

    public static final String ALIAS_PREFIX = "alias/stint-area-";
    public static final String CONTEXT_KEY = "stint-area";
    /** KMS's minimum window for scheduling a key's deletion. */
    public static final int DEFAULT_WINDOW_DAYS = 7;

    private final KmsClient kms;
    private final int windowDays;

    public KmsAreaKeys(KmsClient kms) {
        this(kms, DEFAULT_WINDOW_DAYS);
    }

    public KmsAreaKeys(KmsClient kms, int windowDays) {
        this.kms = Objects.requireNonNull(kms, "kms");
        if (windowDays < 7 || windowDays > 30) {
            throw new IllegalArgumentException("KMS deletes a key 7 to 30 days after it is scheduled");
        }
        this.windowDays = windowDays;
    }

    @Override
    public CompletionStage<DataKey> newDataKey(String area) {
        return CompletableFuture.supplyAsync(() -> {
            AreaKeys.requireArea(area);
            KeyMetadata key = find(area);
            if (key == null) {
                key = create(area);
            }
            if (key.keyState() != KeyState.ENABLED) {
                throw new AreaKeyDestroyedException(area);
            }
            String keyId = key.keyId();
            GenerateDataKeyResponse made = kms.generateDataKey(b -> b.keyId(keyId).keySpec(DataKeySpec.AES_256)
                    .encryptionContext(Map.of(CONTEXT_KEY, area)));
            return new DataKey(made.plaintext().asByteArray(), made.ciphertextBlob().asByteArray());
        });
    }

    @Override
    public CompletionStage<byte[]> unwrap(String area, byte[] wrappedKey) {
        return CompletableFuture.supplyAsync(() -> {
            AreaKeys.requireArea(area);
            try {
                return kms.decrypt(b -> b.ciphertextBlob(SdkBytes.fromByteArray(wrappedKey))
                        .encryptionContext(Map.of(CONTEXT_KEY, area))).plaintext().asByteArray();
            } catch (KmsInvalidStateException | DisabledException | NotFoundException e) {
                throw new AreaKeyDestroyedException(area);
            }
        });
    }

    @Override
    public CompletionStage<State> destroy(String area) {
        return CompletableFuture.supplyAsync(() -> {
            AreaKeys.requireArea(area);
            KeyMetadata key = find(area);
            if (key == null) {
                key = create(area);   // so the name is taken in the window, and nothing can be written to it
            }
            if (key.keyState() != KeyState.PENDING_DELETION) {
                String keyId = key.keyId();
                kms.scheduleKeyDeletion(b -> b.keyId(keyId).pendingWindowInDays(windowDays));
            }
            return State.DESTROYED;
        });
    }

    @Override
    public CompletionStage<State> state(String area) {
        return CompletableFuture.supplyAsync(() -> {
            KeyMetadata key = find(AreaKeys.requireArea(area));
            return key == null ? State.ABSENT : key.keyState() == KeyState.ENABLED ? State.ACTIVE : State.DESTROYED;
        });
    }

    private KeyMetadata find(String area) {
        try {
            return kms.describeKey(b -> b.keyId(ALIAS_PREFIX + area)).keyMetadata();
        } catch (NotFoundException e) {
            return null;
        }
    }

    private KeyMetadata create(String area) {
        String keyId = kms.createKey(b -> b.description("stint area " + area)).keyMetadata().keyId();
        try {
            kms.createAlias(b -> b.aliasName(ALIAS_PREFIX + area).targetKeyId(keyId));
        } catch (AlreadyExistsException e) {   // another writer was first; use its key
            kms.scheduleKeyDeletion(b -> b.keyId(keyId).pendingWindowInDays(windowDays));
        }
        return find(area);
    }
}
