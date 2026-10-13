package io.stintflow.inmemory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import io.stintflow.spi.BlobNotFoundException;
import io.stintflow.spi.BlobStore;
import io.stintflow.spi.RetentionClass;

/**
 * Filesystem blob store (claim-check) for local/dev. Maps a key to a file under a base directory.
 * <p>
 * SDD 3a.5, RF7: a blob put with a {@link RetentionClass} expires by the store's clock — the local stand-in for an S3
 * lifecycle rule. Past its expiry, {@code get} fails as for a missing blob ({@link BlobNotFoundException}) and
 * deletes it; {@link #sweep()} deletes
 * every expired one. Its expiry lives in a sidecar file next to it.
 */
public final class FilesystemBlobStore implements BlobStore {

    static final String EXPIRY_SUFFIX = ".expires";

    private final Path baseDir;
    private final InstantSource clock;

    public FilesystemBlobStore(Path baseDir) {
        this(baseDir, InstantSource.system());
    }

    public FilesystemBlobStore(Path baseDir, InstantSource clock) {
        this.baseDir = baseDir;
        this.clock = clock;
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key) {
        return CompletableFuture.supplyAsync(() -> write(data, key, null));
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key, RetentionClass cls) {
        return CompletableFuture.supplyAsync(() -> write(data, cls.prefixed(key), clock.instant().plus(cls.ttl())));
    }

    private URI write(byte[] data, String key, Instant expiry) {
        try {
            Path target = baseDir.resolve(key.replace('/', '_').replace(':', '_'));
            Files.write(target, data);
            Path sidecar = expiryOf(target);
            if (expiry != null) {
                Files.writeString(sidecar, expiry.toString(), StandardCharsets.US_ASCII);
            } else {
                Files.deleteIfExists(sidecar);
            }
            return target.toUri();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CompletionStage<byte[]> get(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            Path path = Path.of(ref);
            try {
                if (expired(path)) {
                    remove(path);
                    throw new BlobNotFoundException(ref, "expired by its retention class");
                }
                return Files.readAllBytes(path);
            } catch (NoSuchFileException e) {
                throw new BlobNotFoundException(ref, "no such file");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public CompletionStage<Void> delete(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                remove(Path.of(ref));
                return null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    /** Deletes every blob past its expiry; how many it deleted. */
    public CompletionStage<Integer> sweep() {
        return CompletableFuture.supplyAsync(() -> {
            try (Stream<Path> files = Files.list(baseDir)) {
                int deleted = 0;
                for (Path sidecar : files.filter(p -> p.getFileName().toString().endsWith(EXPIRY_SUFFIX)).toList()) {
                    String name = sidecar.getFileName().toString();
                    Path blob = sidecar.resolveSibling(name.substring(0, name.length() - EXPIRY_SUFFIX.length()));
                    if (expired(blob)) {
                        remove(blob);
                        deleted++;
                    }
                }
                return deleted;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private boolean expired(Path blob) throws IOException {
        Path sidecar = expiryOf(blob);
        if (!Files.exists(sidecar)) {
            return false;
        }
        return !clock.instant().isBefore(Instant.parse(Files.readString(sidecar, StandardCharsets.US_ASCII).trim()));
    }

    private static void remove(Path blob) throws IOException {
        for (Path path : List.of(blob, expiryOf(blob))) {
            Files.deleteIfExists(path);
        }
    }

    private static Path expiryOf(Path blob) {
        return blob.resolveSibling(blob.getFileName() + EXPIRY_SUFFIX);
    }
}
