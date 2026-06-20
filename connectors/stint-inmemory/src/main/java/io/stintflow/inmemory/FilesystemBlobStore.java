package io.stintflow.inmemory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.stintflow.spi.BlobStore;

/** Filesystem blob store (claim-check) for local/dev. Maps a key to a file under a base directory. */
public final class FilesystemBlobStore implements BlobStore {

    private final Path baseDir;

    public FilesystemBlobStore(Path baseDir) {
        this.baseDir = baseDir;
        try {
            Files.createDirectories(baseDir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CompletionStage<URI> put(byte[] data, String key) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Path target = baseDir.resolve(key.replace('/', '_').replace(':', '_'));
                Files.write(target, data);
                return target.toUri();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public CompletionStage<byte[]> get(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return Files.readAllBytes(Path.of(ref));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public CompletionStage<Void> delete(URI ref) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Files.deleteIfExists(Path.of(ref));
                return null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }
}
