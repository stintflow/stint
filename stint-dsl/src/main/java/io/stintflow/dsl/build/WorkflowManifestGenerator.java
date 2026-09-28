package io.stintflow.dsl.build;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Build-time tool (SDD 1.4, sec. 8a): scans a directory of {@code *.yaml}/{@code *.yml} workflow
 * definitions and writes {@code index.txt}, one file name per line, sorted for a deterministic
 * build. This manifest is what lets {@link io.stintflow.dsl.DslLoader#loadClasspath} find workflow
 * resources by exact name inside a native image, which cannot list a classpath directory.
 * <p>
 * Invoked by each module's build (e.g. via {@code exec-maven-plugin} in the {@code
 * generate-resources} phase) — never runs inside the shipped application or the native image
 * itself.
 */
public final class WorkflowManifestGenerator {

    private WorkflowManifestGenerator() {
    }

    /** @param args {@code <sourceDir> <outputFile>} */
    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Usage: WorkflowManifestGenerator <sourceDir> <outputFile>");
        }
        generate(Path.of(args[0]), Path.of(args[1]));
    }

    static void generate(Path sourceDir, Path outputFile) {
        List<String> names;
        if (Files.isDirectory(sourceDir)) {
            try (Stream<Path> files = Files.list(sourceDir)) {
                names = files.filter(Files::isRegularFile)
                        .map(p -> p.getFileName().toString())
                        .filter(n -> n.endsWith(".yaml") || n.endsWith(".yml"))
                        .sorted(Comparator.naturalOrder())
                        .toList();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            names = List.of();
        }
        try {
            Files.createDirectories(outputFile.getParent());
            Files.write(outputFile, String.join("\n", names).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
