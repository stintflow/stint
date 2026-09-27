package io.stintflow.dsl.build;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

/** SDD 1.4, sec. 8a: the build-time manifest generator lists only {@code *.yaml}/{@code *.yml}
 *  files, sorted, one per line — the exact contract {@link io.stintflow.dsl.ClasspathManifest} reads. */
class WorkflowManifestGeneratorTest {

    @Test
    void lists_only_yaml_files_sorted() throws Exception {
        Path dir = Files.createTempDirectory("manifest-src");
        Files.writeString(dir.resolve("b.yaml"), "b: 1");
        Files.writeString(dir.resolve("a.yml"), "a: 1");
        Files.writeString(dir.resolve("notes.txt"), "ignored");

        Path out = Files.createTempDirectory("manifest-out").resolve("stint/workflows/index.txt");
        WorkflowManifestGenerator.generate(dir, out);

        assertThat(Files.readString(out)).isEqualTo("a.yml\nb.yaml");
    }

    @Test
    void writes_an_empty_manifest_when_the_source_directory_does_not_exist() throws Exception {
        Path missing = Files.createTempDirectory("manifest-src").resolve("does-not-exist");
        Path out = Files.createTempDirectory("manifest-out").resolve("stint/workflows/index.txt");

        WorkflowManifestGenerator.generate(missing, out);

        assertThat(Files.readString(out)).isEmpty();
    }
}
