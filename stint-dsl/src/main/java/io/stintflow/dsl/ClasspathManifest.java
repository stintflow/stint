package io.stintflow.dsl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Resolves which classpath resources {@link DslLoader#loadClasspath} should read (SDD 1.4, sec.
 * 8a). Native-image cannot list a classpath directory, so by default this reads a build-time
 * generated manifest (one resource name per line) by its exact, known name — never a directory
 * listing. {@link LoadOptions#explicitResources()} is the documented escape hatch.
 */
final class ClasspathManifest {

    private ClasspathManifest() {
    }

    static List<String> resolve(String pattern, LoadOptions opts) {
        if (!opts.explicitResources().isEmpty()) {
            return opts.explicitResources();
        }

        int slash = pattern.lastIndexOf('/');
        String dir = slash < 0 ? "" : pattern.substring(0, slash);
        String manifestResource = dir.isEmpty() ? "index.txt" : dir + "/index.txt";

        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        if (cl == null) {
            cl = ClasspathManifest.class.getClassLoader();
        }
        try (InputStream in = cl.getResourceAsStream(manifestResource)) {
            if (in == null) {
                throw new IllegalStateException("No workflow manifest found at '" + manifestResource
                        + "' — was WorkflowManifestGenerator run for this module's build (SDD 1.4, sec. 8a)?");
            }
            List<String> names;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                names = reader.lines().map(String::trim).filter(l -> !l.isBlank()).toList();
            }
            return names.stream().map(name -> dir.isEmpty() ? name : dir + "/" + name).toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
