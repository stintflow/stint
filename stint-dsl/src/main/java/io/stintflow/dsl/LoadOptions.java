package io.stintflow.dsl;

import java.util.List;

/**
 * Options controlling how {@link DslLoader} treats a document (SDD 1.4, RF3 / sec. 8a).
 *
 * @param ignoreUnsupported when {@code true}, a construct the DSL recognizes but this phase does
 *                          not implement (e.g. {@code schedule}, {@code emit}) is dropped with a
 *                          logged warning instead of failing the load — genuine schema/syntax
 *                          errors still fail regardless of this flag
 * @param explicitResources when non-empty, {@link DslLoader#loadClasspath} reads exactly these
 *                          classpath resource names instead of the build-generated manifest
 *                          (sec. 8a's escape hatch — useful for tests and overrides)
 */
public record LoadOptions(boolean ignoreUnsupported, List<String> explicitResources) {

    public static final LoadOptions STRICT = new LoadOptions(false, List.of());
    public static final LoadOptions PERMISSIVE = new LoadOptions(true, List.of());

    public LoadOptions {
        explicitResources = List.copyOf(explicitResources);
    }

    public static LoadOptions withExplicitResources(List<String> resources) {
        return new LoadOptions(false, resources);
    }
}
