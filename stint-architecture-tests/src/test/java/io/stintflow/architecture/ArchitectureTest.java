package io.stintflow.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * SDD 1.5, RF5: the single consolidated architecture suite for the whole reactor, replacing the
 * separate {@code ArchitectureTest}s that used to live in {@code stint-core} and {@code stint-dsl}.
 * <p>
 * Every rule here is only as trustworthy as {@link #classes_are_actually_imported}: ArchUnit 1.4.0's
 * bundled ASM silently imported zero {@code io.stintflow} classes on JDK 25 (class file v69), which
 * would have made every "no violations" result below meaningless without anyone noticing (SDD 1.5,
 * sec. 1/8a). That was fixed by bumping to ArchUnit 1.5.1; this test now asserts the count directly
 * instead of relying on {@code allowEmptyShould(true)} as a silent safety net.
 */
@AnalyzeClasses(packages = "io.stintflow")
class ArchitectureTest {

    @ArchTest
    static void classes_are_actually_imported(JavaClasses classes) {
        assertThat(classes.size()).isGreaterThan(0);
    }

    @ArchTest
    static final ArchRule spi_and_core_are_cloud_blind = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.core..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "software.amazon..",   // AWS
                    "com.amazonaws..",     // AWS (v1)
                    "com.azure..",         // Azure
                    "com.google.cloud..",  // GCP
                    "io.fabric8..");       // Kubernetes

    @ArchTest
    static final ArchRule spi_and_core_have_no_ai_libs = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.core..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "dev.langchain4j..",
                    "io.quarkiverse.langchain4j..");

    /**
     * SDD 1.5, RF1/RF2: reactivated once {@code Json}, {@code CeWire}, {@code DefaultCloudEventCodec}
     * and {@code ClaimCheck} moved from {@code io.stintflow.core} to {@code io.stintflow.wire} — the
     * 14 pre-existing violations this rule found (sec. 1/8a) are gone now that every connector and
     * the worker SDK point at {@code stint-wire} instead.
     */
    @ArchTest
    static final ArchRule worker_sdk_and_connectors_do_not_depend_on_core = noClasses()
            .that().resideInAnyPackage("io.stintflow.worker..", "io.stintflow.inmemory..", "io.stintflow.aws..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.core..");

    /** RF1: {@code stint-wire} depends only on {@code stint-spi} + Jackson + CloudEvents — never back on core. */
    @ArchTest
    static final ArchRule wire_does_not_depend_on_core = noClasses()
            .that().resideInAnyPackage("io.stintflow.wire..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.core..");

    /** SDD 1.4, RNF2 — migrated here from stint-dsl's own ArchitectureTest. */
    @ArchTest
    static final ArchRule core_never_depends_on_dsl = noClasses()
            .that().resideInAnyPackage("io.stintflow.core..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.dsl..");
}
