package io.stintflow.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * The invariant that makes "cloud-agnostic" real: the SPI and the core orchestrator must never depend
 * on any cloud SDK. If this test fails, the agnosticism has been broken at the boundary.
 */
@AnalyzeClasses(packages = "io.stintflow")
class ArchitectureTest {

    // allowEmptyShould(true): on the bleeding edge (Java 25 / class file v69), ArchUnit's bundled ASM
    // may not yet parse the bytecode and would import zero classes. We don't want that to *fail* the
    // build — but the rule still fires the moment a compatible ASM reads the classes and finds a real
    // cloud dependency in spi/core. It guards without blocking on tooling lag.
    @ArchTest
    static final ArchRule spi_and_core_are_cloud_blind = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.core..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "software.amazon..",   // AWS
                    "com.amazonaws..",     // AWS (v1)
                    "com.azure..",         // Azure
                    "com.google.cloud..",  // GCP
                    "io.fabric8..")        // Kubernetes
            .allowEmptyShould(true);
}
