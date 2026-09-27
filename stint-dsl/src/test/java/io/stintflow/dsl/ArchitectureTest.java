package io.stintflow.dsl;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * SDD 1.4, RNF2 / sec. 8c: the DSL front-end depends on the core tree model, never the other way
 * around — {@code stint-core} must stay usable without a YAML parser on its classpath at all.
 */
@AnalyzeClasses(packages = "io.stintflow")
class ArchitectureTest {

    @ArchTest
    static final ArchRule core_never_depends_on_dsl = noClasses()
            .that().resideInAnyPackage("io.stintflow.core..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.dsl..")
            .allowEmptyShould(true);
}
