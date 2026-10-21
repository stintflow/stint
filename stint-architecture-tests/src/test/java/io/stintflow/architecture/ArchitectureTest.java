package io.stintflow.architecture;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.assertj.core.api.Assertions.assertThat;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import io.stintflow.knowledge.DeleteRequest;
import io.stintflow.knowledge.KnowledgeBatch;
import io.stintflow.knowledge.KnowledgeStore;
import io.stintflow.knowledge.ReindexRequest;
import io.stintflow.spi.AreaKeys;
import io.stintflow.spi.BlobStore;

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
        // SDD 3a.1, RF9: the packages the AI-free rule now also covers are imported too.
        assertThat(classes.containPackage("io.stintflow.wire.decision")).isTrue();
        assertThat(classes.containPackage("io.stintflow.dsl")).isTrue();
        // SDD 3a.3, CA3 and SDD 3a.2, CA10: every package the LLM and decision rules below analyze or name is imported,
        // each with classes.
        for (String pkg : new String[] {"io.stintflow.spi", "io.stintflow.wire", "io.stintflow.core",
                "io.stintflow.dsl", "io.stintflow.worker", "io.stintflow.worker.langchain4j",
                "io.stintflow.worker.langchain4j.memory", "io.stintflow.aws.conversations",
                "io.stintflow.worker.decision", "io.stintflow.worker.decision.jev", "io.stintflow.aws.worker",
                "io.stintflow.knowledge", "io.stintflow.knowledge.inmemory", "io.stintflow.knowledge.schema",
                "io.stintflow.knowledge.extraction",
                "io.stintflow.worker.redaction", "io.stintflow.worker.transcription",
                "io.stintflow.worker.publishing", "io.stintflow.worker.knowledge",
                "io.stintflow.knowledge.inspector"}) {
            assertThat(classes.that(DescribedPredicate.describe(pkg, (JavaClass c) -> c.getPackageName().equals(pkg))))
                    .as("classes imported from %s", pkg).isNotEmpty();
        }
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

    /**
     * SDD 3a.7, 4.1: the wire and the plain worker SDK are cloud-blind too — transports live in connectors (the SQS
     * worker binding is {@code io.stintflow.aws.worker}). {@code io.stintflow.worker} is matched as a single package:
     * the worker modules under it (LLM, decision) have rules of their own.
     */
    @ArchTest
    static final ArchRule wire_and_worker_sdk_are_cloud_blind = noClasses()
            .that().resideInAnyPackage("io.stintflow.wire..", "io.stintflow.worker")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "software.amazon..", "com.amazonaws..", "com.azure..", "com.google.cloud..", "io.fabric8..");

    /**
     * SDD 3a.1, RF9: the decision contract lives in {@code stint-wire} and {@code call: decision} compiles in
     * {@code stint-dsl}; neither may pull an AI library in — providers live only in workers.
     */
    @ArchTest
    static final ArchRule spi_wire_core_and_dsl_have_no_ai_libs = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "dev.langchain4j..",
                    "io.quarkiverse.langchain4j..");

    /**
     * SDD 3a.3, CA3 (RF1): LangChain4j stays in the LLM worker module. Nothing of the engine side — and not the plain
     * worker SDK either — depends on the module or on LangChain4j; {@code io.stintflow.worker} is matched as a single
     * package, since the module lives under it.
     */
    @ArchTest
    static final ArchRule engine_side_and_worker_sdk_do_not_depend_on_the_llm_module = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "io.stintflow.worker.langchain4j..",
                    "dev.langchain4j..",
                    "io.quarkiverse.langchain4j..");

    /** SDD 3a.3, 8h: only the module's main package speaks LangChain4j — its memory port and the adapters don't. */
    @ArchTest
    static final ArchRule only_the_llm_module_uses_langchain4j = noClasses()
            .that().resideInAnyPackage("io.stintflow..")
            .and().resideOutsideOfPackage("io.stintflow.worker.langchain4j")
            .and().resideOutsideOfPackage("io.stintflow.example.ai..")
            .should().dependOnClassesThat().resideInAnyPackage("dev.langchain4j..", "io.quarkiverse.langchain4j..");

    /**
     * SDD 3a.2, CA10: the decision adapters are worker-side. Nothing of the engine side — and not the plain worker SDK —
     * depends on them; {@code io.stintflow.worker} is matched as a single package, since the adapters live under it.
     */
    @ArchTest
    static final ArchRule engine_side_and_worker_sdk_do_not_depend_on_decision_adapters = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.worker.decision..");

    /** SDD 3a.2, 4.1: the decision module and the Jev adapter carry no AI library and no cloud SDK. */
    @ArchTest
    static final ArchRule decision_adapters_have_no_ai_libs_or_cloud_sdks = noClasses()
            .that().resideInAnyPackage("io.stintflow.worker.decision..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "dev.langchain4j..", "io.quarkiverse.langchain4j..", "software.amazon..", "com.amazonaws..");

    /**
     * SDD 3a.5, RF5: the reference redactor is deterministic and runs where the data is — no AI library, no cloud SDK,
     * no OpenTelemetry; and nothing of the engine side depends on it.
     */
    @ArchTest
    static final ArchRule redactor_has_no_ai_libs_or_cloud_sdks = noClasses()
            .that().resideInAnyPackage("io.stintflow.worker.redaction..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "dev.langchain4j..", "io.quarkiverse.langchain4j..", "software.amazon..", "com.amazonaws..",
                    "com.azure..", "com.google.cloud..", "io.opentelemetry..");

    /**
     * SDD 3a.6, 4.1: the transcription and publishing ports and their adapters are plain HTTP over the JDK — no AI
     * library, no cloud SDK, no OpenTelemetry — and nothing of the engine side depends on them.
     */
    @ArchTest
    static final ArchRule transcription_and_publishing_have_no_ai_libs_or_cloud_sdks = noClasses()
            .that().resideInAnyPackage("io.stintflow.worker.transcription..", "io.stintflow.worker.publishing..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "dev.langchain4j..", "io.quarkiverse.langchain4j..", "software.amazon..", "com.amazonaws..",
                    "com.azure..", "com.google.cloud..", "io.opentelemetry..", "io.stintflow.core..");

    @ArchTest
    static final ArchRule engine_side_does_not_depend_on_transcription_or_publishing = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.worker.transcription..",
                    "io.stintflow.worker.publishing..");

    @ArchTest
    static final ArchRule engine_side_and_worker_sdk_do_not_depend_on_the_redactor = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.worker.redaction..");

    /**
     * SDD 3b.1, 4.1: the knowledge port lives outside the SPI and depends on it alone — no engine, no wire, no DSL, no worker, no
     * cloud SDK, no AI library, no OpenTelemetry. The engine side and the plain worker SDK do not depend on it either: the
     * engine never calls the base; workers do.
     */
    @ArchTest
    static final ArchRule knowledge_depends_on_the_spi_alone = noClasses()
            .that().resideInAnyPackage("io.stintflow.knowledge..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "io.stintflow.core..", "io.stintflow.wire..", "io.stintflow.dsl..", "io.stintflow.worker..",
                    "io.stintflow.aws..", "io.stintflow.inmemory..", "software.amazon..", "com.amazonaws..",
                    "com.azure..", "com.google.cloud..", "io.fabric8..", "dev.langchain4j..",
                    "io.quarkiverse.langchain4j..", "io.opentelemetry..");

    @ArchTest
    static final ArchRule engine_side_and_worker_sdk_do_not_depend_on_knowledge = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.knowledge..");

    /**
     * SDD 3b.3, 4.9: the knowledge tasks (write and retrieve) run where the data is — no AI library, no cloud SDK, no
     * OpenTelemetry, and not the engine; and nothing of the engine side depends on them.
     */
    @ArchTest
    static final ArchRule knowledge_workers_have_no_ai_libs_cloud_sdks_or_engine = noClasses()
            .that().resideInAnyPackage("io.stintflow.worker.knowledge..")
            .should().dependOnClassesThat().resideInAnyPackage(
                    "dev.langchain4j..", "io.quarkiverse.langchain4j..", "software.amazon..", "com.amazonaws..",
                    "com.azure..", "com.google.cloud..", "io.opentelemetry..", "io.stintflow.core..", "io.stintflow.dsl..");

    @ArchTest
    static final ArchRule engine_side_does_not_depend_on_the_knowledge_workers = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker", "io.stintflow.knowledge..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.worker.knowledge..");

    /**
     * SDD 3b.5, 4.5: the redactor does not know the base — finding a person from who they are is a task of the knowledge worker,
     * which is handed the secret; and in that module only that task (and the registration that hands it over) touches the secret
     * behind the pseudonyms.
     */
    @ArchTest
    static final ArchRule the_redactor_does_not_know_the_knowledge_base = noClasses()
            .that().resideInAnyPackage("io.stintflow.worker.redaction..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.knowledge..", "io.stintflow.worker.knowledge..");

    @ArchTest
    static final ArchRule only_the_person_resolver_holds_the_pseudonym_secret = noClasses()
            .that().resideInAPackage("io.stintflow.worker.knowledge")
            .and().doNotHaveSimpleName("ResolvePersonHandler").and().doNotHaveSimpleName("KnowledgeWorkers")
            .and().doNotHaveSimpleName("PiiGate")   // a throwaway instance with a secret of its own, to run the patterns
            .should().dependOnClassesThat().haveFullyQualifiedName("io.stintflow.worker.redaction.Pseudonyms");

    /**
     * SDD 3b.5b, CA10: the data key of a blob, in the clear, is asked for and opened by the envelope decorator alone. The task that
     * destroys an area's key only destroys it and asks its state; nothing else reads a blob's key.
     */
    @ArchTest
    static final ArchRule only_the_envelope_store_asks_for_a_data_key = noClasses()
            .that().doNotHaveSimpleName("EnvelopeBlobStore")
            .should().callMethod(AreaKeys.class, "newDataKey", String.class)
            .orShould().callMethod(AreaKeys.class, "unwrap", String.class, byte[].class);

    /**
     * SDD 3c.1, 4.5 (CA8): the pointer of a graph node is not the pointer of a blob. It does not know the claim check of the wire
     * module, nor its field, so the one is never taken for the other.
     */
    @ArchTest
    static final ArchRule the_graph_pointer_is_not_a_claim_check = noClasses()
            .that().haveSimpleNameStartingWith("GraphPointer")
            .should().dependOnClassesThat().resideInAPackage("io.stintflow.wire..");

    /**
     * SDD 3b.4, 4.7 (CA10): the retriever a model's worker reads the base with only reads. It asks the port to retrieve and to look
     * an entity up, and never writes, erases or reindexes; it does not know the in-memory adapters, a batch, an erasure request, a
     * reindexation request or the blob store.
     */
    @ArchTest
    static final ArchRule the_content_retriever_only_reads = noClasses()
            .that().haveSimpleName("KnowledgeContentRetriever")
            .should().callMethod(KnowledgeStore.class, "write", io.stintflow.knowledge.KnowledgeScope.class, KnowledgeBatch.class)
            .orShould().callMethod(KnowledgeStore.class, "delete", io.stintflow.knowledge.KnowledgeScope.class, DeleteRequest.class)
            .orShould().callMethod(KnowledgeStore.class, "reindex", io.stintflow.knowledge.KnowledgeScope.class, ReindexRequest.class)
            .orShould().dependOnClassesThat(com.tngtech.archunit.core.domain.JavaClass.Predicates.belongToAnyOf(
                    KnowledgeBatch.class, DeleteRequest.class, ReindexRequest.class, BlobStore.class))
            .orShould().dependOnClassesThat().resideInAnyPackage("io.stintflow.knowledge.inmemory..", "io.stintflow.inmemory..");

    /**
     * SDD 3b.6, 4.6 (CA9): the inspector looks and never touches. It does not depend on the store that writes, on a batch, on an
     * erasure or a reindexation request, on the blob store, nor on the in-memory adapters — only on the inspection port and its
     * types. And nothing of the engine side or the workers depends on it.
     */
    @ArchTest
    static final ArchRule the_inspector_is_read_only = noClasses()
            .that().resideInAPackage("io.stintflow.knowledge.inspector..")
            .should().dependOnClassesThat(com.tngtech.archunit.core.domain.JavaClass.Predicates.belongToAnyOf(
                    KnowledgeStore.class, KnowledgeBatch.class, DeleteRequest.class, ReindexRequest.class, BlobStore.class))
            .orShould().dependOnClassesThat().resideInAnyPackage("io.stintflow.knowledge.inmemory..", "io.stintflow.inmemory..",
                    "io.stintflow.worker..", "io.stintflow.core..");

    @ArchTest
    static final ArchRule engine_side_and_workers_do_not_depend_on_the_inspector = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.dsl..", "io.stintflow.worker..")
            .should().dependOnClassesThat().resideInAPackage("io.stintflow.knowledge.inspector..");

    /** SDD 3b.1, 4.1: the port and its types do not know the adapters; backends plug in beside them. */
    @ArchTest
    static final ArchRule the_knowledge_port_does_not_depend_on_its_in_memory_adapters = noClasses()
            .that().resideInAPackage("io.stintflow.knowledge")
            .should().dependOnClassesThat().resideInAPackage("io.stintflow.knowledge.inmemory..");

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

    /**
     * SDD 2.5, 8c: tracing is a port (strings + {@code ExecutionTracer}); OpenTelemetry lives only in
     * {@code stint-otel}. The worker SDK is included so lightweight workers never drag OTel in either.
     */
    @ArchTest
    static final ArchRule spi_wire_core_and_worker_sdk_have_no_opentelemetry = noClasses()
            .that().resideInAnyPackage("io.stintflow.spi..", "io.stintflow.wire..", "io.stintflow.core..",
                    "io.stintflow.worker..")
            .should().dependOnClassesThat().resideInAnyPackage("io.opentelemetry..");

    /** SDD 1.4, RNF2 — migrated here from stint-dsl's own ArchitectureTest. */
    @ArchTest
    static final ArchRule core_never_depends_on_dsl = noClasses()
            .that().resideInAnyPackage("io.stintflow.core..")
            .should().dependOnClassesThat().resideInAnyPackage("io.stintflow.dsl..");
}
