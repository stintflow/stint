package io.stintflow.core.expr;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;

import io.stintflow.wire.Json;
import net.thisptr.jackson.jq.Function;

/**
 * SDD 2.10: jackson-jq finds its Java builtin functions through {@link java.util.ServiceLoader}, and Quarkus builds
 * native images without GraalVM's ServiceLoader feature. stint-core therefore carries the native metadata itself
 * ({@code META-INF/native-image/io.stintflow/stint-core}): the service file as a resource and every provider's
 * no-arg constructor. These tests keep that list equal to the providers of the jackson-jq on the classpath, so an
 * upgrade cannot silently leave functions out of native images.
 */
class JqNativeMetadataTest {

    private static final String SERVICE_FILE = "META-INF/services/" + Function.class.getName();
    private static final String METADATA = "META-INF/native-image/io.stintflow/stint-core/";

    @Test
    void every_jq_function_provider_is_registered_for_reflection_with_its_no_arg_constructor() throws IOException {
        Set<String> providers = providers();
        assertThat(providers).as("jackson-jq's providers in " + SERVICE_FILE).hasSizeGreaterThan(50);

        JsonNode reflect = metadata("reflect-config.json");
        Set<String> registered = new LinkedHashSet<>();
        for (JsonNode entry : reflect) {
            registered.add(entry.path("name").asText());
            assertThat(entry.path("methods")).as(entry.path("name").asText())
                    .anySatisfy(m -> {
                        assertThat(m.path("name").asText()).isEqualTo("<init>");
                        assertThat(m.path("parameterTypes")).isEmpty();
                    });
        }
        assertThat(registered).as("reflect-config.json vs " + SERVICE_FILE).containsExactlyInAnyOrderElementsOf(providers);
    }

    @Test
    void the_service_file_is_included_as_a_resource() throws IOException {
        List<String> patterns = new ArrayList<>();
        metadata("resource-config.json").path("resources").path("includes")
                .forEach(i -> patterns.add(i.path("pattern").asText()));
        assertThat(patterns).anyMatch(p -> Pattern.compile(p).matcher(SERVICE_FILE).matches());
    }

    /** The providers listed by jackson-jq's own service file(s) on the classpath. */
    private static Set<String> providers() throws IOException {
        Set<String> out = new LinkedHashSet<>();
        for (URL url : Collections.list(JqNativeMetadataTest.class.getClassLoader().getResources(SERVICE_FILE))) {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {
                in.lines().map(l -> l.replaceAll("#.*", "").strip()).filter(l -> !l.isEmpty()).forEach(out::add);
            }
        }
        return out;
    }

    private static JsonNode metadata(String file) throws IOException {
        try (InputStream in = JqNativeMetadataTest.class.getClassLoader().getResourceAsStream(METADATA + file)) {
            assertThat(in).as(METADATA + file).isNotNull();
            return Json.MAPPER.readTree(in);
        }
    }
}
