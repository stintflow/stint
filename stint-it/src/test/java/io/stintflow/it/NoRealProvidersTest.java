package io.stintflow.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

/**
 * SDD 3a.2, CA4: the build never calls a real model provider. Every test JVM resolves only the names in
 * {@code build/test-hosts} ({@code -Djdk.net.hosts.file}, set in the parent pom), so a provider's host doesn't resolve;
 * and no real provider key may be configured where the tests run.
 */
class NoRealProvidersTest {

    /** Variables that would hold a real provider credential. AWS's must be floci's dummy, if set at all. */
    static final List<String> KEY_VARIABLES = List.of("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "TYPESAFE_API_KEY",
            "JEV_API_KEY", "GOOGLE_API_KEY", "MISTRAL_API_KEY", "AZURE_OPENAI_API_KEY");

    @Test
    void the_test_jvm_resolves_only_localhost() throws Exception {
        assertThat(System.getProperty("jdk.net.hosts.file")).as("the network guard of the parent pom").isNotBlank();
        assertThat(InetAddress.getByName("localhost").isLoopbackAddress()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"api.openai.com", "api.anthropic.com", "api.typesafe.ai", "generativelanguage.googleapis.com"})
    void a_provider_host_does_not_resolve(String host) {
        assertThatThrownBy(() -> InetAddress.getByName(host)).isInstanceOf(UnknownHostException.class);
    }

    @Test
    void no_real_provider_key_is_configured() {
        for (String variable : KEY_VARIABLES) {
            assertThat(System.getenv(variable)).as("%s must not be set where tests run", variable).isNull();
        }
        String aws = System.getenv("AWS_SECRET_ACCESS_KEY");
        assertThat(aws == null || aws.equals("test")).as("AWS_SECRET_ACCESS_KEY is floci's dummy 'test' or unset")
                .isTrue();
    }
}
