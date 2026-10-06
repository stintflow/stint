package io.stintflow.spi;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;

import org.junit.jupiter.api.Test;

/** SDD 2.8: standard types, the legacy aliases and what a raised error carries. */
class ErrorInfoTest {

    @Test
    void legacy_types_are_aliases_of_the_standard_ones() {
        assertThat(ErrorInfo.normalize(URI.create("stint://errors/timeout")))
                .isEqualTo(URI.create("https://serverlessworkflow.io/spec/1.0.0/errors/timeout"));
        assertThat(ErrorInfo.normalize(URI.create("stint://errors/communication"))).isEqualTo(ErrorInfo.TYPE_COMMUNICATION);
        assertThat(ErrorInfo.normalize(URI.create("https://acme.example/errors/quota")))
                .isEqualTo(URI.create("https://acme.example/errors/quota")); // a custom type stays as is
    }

    @Test
    void default_status_follows_the_dsl_table() {
        assertThat(ErrorInfo.defaultStatus(ErrorInfo.TYPE_TIMEOUT)).isEqualTo(408);
        assertThat(ErrorInfo.defaultStatus(URI.create("stint://errors/validation"))).isEqualTo(400);
        assertThat(ErrorInfo.defaultStatus(ErrorInfo.StandardType.AUTHORIZATION.uri())).isEqualTo(403);
        assertThat(ErrorInfo.defaultStatus(URI.create("https://acme.example/errors/quota"))).isEqualTo(500);
    }

    @Test
    void a_raised_error_has_the_standard_type_a_status_and_the_components_pointer() {
        ErrorInfo raised = new ErrorInfo(URI.create("stint://errors/runtime"), null, "Boom", "x", null, null)
                .raisedAt("/do/1/work");
        assertThat(raised.type()).isEqualTo(ErrorInfo.TYPE_RUNTIME);
        assertThat(raised.status()).isEqualTo(500);
        assertThat(raised.instance()).isEqualTo("/do/1/work");

        ErrorInfo fromWorker = new ErrorInfo(ErrorInfo.TYPE_COMMUNICATION, 429, null, null, "/client/call", null)
                .raisedAt("/do/1/work");
        assertThat(fromWorker.status()).isEqualTo(429);                 // the worker's own status is kept
        assertThat(fromWorker.instance()).isEqualTo("/client/call");    // and its more specific instance
    }
}
