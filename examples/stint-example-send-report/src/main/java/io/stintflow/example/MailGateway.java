package io.stintflow.example;

import java.util.concurrent.CompletionStage;

/** Port to whatever actually sends mail (SES, SMTP, ...); written from scratch for SDD 1.4. */
public interface MailGateway {

    /** @return a message id for the sent mail */
    CompletionStage<String> send(String to, String subject, String body);
}
