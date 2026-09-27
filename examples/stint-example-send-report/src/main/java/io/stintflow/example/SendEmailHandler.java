package io.stintflow.example;

import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.stintflow.core.Json;
import io.stintflow.worker.TaskContext;
import io.stintflow.worker.TaskHandler;

/** Worker for {@link SendReport#ROUTE_SEND_EMAIL}: sends the mail via {@link MailGateway} and
 *  returns its message id. Written from scratch for SDD 1.4 (no prior implementation existed). */
public final class SendEmailHandler implements TaskHandler {

    private final MailGateway gateway;

    public SendEmailHandler(MailGateway gateway) {
        this.gateway = gateway;
    }

    @Override
    public CompletionStage<JsonNode> execute(TaskContext ctx) {
        String to = ctx.input().get("to").asText();
        String subject = ctx.input().get("subject").asText();
        String body = ctx.input().get("body").asText();

        return gateway.send(to, subject, body).thenApply(messageId -> {
            ObjectNode out = Json.obj();
            out.put("messageId", messageId);
            return out;
        });
    }
}
