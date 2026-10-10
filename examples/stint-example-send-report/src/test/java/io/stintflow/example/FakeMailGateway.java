package io.stintflow.example;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;

/** Test double for {@link MailGateway} (SDD 1.4, CA2): records every send, no real mail sent. */
final class FakeMailGateway implements MailGateway {

    record Sent(String to, String subject, String body) {
    }

    private final List<Sent> sent = new CopyOnWriteArrayList<>();
    private int nextId = 1;

    @Override
    public CompletionStage<String> send(String to, String subject, String body) {
        sent.add(new Sent(to, subject, body));
        return CompletableFuture.completedFuture("msg-" + nextId++);
    }

    List<Sent> sent() {
        return sent;
    }
}
