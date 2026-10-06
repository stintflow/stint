package io.stintflow.it;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.stintflow.example.MailGateway;

/**
 * A test-only {@link MailGateway} that speaks plain SMTP over a JDK socket (SDD 2.6, sec. 8d) — enough to hand
 * a mail to a capturing server such as Mailpit, without adding a mail library to the repository. Not a
 * production client: no TLS, no auth, ASCII bodies.
 */
final class SmtpMailGateway implements MailGateway {

    private static final String FROM = "reports@stintflow.io";

    private final String host;
    private final int port;

    SmtpMailGateway(String host, int port) {
        this.host = host;
        this.port = port;
    }

    @Override
    public CompletionStage<String> send(String to, String subject, String body) {
        return CompletableFuture.supplyAsync(() -> {
            String messageId = "<" + UUID.randomUUID() + "@stintflow.io>";
            try (Socket socket = new Socket(host, port);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                 OutputStream out = socket.getOutputStream()) {
                expect(in, "220");
                command(out, in, "EHLO stint-it", "250");
                command(out, in, "MAIL FROM:<" + FROM + ">", "250");
                command(out, in, "RCPT TO:<" + to + ">", "250");
                command(out, in, "DATA", "354");
                String message = "From: " + FROM + "\r\nTo: " + to + "\r\nSubject: " + subject
                        + "\r\nMessage-ID: " + messageId + "\r\n\r\n" + body + "\r\n.";
                command(out, in, message, "250");
                command(out, in, "QUIT", "221");
                return messageId;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    private static void command(OutputStream out, BufferedReader in, String line, String expected) throws IOException {
        out.write((line + "\r\n").getBytes(StandardCharsets.US_ASCII));
        out.flush();
        expect(in, expected);
    }

    /** Reads a (possibly multi-line) reply and checks its code. */
    private static void expect(BufferedReader in, String code) throws IOException {
        String line;
        do {
            line = in.readLine();
            if (line == null) {
                throw new IOException("SMTP connection closed while waiting for " + code);
            }
        } while (line.length() > 3 && line.charAt(3) == '-');
        if (!line.startsWith(code)) {
            throw new IOException("SMTP expected " + code + ", got: " + line);
        }
    }
}
