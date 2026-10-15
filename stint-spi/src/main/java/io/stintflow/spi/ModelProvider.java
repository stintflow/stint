package io.stintflow.spi;

import java.util.Locale;
import java.util.Optional;

/**
 * SDD 3a.5, RF8/RF9: who ran the model behind a task — what the worker's sensitivity policy decides on, and what the
 * audit trail records for an AI call. Never a credential, an endpoint or any content. Carried on a task result as the
 * CloudEvents extension {@code stintprovider}, written {@code <name>;model=<model>;remote=<true|false>}.
 *
 * @param name   the provider (e.g. {@code litjev}, {@code openai}); no {@code ;}
 * @param model  the model, or {@code null}
 * @param remote whether the input leaves the worker's process — {@code true} when the worker doesn't know
 */
public record ModelProvider(String name, String model, boolean remote) {

    public ModelProvider {
        if (name == null || name.isBlank() || name.contains(";")) {
            throw new IllegalArgumentException("A provider name is required and has no ';'");
        }
        if (model != null && (model.isBlank() || model.contains(";"))) {
            throw new IllegalArgumentException("A model name has no ';'");
        }
    }

    /** The {@code stintprovider} value. */
    public String wire() {
        return name + (model == null ? "" : ";model=" + model) + ";remote=" + remote;
    }

    /**
     * The provider a {@code stintprovider} value names, or empty when it can't be read. A value without
     * {@code remote=false} is remote: the safe side.
     */
    public static Optional<ModelProvider> parse(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        String[] parts = value.split(";");
        String model = null;
        boolean remote = true;
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i].trim();
            if (part.startsWith("model=")) {
                model = part.substring("model=".length());
            } else if (part.toLowerCase(Locale.ROOT).equals("remote=false")) {
                remote = false;
            }
        }
        try {
            return Optional.of(new ModelProvider(parts[0].trim(), model == null || model.isBlank() ? null : model,
                    remote));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }
}
