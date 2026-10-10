package io.stintflow.dsl;

import java.util.List;

/**
 * Thrown when a document fails to compile into a {@link io.stintflow.core.WorkflowDefinition}
 * (SDD 1.4, RF3/RF5/sec. 8b): invalid YAML, an unsupported construct (in strict mode), a bad
 * expression, or a schema violation. Carries every violation found, not just the first, so a
 * document with several problems (e.g. {@code schedule} and {@code emit} both unsupported) is
 * reported in one pass.
 */
public final class DslValidationException extends RuntimeException {

    private final String file;
    private final List<Violation> violations;

    public DslValidationException(String file, List<Violation> violations) {
        super(format(file, violations));
        this.file = file;
        this.violations = List.copyOf(violations);
    }

    public String file() {
        return file;
    }

    public List<Violation> violations() {
        return violations;
    }

    /** @param line 1-based, or {@code null} when a location could not be recovered (RF5: "when possible") */
    public record Violation(String pointer, String message, Integer line) {
    }

    private static String format(String file, List<Violation> violations) {
        StringBuilder sb = new StringBuilder("Failed to load workflow from ").append(file).append(':');
        for (Violation v : violations) {
            sb.append("\n  at ").append(v.pointer());
            if (v.line() != null) {
                sb.append(" (line ").append(v.line()).append(')');
            }
            sb.append(": ").append(v.message());
        }
        return sb.toString();
    }
}
