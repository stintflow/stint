package io.stintflow.spi;

/** Transport-agnostic representation of a task failure. */
public record ErrorInfo(String type, String message) {
    public static ErrorInfo of(Throwable t) {
        return new ErrorInfo(t.getClass().getName(), String.valueOf(t.getMessage()));
    }
}
