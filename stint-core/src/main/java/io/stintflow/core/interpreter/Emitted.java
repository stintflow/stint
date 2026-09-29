package io.stintflow.core.interpreter;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * A fact an activation emitted (SDD 2.2): the evaluated {@code emit.event.with} object of the
 * {@code EmitNode} at {@code pointer}, and how many times that node had already emitted in the same
 * activation ({@code occurrence}, for local loops — part of the stable id, sec. 8b).
 */
public record Emitted(String pointer, int occurrence, JsonNode with) {
}
