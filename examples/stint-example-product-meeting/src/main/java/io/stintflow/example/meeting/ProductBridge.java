package io.stintflow.example.meeting;

import java.io.IOException;
import java.io.InputStream;

import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.worker.knowledge.BridgeContract;

/** SDD 3c.5, 4.1: the contract of exposure of the product domain's base, the document the origin's policy is built from. */
final class ProductBridge {

    static final String RESOURCE = "bridge-product-meetings-1.0.0.json";

    private ProductBridge() {
    }

    static BridgeContract contract() {
        try (InputStream in = ProductBridge.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            return BridgeContract.parse(in.readAllBytes(), KnowledgeSchema.meetings());
        } catch (IOException e) {
            throw new IllegalStateException("The bridge contract could not be read", e);
        }
    }
}
