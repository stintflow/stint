package io.stintflow.worker.knowledge;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import io.stintflow.knowledge.KnowledgeScope;
import io.stintflow.knowledge.PassageIds;
import io.stintflow.knowledge.schema.KnowledgeSchema;
import io.stintflow.spi.Sensitivity;
import io.stintflow.wire.Json;

/**
 * SDD 3c.5, 4.1: the contract of exposure of one origin base — a versioned document that says which definitions may resolve what
 * in it, through which view, up to which level. It produces the {@link ResolutionPolicy} the resolver applies (the {@link Grant}
 * and the {@link PublishedView} are the ones of SDD 3c.2); what the auditor asks ("what does this domain expose, to whom, up to
 * where, in which version?") is answered by the document, and the audit of each resolution cites its version and hash.
 * <pre>
 * { "bridge": 1, "base": "ns/base", "version": "1.0.0",
 *   "consumers": [ { "definition": "ns:name", "types": ["Decision"], "view": "v1", "ceiling": "confidential" } ],
 *   "views": { "v1": { "allowSensitive": false,
 *                      "types": { "Decision": { "label": true, "properties": ["status"], "relations": ["supersedes"] } } } } }
 * </pre>
 * A document that does not load — a type or property the schema does not know, a sensitive property in a view that does not allow
 * it, a view that does not exist, a consumer with no type, a ceiling above {@code confidential} — is refused whole, with the path.
 */
public final class BridgeContract {

    private static final Pattern VERSION = Pattern.compile("\\d{1,4}\\.\\d{1,4}\\.\\d{1,4}");
    private static final Pattern DEFINITION = Pattern.compile("[a-z0-9][a-z0-9._-]{0,62}:[a-z0-9][a-z0-9._-]{0,62}");

    private final KnowledgeScope base;
    private final String version;
    private final String hash;
    private final ResolutionPolicy policy;

    private BridgeContract(KnowledgeScope base, String version, String hash, ResolutionPolicy policy) {
        this.base = base;
        this.version = version;
        this.hash = hash;
        this.policy = policy;
    }

    /** A contract that does not load. */
    public static final class Invalid extends IllegalArgumentException {
        public Invalid(String path, String problem) {
            super(path + ": " + problem);
        }
    }

    public KnowledgeScope base() {
        return base;
    }

    public String version() {
        return version;
    }

    /** SHA-256 of the canonical content (keys in order): the same document always has the same hash. */
    public String contentHash() {
        return hash;
    }

    /** The policy the resolver applies; it carries this contract's version and hash for the audit. */
    public ResolutionPolicy policy() {
        return policy;
    }

    /** Parses and checks {@code json} against the schema of the base it exposes. */
    public static BridgeContract parse(byte[] json, KnowledgeSchema schema) {
        Objects.requireNonNull(schema, "schema");
        JsonNode root;
        try {
            root = Json.read(json);
        } catch (RuntimeException e) {
            throw new Invalid("$", "not a JSON document");
        }
        if (root.path("bridge").asInt(0) != 1) {
            throw new Invalid("$.bridge", "the format is 1");
        }
        String version = root.path("version").asText("");
        if (!VERSION.matcher(version).matches()) {
            throw new Invalid("$.version", "a version is major.minor.patch");
        }
        String[] parts = root.path("base").asText("").split("/");
        KnowledgeScope base;
        try {
            if (parts.length != 2) {
                throw new IllegalArgumentException();
            }
            base = KnowledgeScope.named(parts[0], parts[1]);
        } catch (IllegalArgumentException e) {
            throw new Invalid("$.base", "a base is namespace/base");
        }

        Map<String, PublishedView> views = new LinkedHashMap<>();
        JsonNode declared = root.path("views");
        if (!declared.isObject() || declared.isEmpty()) {
            throw new Invalid("$.views", "at least one view");
        }
        for (Iterator<Map.Entry<String, JsonNode>> it = declared.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> view = it.next();
            String path = "$.views." + view.getKey();
            List<PublishedType> types = new ArrayList<>();
            JsonNode typed = view.getValue().path("types");
            if (!typed.isObject() || typed.isEmpty()) {
                throw new Invalid(path + ".types", "at least one type");
            }
            for (Iterator<Map.Entry<String, JsonNode>> t = typed.fields(); t.hasNext();) {
                Map.Entry<String, JsonNode> type = t.next();
                types.add(PublishedType.of(type.getKey(), type.getValue().path("label").asBoolean(false),
                        strings(type.getValue().path("properties")), strings(type.getValue().path("relations"))));
            }
            try {
                views.put(view.getKey(), PublishedView.of(view.getKey(), base, schema, types,
                        view.getValue().path("allowSensitive").asBoolean(false)));
            } catch (IllegalArgumentException e) {
                throw new Invalid(path, e.getMessage());
            }
        }

        List<Grant> grants = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        JsonNode consumers = root.path("consumers");
        if (!consumers.isArray() || consumers.isEmpty()) {
            throw new Invalid("$.consumers", "at least one consumer");
        }
        for (int i = 0; i < consumers.size(); i++) {
            JsonNode consumer = consumers.get(i);
            String path = "$.consumers[" + i + "]";
            String definition = consumer.path("definition").asText("");
            if (!DEFINITION.matcher(definition).matches() || !seen.add(definition)) {
                throw new Invalid(path + ".definition", "a unique namespace:name");
            }
            Set<String> types = strings(consumer.path("types"));
            if (types.isEmpty()) {
                throw new Invalid(path + ".types", "a consumer sees at least one type");
            }
            String viewName = consumer.path("view").asText("");
            PublishedView view = views.get(viewName);
            if (view == null) {
                throw new Invalid(path + ".view", "the contract has no view " + viewName);
            }
            for (String type : types) {
                if (!view.types().containsKey(type)) {
                    throw new Invalid(path + ".types", "the view " + viewName + " does not publish " + type);
                }
            }
            Sensitivity ceiling = Sensitivity.parse(consumer.path("ceiling").asText("")).orElseThrow(() ->
                    new Invalid(path + ".ceiling", "internal or confidential"));
            if (ceiling == Sensitivity.RESTRICTED) {
                throw new Invalid(path + ".ceiling", "the bridge never reaches restricted content");
            }
            String[] who = definition.split(":");
            grants.add(new Grant(who[0], who[1], Set.of(base), types, ceiling, viewName));
        }

        String hash = canonicalHash(root);
        ResolutionPolicy policy = new ResolutionPolicy(grants, List.copyOf(views.values())).contract(version, hash);
        return new BridgeContract(base, version, hash, policy);
    }

    private static Set<String> strings(JsonNode array) {
        Set<String> out = new java.util.LinkedHashSet<>();
        array.forEach(n -> out.add(n.asText()));
        return out;
    }

    private static String canonicalHash(JsonNode root) {
        try {
            Object tree = Json.MAPPER.treeToValue(root, Object.class);
            String canonical = Json.MAPPER.copy().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .writeValueAsString(tree);
            return PassageIds.sha256Hex(canonical);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new IllegalStateException("The contract could not be hashed", e);
        }
    }
}
