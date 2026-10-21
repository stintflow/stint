package io.stintflow.knowledge;

import java.util.regex.Pattern;

/**
 * SDD 3c.1, 4.1: a pointer to a node of another domain's graph — {@code stint-kg://<namespace>/<base>/<node>?v=<version>}.
 * The domain is a base ({@link KnowledgeScope}); the node is an entity id (lowercase, digits, dot, dash and underscore); the version is the node's
 * version in the base's change log ({@link GraphPointers#versionOf}), so a holder can tell the origin changed since the link.
 * <p>
 * Not the pointer of a blob (the claim check of the wire module): a different thing with a different shape, and neither is
 * accepted for the other. The parsing is strict — nothing past the form is allowed, and a pointer with no version is refused
 * (there is no "the latest"). A refusal says <em>why</em>, with a short code, and never echoes what it was given.
 */
public record GraphPointer(String namespace, String base, String nodeId, long version) {

    public static final String SCHEME = "stint-kg://";
    public static final int MAX_LENGTH = 256;

    private static final Pattern DOMAIN = Pattern.compile("[a-z0-9][a-z0-9._-]{0,62}");
    /** An entity id is opaque (SDD 3b.1): {@link EntityIds#of} makes {@code e-<hex>}, a glossary may name its own, e.g. {@code sys-checkout}. */
    private static final Pattern NODE = Pattern.compile("[a-z0-9][a-z0-9._-]{0,127}");
    private static final Pattern VERSION = Pattern.compile("[1-9][0-9]{0,18}");

    public GraphPointer {
        require(namespace != null && DOMAIN.matcher(namespace).matches(), "domain");
        require(base != null && DOMAIN.matcher(base).matches(), "domain");
        require(nodeId != null && NODE.matcher(nodeId).matches(), "node");
        require(version >= 1, "version");
    }

    /** The pointer to {@code nodeId} of {@code scope}, at {@code version}. */
    public static GraphPointer of(KnowledgeScope scope, String nodeId, long version) {
        return new GraphPointer(scope.namespace(), scope.base(), nodeId, version);
    }

    /**
     * @throws KnowledgeValidationException with a code ({@code length}, {@code scheme}, {@code extra}, {@code domain},
     *                                      {@code node}, {@code version}) and nothing of the text
     */
    public static GraphPointer parse(String text) {
        require(text != null && !text.isEmpty() && text.length() <= MAX_LENGTH, "length");
        require(text.startsWith(SCHEME), "scheme");
        String rest = text.substring(SCHEME.length());
        int question = rest.indexOf('?');
        require(question >= 0, "version");
        String[] path = rest.substring(0, question).split("/", -1);
        require(path.length == 3, "extra");
        String query = rest.substring(question + 1);
        require(query.startsWith("v="), query.isEmpty() ? "version" : "extra");
        String version = query.substring(2);
        require(!version.contains("&") && !version.contains("#"), "extra");
        require(VERSION.matcher(version).matches(), "version");
        long parsed;
        try {
            parsed = Long.parseLong(version);
        } catch (NumberFormatException e) {
            throw new KnowledgeValidationException("Invalid graph pointer (version)");
        }
        return new GraphPointer(path[0], path[1], path[2], parsed);
    }

    public String format() {
        return SCHEME + namespace + "/" + base + "/" + nodeId + "?v=" + version;
    }

    public KnowledgeScope scope() {
        return KnowledgeScope.named(namespace, base);
    }

    /** Whether the two point at the same node, whatever their versions. */
    public boolean sameNodeAs(GraphPointer other) {
        return namespace.equals(other.namespace) && base.equals(other.base) && nodeId.equals(other.nodeId);
    }

    /** This pointer without its version: what identifies the node. */
    public String nodeKey() {
        return SCHEME + namespace + "/" + base + "/" + nodeId;
    }

    @Override
    public String toString() {
        return format();
    }

    private static void require(boolean ok, String code) {
        if (!ok) {
            throw new KnowledgeValidationException("Invalid graph pointer (" + code + ")");
        }
    }
}
