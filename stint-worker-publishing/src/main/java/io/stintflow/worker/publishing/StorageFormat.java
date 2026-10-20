package io.stintflow.worker.publishing;

/**
 * The little markdown a generated document uses — {@code #}/{@code ##} headings, {@code - } list items and paragraphs —
 * as Confluence storage format (XHTML), every text escaped. Not a markdown parser: anything else is a paragraph.
 */
public final class StorageFormat {

    private StorageFormat() {
    }

    public static String fromMarkdown(String markdown) {
        StringBuilder out = new StringBuilder();
        boolean inList = false;
        for (String raw : markdown.split("\\R")) {
            String line = raw.strip();
            boolean item = line.startsWith("- ");
            if (inList && !item) {
                out.append("</ul>");
                inList = false;
            }
            if (line.isEmpty()) {
                continue;
            }
            if (item) {
                if (!inList) {
                    out.append("<ul>");
                    inList = true;
                }
                out.append("<li>").append(escape(line.substring(2))).append("</li>");
            } else if (line.startsWith("## ")) {
                out.append("<h2>").append(escape(line.substring(3))).append("</h2>");
            } else if (line.startsWith("# ")) {
                out.append("<h1>").append(escape(line.substring(2))).append("</h1>");
            } else {
                out.append("<p>").append(escape(line)).append("</p>");
            }
        }
        if (inList) {
            out.append("</ul>");
        }
        return out.toString();
    }

    static String escape(String text) {
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
