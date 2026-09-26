package com.predisched.common.db;

import java.util.Map;

/** The flat JSON objects stored in {@code jsonb} columns; no JSON library needed for these. */
final class Json {

    private Json() {}

    static String object(Map<String, ?> fields) {
        if (fields == null) {
            return "{}";
        }
        StringBuilder out = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, ?> field : fields.entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            quote(out, field.getKey());
            out.append(':');
            Object value = field.getValue();
            if (value instanceof Number number && Double.isFinite(number.doubleValue())) {
                out.append(number);
            } else if (value instanceof Boolean) {
                out.append(value);
            } else {
                quote(out, String.valueOf(value));
            }
        }
        return out.append('}').toString();
    }

    private static void quote(StringBuilder out, String value) {
        out.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        out.append('"');
    }
}
