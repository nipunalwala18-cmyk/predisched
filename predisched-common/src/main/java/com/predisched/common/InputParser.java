package com.predisched.common;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Parses the {@code key=value[, key=value]} input format of spec section 7.2. */
public final class InputParser {

    private InputParser() {}

    public static Map<String, String> parse(String input) {
        if (input == null || input.trim().isEmpty()) {
            throw new IllegalArgumentException("input must be non-empty key=value[, key=value]");
        }
        Map<String, String> out = new LinkedHashMap<>();
        String[] parts = input.split(",");
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.isEmpty()) {
                throw new IllegalArgumentException("empty segment in input: '" + input + "'");
            }
            int eq = trimmed.indexOf('=');
            if (eq <= 0 || eq == trimmed.length() - 1) {
                throw new IllegalArgumentException(
                        "segment must be key=value, got '" + trimmed + "'");
            }
            String key = trimmed.substring(0, eq).trim();
            String value = trimmed.substring(eq + 1).trim();
            if (key.isEmpty() || value.isEmpty()) {
                throw new IllegalArgumentException(
                        "segment must be key=value, got '" + trimmed + "'");
            }
            if (!key.matches("[A-Za-z0-9_.-]+")) {
                throw new IllegalArgumentException("invalid key '" + key + "'");
            }
            out.put(key, value);
        }
        if (out.isEmpty()) {
            throw new IllegalArgumentException("input must be non-empty key=value[, key=value]");
        }
        return Collections.unmodifiableMap(out);
    }

    public static String require(Map<String, String> params, String key) {
        String value = params.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("missing required key '" + key + "'");
        }
        return value;
    }
}
