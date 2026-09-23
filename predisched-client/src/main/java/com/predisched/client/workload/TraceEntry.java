package com.predisched.client.workload;

import com.predisched.proto.TaskType;

/**
 * One task in a saved workload trace (F8, rule 8). Offsets are milliseconds from the start of the
 * replay, so the same file replays identically at any speed. {@code timeoutMs} of 0 means the
 * task carries no deadline.
 */
public record TraceEntry(
        long offsetMs, String taskId, TaskType type, String input, int priority, long timeoutMs) {

    public String toJson() {
        StringBuilder json = new StringBuilder(160);
        json.append("{\"offset_ms\":").append(offsetMs);
        json.append(",\"task_id\":").append(quote(taskId));
        json.append(",\"type\":").append(quote(type.name()));
        json.append(",\"input\":").append(quote(input));
        json.append(",\"priority\":").append(priority);
        if (timeoutMs > 0) {
            json.append(",\"timeout_ms\":").append(timeoutMs);
        }
        json.append('}');
        return json.toString();
    }

    /** Parses one line written by {@link #toJson()}. Unknown keys are ignored. */
    public static TraceEntry fromJson(String line) {
        long offsetMs = 0;
        String taskId = null;
        TaskType type = null;
        String input = null;
        int priority = 0;
        long timeoutMs = 0;
        int i = skipWs(line, 0);
        if (i >= line.length() || line.charAt(i) != '{') {
            throw new IllegalArgumentException("trace line must be a JSON object: " + line);
        }
        i++;
        while (true) {
            i = skipWs(line, i);
            if (i < line.length() && line.charAt(i) == '}') {
                break;
            }
            if (line.charAt(i) != '"') {
                throw new IllegalArgumentException("expected a key in: " + line);
            }
            int[] end = new int[1];
            String key = parseString(line, i, end);
            i = skipWs(line, end[0]);
            if (i >= line.length() || line.charAt(i) != ':') {
                throw new IllegalArgumentException("expected ':' after key in: " + line);
            }
            i = skipWs(line, i + 1);
            if (i >= line.length()) {
                throw new IllegalArgumentException("truncated value in: " + line);
            }
            String value;
            if (line.charAt(i) == '"') {
                value = parseString(line, i, end);
                i = end[0];
            } else {
                int start = i;
                while (i < line.length() && line.charAt(i) != ',' && line.charAt(i) != '}') {
                    i++;
                }
                value = line.substring(start, i).trim();
            }
            switch (key) {
                case "offset_ms" -> offsetMs = Long.parseLong(value);
                case "task_id" -> taskId = value;
                case "type" -> type = TaskType.valueOf(value);
                case "input" -> input = value;
                case "priority" -> priority = Integer.parseInt(value);
                case "timeout_ms" -> timeoutMs = Long.parseLong(value);
                default -> {
                    // Forward-compatible: ignore keys a newer generator wrote.
                }
            }
            i = skipWs(line, i);
            if (i < line.length() && line.charAt(i) == ',') {
                i++;
            }
        }
        if (taskId == null || type == null || input == null) {
            throw new IllegalArgumentException("trace line misses task_id/type/input: " + line);
        }
        return new TraceEntry(offsetMs, taskId, type, input, priority, timeoutMs);
    }

    static String quote(String raw) {
        StringBuilder out = new StringBuilder(raw.length() + 2);
        out.append('"');
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                case '\b' -> out.append("\\b");
                case '\f' -> out.append("\\f");
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
        return out.toString();
    }

    private static String parseString(String line, int start, int[] end) {
        StringBuilder out = new StringBuilder();
        int i = start + 1;
        while (i < line.length()) {
            char c = line.charAt(i);
            if (c == '"') {
                end[0] = i + 1;
                return out.toString();
            }
            if (c == '\\' && i + 1 < line.length()) {
                char esc = line.charAt(i + 1);
                switch (esc) {
                    case '"' -> out.append('"');
                    case '\\' -> out.append('\\');
                    case 'n' -> out.append('\n');
                    case 'r' -> out.append('\r');
                    case 't' -> out.append('\t');
                    case 'b' -> out.append('\b');
                    case 'f' -> out.append('\f');
                    case 'u' -> {
                        out.append((char) Integer.parseInt(line.substring(i + 2, i + 6), 16));
                        i += 4;
                    }
                    default -> out.append(esc);
                }
                i += 2;
            } else {
                out.append(c);
                i++;
            }
        }
        throw new IllegalArgumentException("unterminated string in: " + line);
    }

    private static int skipWs(String line, int i) {
        while (i < line.length() && Character.isWhitespace(line.charAt(i))) {
            i++;
        }
        return i;
    }
}
