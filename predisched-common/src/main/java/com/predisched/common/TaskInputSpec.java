package com.predisched.common;

import com.predisched.proto.TaskType;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-type input rules: which keys a task type needs and the range each numeric value may take.
 *
 * <p>The scheduler uses this to reject malformed input at submit time (FR2) and the worker's
 * executors use it before doing any work, so both sides agree on what "well-formed" means without
 * the scheduler importing worker classes.
 *
 * <p>A type with no entry here has no key rules: its input only has to parse as
 * {@code key=value[, key=value]}. Types arrive as their executors do (prompts 05 onwards).
 */
public final class TaskInputSpec {

    /** One required numeric key and the inclusive range its value must fall in. */
    public record NumericKey(String key, long min, long max) {}

    /** An optional key whose value is a fraction, such as a failure rate. */
    public record FractionKey(String key, double min, double max) {}

    /** A key whose value must be one of a fixed set, such as {@code type=random|sorted|reversed}. */
    public record EnumKey(String key, List<String> allowed) {}

    /** A key whose value must be an http(s) URL, such as an {@code HTTP_TASK} target. */
    public record UrlKey(String key) {}

    private static final Map<TaskType, List<NumericKey>> REQUIRED = new LinkedHashMap<>();
    private static final Map<TaskType, List<NumericKey>> OPTIONAL = new LinkedHashMap<>();
    private static final Map<TaskType, List<FractionKey>> FRACTIONS = new LinkedHashMap<>();
    private static final Map<TaskType, List<EnumKey>> REQUIRED_ENUMS = new LinkedHashMap<>();
    private static final Map<TaskType, List<EnumKey>> OPTIONAL_ENUMS = new LinkedHashMap<>();
    private static final Map<TaskType, List<UrlKey>> REQUIRED_URLS = new LinkedHashMap<>();
    /** Keys whose value is free text, such as a file path; only presence is checked. */
    private static final Map<TaskType, List<String>> REQUIRED_TEXT = new LinkedHashMap<>();

    /**
     * Types with no executor yet: four arrive with later prompts and one stays reserved.
     * A submit of any of these is rejected by validation with this reason.
     */
    private static final Map<TaskType, String> RESERVED = new LinkedHashMap<>();

    static {
        REQUIRED.put(TaskType.CPU_TASK, List.of(new NumericKey("n", 2, 100_000_000L)));
        REQUIRED.put(TaskType.SLEEP_TASK, List.of(new NumericKey("ms", 0, 60_000L)));
        REQUIRED.put(TaskType.MATRIX_TASK, List.of(new NumericKey("size", 1, 1_000L)));
        // Optional keys are only range-checked when present.
        OPTIONAL.put(TaskType.MATRIX_TASK, List.of(new NumericKey("threads", 1, 64)));
        // Test hooks for the retry and dead-letter demos (F4): a reproducible failure rate.
        FRACTIONS.put(TaskType.SLEEP_TASK, List.of(new FractionKey("failRate", 0.0, 1.0)));
        OPTIONAL.put(TaskType.SLEEP_TASK, List.of(new NumericKey("seed", 0, Long.MAX_VALUE)));
        // Full catalogue (prompt 05, spec section 7.2).
        REQUIRED.put(TaskType.HASH_TASK, List.of(new NumericKey("rounds", 1, 20_000_000L)));
        REQUIRED.put(TaskType.MONTE_CARLO_TASK, List.of(new NumericKey("samples", 1, 500_000_000L)));
        OPTIONAL.put(TaskType.MONTE_CARLO_TASK, List.of(new NumericKey("seed", 0, Long.MAX_VALUE)));
        REQUIRED.put(TaskType.SORT_TASK, List.of(new NumericKey("n", 2, 20_000_000L)));
        OPTIONAL_ENUMS.put(TaskType.SORT_TASK,
                List.of(new EnumKey("type", List.of("random", "sorted", "reversed"))));
        REQUIRED.put(TaskType.COMPRESS_TASK, List.of(new NumericKey("size_mb", 1, 512L)));
        OPTIONAL.put(TaskType.COMPRESS_TASK, List.of(new NumericKey("level", 1, 9)));
        REQUIRED.put(TaskType.GRAPH_TASK, List.of(new NumericKey("nodes", 2, 5_000_000L)));
        OPTIONAL_ENUMS.put(TaskType.GRAPH_TASK,
                List.of(new EnumKey("algo", List.of("bfs", "pagerank"))));
        OPTIONAL.put(TaskType.GRAPH_TASK, List.of(new NumericKey("seed", 0, Long.MAX_VALUE)));
        REQUIRED.put(TaskType.FILE_IO_TASK, List.of(new NumericKey("size_mb", 1, 2048L)));
        OPTIONAL_ENUMS.put(TaskType.FILE_IO_TASK,
                List.of(new EnumKey("mode", List.of("write", "read", "both"))));
        REQUIRED.put(TaskType.HTTP_TASK, List.of(new NumericKey("timeout", 1, 120_000L)));
        REQUIRED_URLS.put(TaskType.HTTP_TASK, List.of(new UrlKey("url")));
        // Workflows (prompt 09, F1): the scheduler expands it; no worker runs it.
        REQUIRED_TEXT.put(TaskType.WORKFLOW_TASK, List.of("dag"));
        RESERVED.put(TaskType.DB_QUERY_TASK, "arrives in prompt 11 (PostgreSQL persistence)");
        RESERVED.put(TaskType.MAPREDUCE_TASK, "arrives in prompt 12 (Spark MapReduce)");
        RESERVED.put(TaskType.ML_INFER_TASK, "arrives in prompt 16 (ML models)");
        RESERVED.put(TaskType.IMAGE_TASK, "reserved: in the enum but not in the catalogue (spec 7.2)");
    }

    private TaskInputSpec() {}

    /** True when this type has an executor's input rules registered. */
    public static boolean isKnown(TaskType type) {
        return REQUIRED.containsKey(type) || REQUIRED_TEXT.containsKey(type);
    }

    /**
     * Why a type with no executor cannot run yet, or null when it simply has no executor.
     * Used by validation so a submit of a later prompt's type says where it arrives.
     */
    public static String reservedReason(TaskType type) {
        return RESERVED.get(type);
    }

    /** The types that have an executor, for error messages. */
    public static List<TaskType> knownTypes() {
        List<TaskType> known = new ArrayList<>(REQUIRED.keySet());
        known.addAll(REQUIRED_TEXT.keySet());
        return List.copyOf(known);
    }

    /**
     * Returns every problem with this input for this type: bad format, missing key, non-numeric
     * value, or a value outside its range. An empty list means the input is well-formed.
     */
    public static List<String> validate(TaskType type, String input) {
        List<String> errors = new ArrayList<>();
        Map<String, String> params;
        try {
            params = InputParser.parse(input);
        } catch (IllegalArgumentException e) {
            errors.add("input must be well-formed key=value[, key=value]: " + e.getMessage());
            return errors;
        }
        for (NumericKey optional : OPTIONAL.getOrDefault(type, List.of())) {
            if (params.containsKey(optional.key())) {
                checkNumeric(params.get(optional.key()), optional, errors);
            }
        }
        for (FractionKey fraction : FRACTIONS.getOrDefault(type, List.of())) {
            if (params.containsKey(fraction.key())) {
                checkFraction(params.get(fraction.key()), fraction, errors);
            }
        }
        for (NumericKey required : REQUIRED.getOrDefault(type, List.of())) {
            String raw = params.get(required.key());
            if (raw == null || raw.isEmpty()) {
                errors.add(type + " requires '" + required.key() + "=<number>'");
                continue;
            }
            checkNumeric(raw, required, errors);
        }
        for (EnumKey required : REQUIRED_ENUMS.getOrDefault(type, List.of())) {
            String raw = params.get(required.key());
            if (raw == null || raw.isEmpty()) {
                errors.add(type + " requires '" + required.key() + "="
                        + String.join("|", required.allowed()) + "'");
                continue;
            }
            checkEnum(raw, required, errors);
        }
        for (EnumKey optional : OPTIONAL_ENUMS.getOrDefault(type, List.of())) {
            if (params.containsKey(optional.key())) {
                checkEnum(params.get(optional.key()), optional, errors);
            }
        }
        for (UrlKey required : REQUIRED_URLS.getOrDefault(type, List.of())) {
            String raw = params.get(required.key());
            if (raw == null || raw.isEmpty()) {
                errors.add(type + " requires '" + required.key() + "=<http(s) url>'");
                continue;
            }
            checkUrl(raw, required, errors);
        }
        for (String required : REQUIRED_TEXT.getOrDefault(type, List.of())) {
            String raw = params.get(required);
            if (raw == null || raw.isEmpty()) {
                errors.add(type + " requires '" + required + "=<value>'");
            }
        }
        return errors;
    }

    private static void checkEnum(String raw, EnumKey key, List<String> errors) {
        if (!key.allowed().contains(raw)) {
            errors.add(key.key() + " must be one of " + key.allowed() + " (got '" + raw + "')");
        }
    }

    private static void checkUrl(String raw, UrlKey key, List<String> errors) {
        if (!(raw.startsWith("http://") || raw.startsWith("https://")) || raw.length() < 10) {
            errors.add(key.key() + " must be an http(s) URL (got '" + raw + "')");
        }
    }

    private static void checkFraction(String raw, FractionKey key, List<String> errors) {
        final double value;
        try {
            value = Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            errors.add(key.key() + " must be a number (got '" + raw + "')");
            return;
        }
        if (value < key.min() || value > key.max()) {
            errors.add(key.key() + " must be between " + key.min() + " and " + key.max()
                    + " (got " + value + ")");
        }
    }

    private static void checkNumeric(String raw, NumericKey key, List<String> errors) {
        final long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            errors.add(key.key() + " must be a number (got '" + raw + "')");
            return;
        }
        if (value < key.min() || value > key.max()) {
            errors.add(key.key() + " must be between " + key.min() + " and " + key.max()
                    + " (got " + value + ")");
        }
    }

    /** The same rules as a single message, for executors that fail fast. Null when valid. */
    public static String firstError(TaskType type, String input) {
        List<String> errors = validate(type, input);
        return errors.isEmpty() ? null : String.join("; ", errors);
    }
}
