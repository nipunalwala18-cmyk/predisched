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

    private static final Map<TaskType, List<NumericKey>> REQUIRED = new LinkedHashMap<>();
    private static final Map<TaskType, List<NumericKey>> OPTIONAL = new LinkedHashMap<>();
    private static final Map<TaskType, List<FractionKey>> FRACTIONS = new LinkedHashMap<>();

    static {
        REQUIRED.put(TaskType.CPU_TASK, List.of(new NumericKey("n", 2, 100_000_000L)));
        REQUIRED.put(TaskType.SLEEP_TASK, List.of(new NumericKey("ms", 0, 60_000L)));
        REQUIRED.put(TaskType.MATRIX_TASK, List.of(new NumericKey("size", 1, 1_000L)));
        // Optional keys are only range-checked when present.
        OPTIONAL.put(TaskType.MATRIX_TASK, List.of(new NumericKey("threads", 1, 64)));
        // Test hooks for the retry and dead-letter demos (F4): a reproducible failure rate.
        FRACTIONS.put(TaskType.SLEEP_TASK, List.of(new FractionKey("failRate", 0.0, 1.0)));
        OPTIONAL.put(TaskType.SLEEP_TASK, List.of(new NumericKey("seed", 0, Long.MAX_VALUE)));
    }

    private TaskInputSpec() {}

    /** True when this type has an executor's input rules registered. */
    public static boolean isKnown(TaskType type) {
        return REQUIRED.containsKey(type);
    }

    /** The types that have an executor, for error messages. */
    public static List<TaskType> knownTypes() {
        return List.copyOf(REQUIRED.keySet());
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
        return errors;
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
