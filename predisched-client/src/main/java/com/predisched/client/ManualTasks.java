package com.predisched.client;

import com.predisched.common.TaskInputSpec;
import com.predisched.proto.TaskType;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tasks typed by hand: the one-line format shared by {@code submit-file} and {@code shell}, and a
 * copyable example per task type for {@code predisched tasks}.
 *
 * <pre>
 *   CPU_TASK 5 n=2000000              type, priority (1-10), input
 *   SORT_TASK n=100000, type=reversed priority left out: {@value #DEFAULT_PRIORITY}
 *   # a comment; blank lines are skipped
 * </pre>
 */
public final class ManualTasks {

    public static final int DEFAULT_PRIORITY = 5;

    /** One example input per type that has an executor, runnable as is on a local cluster. */
    public static final Map<TaskType, String> EXAMPLES = new LinkedHashMap<>();

    static {
        EXAMPLES.put(TaskType.CPU_TASK, "n=2000000");
        EXAMPLES.put(TaskType.SLEEP_TASK, "ms=500");
        EXAMPLES.put(TaskType.MATRIX_TASK, "size=200");
        EXAMPLES.put(TaskType.HASH_TASK, "rounds=100000");
        EXAMPLES.put(TaskType.MONTE_CARLO_TASK, "samples=1000000, seed=7");
        EXAMPLES.put(TaskType.SORT_TASK, "n=100000, type=random");
        EXAMPLES.put(TaskType.COMPRESS_TASK, "size_mb=4, level=6");
        EXAMPLES.put(TaskType.GRAPH_TASK, "nodes=10000, algo=bfs");
        EXAMPLES.put(TaskType.FILE_IO_TASK, "size_mb=8, mode=both");
        EXAMPLES.put(TaskType.HTTP_TASK, "url=http://localhost:8099/, timeout=2000");
        EXAMPLES.put(TaskType.DB_QUERY_TASK, "rows=1000");
        EXAMPLES.put(TaskType.WORKFLOW_TASK, "dag=workloads/dags/map-reduce.json");
        EXAMPLES.put(TaskType.MAPREDUCE_TASK, "dataset=spark/data/execution_history.csv, job=avg_exec");
        EXAMPLES.put(TaskType.ML_INFER_TASK, "model=exec_time, batch=1000");
    }

    /** A parsed line: a task to submit, or why the line is not one. */
    public record Line(TaskType type, int priority, String input, String error) {

        public boolean ok() {
            return error == null;
        }

        static Line error(String message) {
            return new Line(null, 0, null, message);
        }
    }

    private ManualTasks() {}

    /** True for lines to skip: blank or starting with {@code #}. */
    public static boolean skip(String line) {
        String trimmed = line.strip();
        return trimmed.isEmpty() || trimmed.startsWith("#");
    }

    /**
     * Parses {@code TYPE [priority] input}. The type is case-insensitive and may leave out the
     * {@code _TASK} suffix ({@code cpu 5 n=100}). The input is validated with the same rules the
     * scheduler applies, so mistakes show before anything is sent.
     */
    public static Line parse(String line) {
        String[] head = line.strip().split("\s+", 2);
        if (head[0].isEmpty()) {
            return Line.error("empty line");
        }
        TaskType type = typeOf(head[0]);
        if (type == null) {
            return Line.error("unknown task type '" + head[0] + "' (run 'predisched tasks')");
        }
        String rest = head.length > 1 ? head[1].strip() : "";
        int priority = DEFAULT_PRIORITY;
        String[] parts = rest.split("\s+", 2);
        if (!parts[0].isEmpty() && parts[0].chars().allMatch(Character::isDigit)) {
            priority = Integer.parseInt(parts[0]);
            rest = parts.length > 1 ? parts[1].strip() : "";
            if (priority < 1 || priority > 10) {
                return Line.error("priority must be 1-10 (got " + priority + ")");
            }
        }
        if (rest.isEmpty()) {
            return Line.error(type + " needs an input: " + TaskInputSpec.describe(type)
                    + (EXAMPLES.containsKey(type) ? " (e.g. " + EXAMPLES.get(type) + ")" : ""));
        }
        String error = TaskInputSpec.firstError(type, rest);
        if (error != null) {
            return Line.error(type + ": " + error);
        }
        return new Line(type, priority, rest, null);
    }

    /** {@code CPU_TASK}, {@code cpu_task} or {@code cpu}; null when no such type exists. */
    static TaskType typeOf(String name) {
        String upper = name.toUpperCase(Locale.ROOT);
        for (String candidate : List.of(upper, upper + "_TASK")) {
            try {
                TaskType type = TaskType.valueOf(candidate);
                return type == TaskType.UNRECOGNIZED ? null : type;
            } catch (IllegalArgumentException ignored) {
                // try the next spelling
            }
        }
        return null;
    }
}
