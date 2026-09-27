package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.common.TaskExecutor;
import com.predisched.proto.TaskType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * The Java executors produce exactly the outputs in {@code testdata/task-outputs.json}, which
 * {@code mpi/tests/test_tasks.py} also asserts for the Python versions (prompt 13), so a batch
 * run under MPI can be cross-checked against the scheduler.
 */
class TaskOutputsFixtureTest {

    /** One fixture object per line: {"type": .., "input": .., "success": .., "output": ..}. */
    private static final Pattern CASE = Pattern.compile(
            "\\{\"type\": \"(\\w+)\", \"input\": \"([^\"]*)\", \"success\": (true|false),"
                    + " \"output\": \"([^\"]*)\"}");

    @Test
    void javaExecutorsMatchTheSharedFixtures() throws Exception {
        Path fixture = Paths.get("..", "testdata", "task-outputs.json");
        int checked = 0;
        for (String line : Files.readAllLines(fixture, StandardCharsets.UTF_8)) {
            Matcher m = CASE.matcher(line);
            if (!m.find()) {
                continue;
            }
            TaskType type = TaskType.valueOf(m.group(1));
            // A fresh executor per case: SleepTaskExecutor's failure draw depends on the attempt.
            TaskExecutor executor = switch (type) {
                case CPU_TASK -> new CpuTaskExecutor();
                case HASH_TASK -> new HashTaskExecutor();
                case MONTE_CARLO_TASK -> new MonteCarloTaskExecutor();
                case SLEEP_TASK -> new SleepTaskExecutor();
                case MATRIX_TASK -> new MatrixTaskExecutor();
                default -> throw new AssertionError("no executor for " + type);
            };
            ExecutionResult result = executor.execute(m.group(2));
            boolean success = Boolean.parseBoolean(m.group(3));
            assertEquals(success, result.success(), line);
            assertEquals(m.group(4), success ? result.output() : result.errorMessage(), line);
            checked++;
        }
        assertTrue(checked >= 16, "only " + checked + " fixture cases read");
    }
}
