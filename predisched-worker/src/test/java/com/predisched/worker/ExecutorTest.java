package com.predisched.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.ExecutionResult;
import com.predisched.proto.TaskType;
import org.junit.jupiter.api.Test;

public class ExecutorTest {

    private final ExecutorRegistry registry = new ExecutorRegistry();

    @Test
    public void cpuExecutorCountsPrimesDeterministically() {
        CpuTaskExecutor executor = new CpuTaskExecutor();
        ExecutionResult first = executor.execute("n=100");
        ExecutionResult second = executor.execute("n=100");
        assertTrue(first.success());
        assertEquals(first.output(), second.output());
        assertEquals("primes_below_100=25", first.output());
    }

    @Test
    public void sleepExecutorSleepsAndReports() {
        SleepTaskExecutor executor = new SleepTaskExecutor();
        ExecutionResult result = executor.execute("ms=5");
        assertTrue(result.success());
        assertEquals("slept_ms=5", result.output());
    }

    @Test
    public void matrixExecutorIsDeterministic() {
        MatrixTaskExecutor executor = new MatrixTaskExecutor();
        ExecutionResult first = executor.execute("size=5");
        ExecutionResult second = executor.execute("size=5");
        assertTrue(first.success());
        assertEquals(first.output(), second.output());
        assertTrue(first.output().startsWith("size=5 checksum="));
    }

    @Test
    public void badInputsBecomeFailedResults() {
        assertFalse(registry.execute(TaskType.CPU_TASK, "n=abc").success());
        assertFalse(registry.execute(TaskType.CPU_TASK, "garbage").success());
        assertFalse(registry.execute(TaskType.CPU_TASK, "").success());
        assertFalse(registry.execute(TaskType.SLEEP_TASK, "ms=abc").success());
        assertFalse(registry.execute(TaskType.MATRIX_TASK, "size=0").success());
        assertFalse(registry.execute(TaskType.MATRIX_TASK, "oops").success());
    }

    @Test
    public void unknownTypeBecomesFailedResult() {
        ExecutionResult result = registry.execute(TaskType.WORKFLOW_TASK, "dag=x.json");
        assertFalse(result.success());
    }
}
