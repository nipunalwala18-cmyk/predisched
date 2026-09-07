package com.predisched.worker;

import com.predisched.proto.TaskRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerNodeTest {

    @Test
    @DisplayName("1. Test single task executes successfully")
    public void testSingleTaskExecution() throws Exception {
        WorkerNode worker = new WorkerNode("TestWorker", 2);

        TaskRequest request = TaskRequest.newBuilder()
                .setTaskId("T_SINGLE")
                .setTaskType("CPU_TASK")
                .setInput("1000")
                .setPriority(1)
                .build();

        Future<WorkerTaskExecutor.TaskExecutionResult> future = worker.submitTask(request);
        WorkerTaskExecutor.TaskExecutionResult result = future.get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertEquals("T_SINGLE", result.getTaskId());
        assertEquals("COMPLETED", result.getStatus());
        assertEquals("500500", result.getResult());

        worker.shutdown();
    }

    @Test
    @DisplayName("2. Test multiple tasks execute successfully concurrently")
    public void testMultipleTasksConcurrentExecution() {
        WorkerNode worker = new WorkerNode("TestWorker", 4);

        List<TaskRequest> tasks = List.of(
                TaskRequest.newBuilder().setTaskId("T1").setTaskType("CPU_TASK").setInput("100").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T2").setTaskType("MATRIX_TASK").setInput("2").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T3").setTaskType("SLEEP_TASK").setInput("100").setPriority(1).build()
        );

        List<WorkerTaskExecutor.TaskExecutionResult> results = worker.executeBatchConcurrently(tasks);

        assertEquals(3, results.size());
        for (WorkerTaskExecutor.TaskExecutionResult res : results) {
            assertEquals("COMPLETED", res.getStatus());
        }

        worker.shutdown();
    }

    @Test
    @DisplayName("3. Test thread pool limits concurrent execution")
    public void testThreadPoolLimit() throws Exception {
        // Worker with only 1 thread
        WorkerNode worker = new WorkerNode("SingleThreadWorker", 1);

        TaskRequest t1 = TaskRequest.newBuilder().setTaskId("T_LIMIT_1").setTaskType("SLEEP_TASK").setInput("300").setPriority(1).build();
        TaskRequest t2 = TaskRequest.newBuilder().setTaskId("T_LIMIT_2").setTaskType("SLEEP_TASK").setInput("300").setPriority(1).build();

        long start = System.currentTimeMillis();
        Future<WorkerTaskExecutor.TaskExecutionResult> f1 = worker.submitTask(t1);
        Future<WorkerTaskExecutor.TaskExecutionResult> f2 = worker.submitTask(t2);

        f1.get();
        f2.get();
        long elapsed = System.currentTimeMillis() - start;

        // On 1 thread, 2 × 300ms sleep tasks must execute sequentially (>= 550ms)
        assertTrue(elapsed >= 550, "Sequential execution on 1 thread should take at least 550ms, took: " + elapsed);

        worker.shutdown();
    }

    @Test
    @DisplayName("4. Test four 2-second sleep tasks complete in ~2 seconds with 4 threads")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    public void testFourSleepTasksWithFourThreads() {
        WorkerNode worker = new WorkerNode("FourThreadWorker", 4);

        List<TaskRequest> tasks = List.of(
                TaskRequest.newBuilder().setTaskId("T001").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T002").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T003").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T004").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build()
        );

        long start = System.currentTimeMillis();
        List<WorkerTaskExecutor.TaskExecutionResult> results = worker.executeBatchConcurrently(tasks);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(4, results.size());
        for (WorkerTaskExecutor.TaskExecutionResult res : results) {
            assertEquals("COMPLETED", res.getStatus());
        }

        // Concurrent execution of 4 × 2s tasks on 4 threads should finish in < 3500ms
        assertTrue(elapsed < 3500, "Four 2s tasks on 4 threads should complete in <3500ms, took: " + elapsed + " ms");

        worker.shutdown();
    }

    @Test
    @DisplayName("5. Test all task statuses updated correctly")
    public void testTaskStatusUpdates() {
        WorkerNode worker = new WorkerNode("StatusWorker", 2);

        TaskRequest validTask = TaskRequest.newBuilder().setTaskId("T_VALID").setTaskType("CPU_TASK").setInput("50").setPriority(1).build();
        TaskRequest invalidTask = TaskRequest.newBuilder().setTaskId("T_INVALID").setTaskType("UNKNOWN_TASK").setInput("50").setPriority(1).build();

        List<WorkerTaskExecutor.TaskExecutionResult> results = worker.executeBatchConcurrently(List.of(validTask, invalidTask));

        assertEquals(2, results.size());
        assertEquals("COMPLETED", results.get(0).getStatus());
        assertEquals("FAILED", results.get(1).getStatus());

        worker.shutdown();
    }

    @Test
    @DisplayName("6. Test no task is lost in batch submission")
    public void testNoTaskLost() {
        WorkerNode worker = new WorkerNode("BatchWorker", 4);
        int taskCount = 10;
        List<TaskRequest> tasks = new ArrayList<>();

        for (int i = 1; i <= taskCount; i++) {
            tasks.add(TaskRequest.newBuilder()
                    .setTaskId("T_BATCH_" + i)
                    .setTaskType("CPU_TASK")
                    .setInput("100")
                    .setPriority(1)
                    .build());
        }

        List<WorkerTaskExecutor.TaskExecutionResult> results = worker.executeBatchConcurrently(tasks);
        assertEquals(taskCount, results.size(), "All 10 submitted tasks must be returned");

        worker.shutdown();
    }

    @Test
    @DisplayName("7. Test invalid input handling")
    public void testInvalidInputHandling() throws Exception {
        WorkerNode worker = new WorkerNode("ValidationWorker", 2);

        TaskRequest emptyIdTask = TaskRequest.newBuilder().setTaskId("").setTaskType("CPU_TASK").setInput("100").setPriority(1).build();
        TaskRequest badInputTask = TaskRequest.newBuilder().setTaskId("T_BAD").setTaskType("CPU_TASK").setInput("invalid_number").setPriority(1).build();

        WorkerTaskExecutor.TaskExecutionResult r1 = worker.submitTask(emptyIdTask).get();
        WorkerTaskExecutor.TaskExecutionResult r2 = worker.submitTask(badInputTask).get();

        assertEquals("FAILED", r1.getStatus());
        assertEquals("Invalid task ID", r1.getMessage());

        assertEquals("FAILED", r2.getStatus());
        assertTrue(r2.getMessage().contains("expected a numeric value"));

        worker.shutdown();
    }
}
