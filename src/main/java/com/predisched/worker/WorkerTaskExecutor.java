package com.predisched.worker;

import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;

/**
 * ================================================================
 *  PrediSched — Experiment 2: Multithreading in a Distributed System
 *  WorkerTaskExecutor.java — Task Execution Engine
 * ================================================================
 *
 *  Executes computational tasks (CPU_TASK, MATRIX_TASK, SLEEP_TASK)
 *  on worker pool threads, measuring execution time, thread name,
 *  and execution status.
 */
public class WorkerTaskExecutor {

    /**
     * Data structure holding full task execution metadata and metrics.
     */
    public static class TaskExecutionResult {
        private final String taskId;
        private final String taskType;
        private final String workerId;
        private final String threadName;
        private final long startTime;
        private final long endTime;
        private final long executionTime;
        private final String status;
        private final String result;
        private final String message;

        public TaskExecutionResult(String taskId, String taskType, String workerId, String threadName,
                                   long startTime, long endTime, long executionTime,
                                   String status, String result, String message) {
            this.taskId = taskId;
            this.taskType = taskType;
            this.workerId = workerId;
            this.threadName = threadName;
            this.startTime = startTime;
            this.endTime = endTime;
            this.executionTime = executionTime;
            this.status = status;
            this.result = result;
            this.message = message;
        }

        public String getTaskId() { return taskId; }
        public String getTaskType() { return taskType; }
        public String getWorkerId() { return workerId; }
        public String getThreadName() { return threadName; }
        public long getStartTime() { return startTime; }
        public long getEndTime() { return endTime; }
        public long getExecutionTime() { return executionTime; }
        public String getStatus() { return status; }
        public String getResult() { return result; }
        public String getMessage() { return message; }

        public TaskResponse toProtoResponse() {
            return TaskResponse.newBuilder()
                    .setTaskId(taskId != null ? taskId : "")
                    .setStatus(status != null ? status : "FAILED")
                    .setResult(result != null ? result : "")
                    .setMessage(message != null ? message : "")
                    .build();
        }

        @Override
        public String toString() {
            return String.format("[%s] Task ID: %s | Type: %s | Worker: %s | Thread: %s | Status: %s | Execution Time: %d ms | Result: %s",
                    status, taskId, taskType, workerId, threadName, status, executionTime, result);
        }
    }

    /**
     * Executes a single TaskRequest and records execution metrics.
     */
    public static TaskExecutionResult execute(TaskRequest request, String workerId) {
        String taskId = request.getTaskId();
        String taskType = request.getTaskType();
        String input = request.getInput();
        String threadName = Thread.currentThread().getName();
        long startTime = System.currentTimeMillis();

        if (taskId == null || taskId.trim().isEmpty()) {
            long endTime = System.currentTimeMillis();
            return new TaskExecutionResult("", taskType, workerId, threadName,
                    startTime, endTime, endTime - startTime, "FAILED", "", "Invalid task ID");
        }

        if (taskType == null || taskType.trim().isEmpty()) {
            long endTime = System.currentTimeMillis();
            return new TaskExecutionResult(taskId, "", workerId, threadName,
                    startTime, endTime, endTime - startTime, "FAILED", "", "Task type is required");
        }

        switch (taskType.toUpperCase()) {
            case "CPU_TASK":
                return executeCpuTask(taskId, input, workerId, threadName, startTime);

            case "MATRIX_TASK":
                return executeMatrixTask(taskId, input, workerId, threadName, startTime);

            case "SLEEP_TASK":
                return executeSleepTask(taskId, input, workerId, threadName, startTime);

            default:
                long endTime = System.currentTimeMillis();
                return new TaskExecutionResult(taskId, taskType, workerId, threadName,
                        startTime, endTime, endTime - startTime, "FAILED", "", "Unsupported task type: " + taskType);
        }
    }

    // ──────────────────────────────────────────────────
    //  CPU_TASK — Sum of numbers from 1 to N
    // ──────────────────────────────────────────────────
    private static TaskExecutionResult executeCpuTask(String taskId, String input, String workerId, String threadName, long startTime) {
        try {
            long n = Long.parseLong(input.trim());

            if (n <= 0) {
                long endTime = System.currentTimeMillis();
                return new TaskExecutionResult(taskId, "CPU_TASK", workerId, threadName,
                        startTime, endTime, endTime - startTime, "FAILED", "", "Invalid input: N must be a positive number");
            }

            long sum = 0;
            for (long i = 1; i <= n; i++) {
                sum += i;
            }

            long endTime = System.currentTimeMillis();
            long elapsed = endTime - startTime;

            return new TaskExecutionResult(taskId, "CPU_TASK", workerId, threadName,
                    startTime, endTime, elapsed, "COMPLETED", String.valueOf(sum),
                    "CPU task completed in " + elapsed + " ms. Computed sum of 1 to " + n + ".");

        } catch (NumberFormatException e) {
            long endTime = System.currentTimeMillis();
            return new TaskExecutionResult(taskId, "CPU_TASK", workerId, threadName,
                    startTime, endTime, endTime - startTime, "FAILED", "", "Invalid input: expected a numeric value, got '" + input + "'");
        }
    }

    // ──────────────────────────────────────────────────
    //  MATRIX_TASK — NxN Matrix Multiplication
    // ──────────────────────────────────────────────────
    private static TaskExecutionResult executeMatrixTask(String taskId, String input, String workerId, String threadName, long startTime) {
        try {
            int n = Integer.parseInt(input.trim());

            if (n <= 0 || n > 10) {
                long endTime = System.currentTimeMillis();
                return new TaskExecutionResult(taskId, "MATRIX_TASK", workerId, threadName,
                        startTime, endTime, endTime - startTime, "FAILED", "", "Invalid input: matrix size must be between 1 and 10");
            }

            // Create Matrix A with sequential values starting at 1
            int[][] matA = new int[n][n];
            int val = 1;
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    matA[i][j] = val++;
                }
            }

            // Create Matrix B with sequential values continuing from A
            int[][] matB = new int[n][n];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    matB[i][j] = val++;
                }
            }

            // Multiply: C = A × B
            int[][] matC = new int[n][n];
            for (int i = 0; i < n; i++) {
                for (int j = 0; j < n; j++) {
                    matC[i][j] = 0;
                    for (int k = 0; k < n; k++) {
                        matC[i][j] += matA[i][k] * matB[k][j];
                    }
                }
            }

            long endTime = System.currentTimeMillis();
            long elapsed = endTime - startTime;

            StringBuilder resultStr = new StringBuilder();
            resultStr.append("A(").append(n).append("x").append(n).append(") × ");
            resultStr.append("B(").append(n).append("x").append(n).append(") = ");
            resultStr.append(matrixToString(matC, n));

            return new TaskExecutionResult(taskId, "MATRIX_TASK", workerId, threadName,
                    startTime, endTime, elapsed, "COMPLETED", resultStr.toString(),
                    "Matrix multiplication completed in " + elapsed + " ms.");

        } catch (NumberFormatException e) {
            long endTime = System.currentTimeMillis();
            return new TaskExecutionResult(taskId, "MATRIX_TASK", workerId, threadName,
                    startTime, endTime, endTime - startTime, "FAILED", "", "Invalid input: expected a numeric value, got '" + input + "'");
        }
    }

    private static String matrixToString(int[][] matrix, int n) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < n; i++) {
            sb.append("[");
            for (int j = 0; j < n; j++) {
                sb.append(matrix[i][j]);
                if (j < n - 1) sb.append(", ");
            }
            sb.append("]");
            if (i < n - 1) sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    // ──────────────────────────────────────────────────
    //  SLEEP_TASK — Simulate Work
    // ──────────────────────────────────────────────────
    private static TaskExecutionResult executeSleepTask(String taskId, String input, String workerId, String threadName, long startTime) {
        try {
            long sleepMs = Long.parseLong(input.trim());

            if (sleepMs <= 0 || sleepMs > 30000) {
                long endTime = System.currentTimeMillis();
                return new TaskExecutionResult(taskId, "SLEEP_TASK", workerId, threadName,
                        startTime, endTime, endTime - startTime, "FAILED", "", "Invalid input: sleep duration must be between 1 and 30000 ms");
            }

            Thread.sleep(sleepMs);
            long endTime = System.currentTimeMillis();
            long elapsed = endTime - startTime;

            return new TaskExecutionResult(taskId, "SLEEP_TASK", workerId, threadName,
                    startTime, endTime, elapsed, "COMPLETED", "Slept for " + elapsed + " ms",
                    "Sleep task completed after " + elapsed + " ms.");

        } catch (NumberFormatException e) {
            long endTime = System.currentTimeMillis();
            return new TaskExecutionResult(taskId, "SLEEP_TASK", workerId, threadName,
                    startTime, endTime, endTime - startTime, "FAILED", "", "Invalid input: expected a numeric value, got '" + input + "'");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            long endTime = System.currentTimeMillis();
            return new TaskExecutionResult(taskId, "SLEEP_TASK", workerId, threadName,
                    startTime, endTime, endTime - startTime, "FAILED", "", "Sleep task was interrupted");
        }
    }
}
