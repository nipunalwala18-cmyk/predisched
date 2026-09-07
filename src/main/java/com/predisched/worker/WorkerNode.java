package com.predisched.worker;

import com.predisched.proto.TaskRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * ================================================================
 *  PrediSched — Experiment 2: Multithreading in a Distributed System
 *  WorkerNode.java — Multithreaded Worker Node Component
 * ================================================================
 *
 *  Manages a configurable thread pool (ExecutorService) to execute
 *  computational tasks concurrently in a distributed architecture.
 */
public class WorkerNode {

    private final String workerId;
    private final int threadCount;
    private final ExecutorService threadPool;

    public WorkerNode() {
        this("Worker-1", 4);
    }

    public WorkerNode(int threadCount) {
        this("Worker-1", threadCount);
    }

    public WorkerNode(String workerId, int threadCount) {
        if (threadCount <= 0) {
            throw new IllegalArgumentException("Thread count must be greater than 0");
        }
        this.workerId = workerId;
        this.threadCount = threadCount;
        this.threadPool = Executors.newFixedThreadPool(threadCount, new ThreadFactory() {
            private int counter = 1;
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "Thread-" + counter++);
            }
        });
    }

    public String getWorkerId() {
        return workerId;
    }

    public int getThreadCount() {
        return threadCount;
    }

    /**
     * Submits a single TaskRequest to the worker's thread pool for execution.
     * Returns a Future containing the execution result.
     */
    public Future<WorkerTaskExecutor.TaskExecutionResult> submitTask(TaskRequest request) {
        return threadPool.submit(() -> {
            String currentThread = Thread.currentThread().getName();
            System.out.println("  [" + currentThread + "] START " + request.getTaskId() +
                    " | " + request.getTaskType() + " | Input: " + request.getInput());

            WorkerTaskExecutor.TaskExecutionResult result = WorkerTaskExecutor.execute(request, workerId);

            System.out.println("  [" + result.getThreadName() + "] COMPLETE " + result.getTaskId() +
                    " | " + result.getExecutionTime() + " ms | Status: " + result.getStatus());

            return result;
        });
    }

    /**
     * Submits a batch of tasks to the worker and waits for all of them to complete concurrently.
     * Displays a clean console execution summary.
     */
    public List<WorkerTaskExecutor.TaskExecutionResult> executeBatchConcurrently(List<TaskRequest> tasks) {
        System.out.println();
        System.out.println("========================================");
        System.out.println("       PrediSched Worker Node           ");
        System.out.println("========================================");
        System.out.println("Worker ID       : " + workerId);
        System.out.println("Thread Pool     : " + threadCount + " threads");
        System.out.println("Status          : ONLINE");
        System.out.println();
        System.out.println("Task Batch Submitted (" + tasks.size() + " tasks):");

        for (TaskRequest task : tasks) {
            System.out.println("  Task received: " + task.getTaskId() + " (" + task.getTaskType() + ")");
        }
        System.out.println();

        long batchStartTime = System.currentTimeMillis();
        List<Future<WorkerTaskExecutor.TaskExecutionResult>> futures = new ArrayList<>();

        for (TaskRequest task : tasks) {
            futures.add(submitTask(task));
        }

        List<WorkerTaskExecutor.TaskExecutionResult> results = new ArrayList<>();
        int completedCount = 0;
        int failedCount = 0;

        for (Future<WorkerTaskExecutor.TaskExecutionResult> future : futures) {
            try {
                WorkerTaskExecutor.TaskExecutionResult res = future.get();
                results.add(res);
                if ("COMPLETED".equals(res.getStatus())) {
                    completedCount++;
                } else {
                    failedCount++;
                }
            } catch (InterruptedException | ExecutionException e) {
                failedCount++;
            }
        }

        long batchEndTime = System.currentTimeMillis();
        long totalBatchTime = batchEndTime - batchStartTime;

        System.out.println();
        System.out.println("========================================");
        System.out.println("        EXECUTION SUMMARY               ");
        System.out.println("========================================");
        System.out.println("Total Tasks       : " + tasks.size());
        System.out.println("Worker            : " + workerId);
        System.out.println("Thread Pool Size  : " + threadCount);
        System.out.println("Total Time        : ~" + totalBatchTime + " ms");
        System.out.println("Completed         : " + completedCount);
        System.out.println("Failed            : " + failedCount);
        System.out.println("========================================");
        System.out.println();

        return results;
    }

    /**
     * Shuts down the thread pool gracefully.
     */
    public void shutdown() {
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Main entry point to launch WorkerNode directly with configurable thread count.
     */
    public static void main(String[] args) {
        int threads = 4;
        if (args.length > 0) {
            try {
                threads = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid thread count: " + args[0] + ". Using default of 4 threads.");
            }
        }

        WorkerNode worker = new WorkerNode("Worker-1", threads);

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║       PrediSched Worker Node (Exp 2)         ║");
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.println("║  Worker ID   : " + String.format("%-29s", worker.getWorkerId()) + "║");
        System.out.println("║  Thread Pool : " + String.format("%-29s", threads + " threads") + "║");
        System.out.println("║  Status      : ONLINE                        ║");
        System.out.println("╚══════════════════════════════════════════════╝");

        // Run sample demo batch with 4 SLEEP_TASKs (2000 ms each)
        List<TaskRequest> sampleBatch = List.of(
                TaskRequest.newBuilder().setTaskId("T001").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T002").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T003").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                TaskRequest.newBuilder().setTaskId("T004").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build()
        );

        worker.executeBatchConcurrently(sampleBatch);
        worker.shutdown();
    }
}
