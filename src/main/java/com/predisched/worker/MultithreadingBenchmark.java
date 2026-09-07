package com.predisched.worker;

import com.predisched.proto.TaskRequest;

import java.util.ArrayList;
import java.util.List;

/**
 * ================================================================
 *  PrediSched — Experiment 2: Multithreading in a Distributed System
 *  MultithreadingBenchmark.java — Performance & Speedup Benchmark
 * ================================================================
 *
 *  Compares concurrent execution of four 2000ms SLEEP_TASK tasks across:
 *   - 1 Thread
 *   - 2 Threads
 *   - 4 Threads
 *
 *  Measures total time, average task time, throughput, and speedup.
 */
public class MultithreadingBenchmark {

    public static class BenchmarkResult {
        private final int threadCount;
        private final long totalTimeMs;
        private final double avgTaskTimeMs;
        private final double throughput; // tasks per second
        private final double speedup;

        public BenchmarkResult(int threadCount, long totalTimeMs, double avgTaskTimeMs, double throughput, double speedup) {
            this.threadCount = threadCount;
            this.totalTimeMs = totalTimeMs;
            this.avgTaskTimeMs = avgTaskTimeMs;
            this.throughput = throughput;
            this.speedup = speedup;
        }

        public int getThreadCount() { return threadCount; }
        public long getTotalTimeMs() { return totalTimeMs; }
        public double getAvgTaskTimeMs() { return avgTaskTimeMs; }
        public double getThroughput() { return throughput; }
        public double getSpeedup() { return speedup; }
    }

    public static void main(String[] args) {
        System.out.println();
        System.out.println("========================================================================================");
        System.out.println("                     PREDISCHED MULTITHREADING BENCHMARK (Exp 2)                        ");
        System.out.println("========================================================================================");
        System.out.println("Workload: 4 tasks × SLEEP_TASK (2000 ms duration each)");
        System.out.println("Testing thread counts: 1 Thread, 2 Threads, 4 Threads");
        System.out.println();

        int[] threadCounts = {1, 2, 4};
        List<BenchmarkResult> results = new ArrayList<>();
        long baselineTotalTime = 0;

        for (int threads : threadCounts) {
            System.out.println("── Running Benchmark with " + threads + " Thread(s)... ─────────────────────────────────");

            WorkerNode worker = new WorkerNode("Worker-Benchmark", threads);

            List<TaskRequest> tasks = List.of(
                    TaskRequest.newBuilder().setTaskId("T001").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                    TaskRequest.newBuilder().setTaskId("T002").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                    TaskRequest.newBuilder().setTaskId("T003").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build(),
                    TaskRequest.newBuilder().setTaskId("T004").setTaskType("SLEEP_TASK").setInput("2000").setPriority(1).build()
            );

            long startTime = System.currentTimeMillis();
            List<WorkerTaskExecutor.TaskExecutionResult> executionResults = worker.executeBatchConcurrently(tasks);
            long endTime = System.currentTimeMillis();
            long totalTimeMs = endTime - startTime;

            if (threads == 1) {
                baselineTotalTime = totalTimeMs;
            }

            long totalIndividualTaskTime = 0;
            for (WorkerTaskExecutor.TaskExecutionResult r : executionResults) {
                totalIndividualTaskTime += r.getExecutionTime();
            }

            double avgTaskTimeMs = (double) totalIndividualTaskTime / tasks.size();
            double totalTimeSec = (double) totalTimeMs / 1000.0;
            double throughput = tasks.size() / totalTimeSec;
            double speedup = (double) baselineTotalTime / totalTimeMs;

            results.add(new BenchmarkResult(threads, totalTimeMs, avgTaskTimeMs, throughput, speedup));
            worker.shutdown();
        }

        // Print Comparison Summary Table
        System.out.println();
        System.out.println("========================================================================================");
        System.out.println("                               BENCHMARK COMPARISON TABLE                               ");
        System.out.println("========================================================================================");
        System.out.printf("%-15s | %-16s | %-18s | %-22s | %-10s%n",
                "Thread Count", "Total Time (ms)", "Avg Task Time (ms)", "Throughput (tasks/sec)", "Speedup");
        System.out.println("----------------------------------------------------------------------------------------");

        for (BenchmarkResult res : results) {
            System.out.printf("%-15s | %-16d | %-18.2f | %-22.2f | %-10.2fx%n",
                    res.getThreadCount() + " Thread(s)",
                    res.getTotalTimeMs(),
                    res.getAvgTaskTimeMs(),
                    res.getThroughput(),
                    res.getSpeedup());
        }

        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("Conclusion:");
        System.out.println("  1 Thread  : Sequential execution (~8000 ms total for 4 × 2s tasks)");
        System.out.println("  2 Threads : 2-way concurrent execution (~4000 ms total)");
        System.out.println("  4 Threads : 4-way concurrent execution (~2000 ms total)");
        System.out.println("Increasing worker thread pool size allows independent tasks to execute in parallel,");
        System.out.println("scaling throughput linearly with available threads.");
        System.out.println("========================================================================================");
        System.out.println();
    }
}
