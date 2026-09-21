package com.predisched.benchmark;

import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.RegisterRequest;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskStatus;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.Dispatcher;
import com.predisched.scheduler.SchedulerServiceImpl;
import com.predisched.scheduler.WorkerRegistry;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.worker.ExecutionEngine;
import com.predisched.worker.ExecutorRegistry;
import com.predisched.worker.WorkerMetrics;
import com.predisched.worker.WorkerServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Measures what the worker's thread pool buys (lab Exp 2).
 *
 * <p>The same batch of CPU tasks is submitted by several concurrent clients against a scheduler and
 * one worker, once per pool size. Everything runs in one JVM over in-process gRPC so the numbers
 * are about the pool, not the network.
 */
public class PoolSizeBenchmark {

    private static final int[] POOL_SIZES = {1, 2, 4, 8};
    private static final int CLIENT_THREADS = 4;

    private final int tasks;
    private final String input;
    private final Path output;
    private final int reps;

    public PoolSizeBenchmark(int tasks, String input, Path output, int reps) {
        this.tasks = tasks;
        this.input = input;
        this.output = output;
        this.reps = reps;
    }

    /** One measured repetition. */
    public record Run(
            int poolSize,
            int rep,
            int tasks,
            long makespanMs,
            double throughputPerSec,
            double meanLatencyMs,
            long p95LatencyMs,
            double avgExecMs) {}

    public List<Run> run() throws Exception {
        // The first batch pays for class loading and JIT compilation, which would otherwise be
        // charged to whichever pool size ran first. Measure nothing until that is done.
        System.out.println("Warming up (" + tasks + " tasks, discarded)...");
        measure(4, 0);

        List<Run> all = new ArrayList<>();
        List<Run> medians = new ArrayList<>();
        for (int poolSize : POOL_SIZES) {
            List<Run> repetitions = new ArrayList<>();
            for (int rep = 1; rep <= reps; rep++) {
                Run run = measure(poolSize, rep);
                repetitions.add(run);
                all.add(run);
                System.out.printf(Locale.ROOT,
                        "pool=%d rep=%d  makespan=%d ms  throughput=%.2f tasks/s  mean=%.1f ms"
                                + "  p95=%d ms%n",
                        run.poolSize(), run.rep(), run.makespanMs(), run.throughputPerSec(),
                        run.meanLatencyMs(), run.p95LatencyMs());
            }
            repetitions.sort((a, b) -> Double.compare(a.throughputPerSec(), b.throughputPerSec()));
            medians.add(repetitions.get(repetitions.size() / 2));
        }
        writeCsv(all);
        printSpeedupTable(medians);
        return medians;
    }

    private Run measure(int poolSize, int rep) throws Exception {
        String workerName = "bench-worker-" + UUID.randomUUID();
        String schedulerName = "bench-sched-" + UUID.randomUUID();
        TaskStore store = new InMemoryTaskStore();
        TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
        // Retries would hide a saturated worker behind re-dispatches, so the benchmark runs with
        // none: every task gets exactly one attempt.
        RetryCoordinator retries = new RetryCoordinator(
                store, queue, new RetryPolicy(10, 100, 0, 0, 42), new DeadLetterQueue());
        RunningTasks running = new RunningTasks();
        WorkerMetrics metrics = new WorkerMetrics();

        try (ExecutionEngine engine = new ExecutionEngine(
                new ExecutorRegistry(), metrics, "bench-worker", poolSize, tasks * 2)) {
            Server workerServer = InProcessServerBuilder.forName(workerName)
                    .addService(new WorkerServiceImpl(engine, "bench-worker"))
                    .build()
                    .start();
            ManagedChannel workerChannel = InProcessChannelBuilder.forName(workerName).build();
            WorkerServiceGrpc.WorkerServiceBlockingStub workerStub =
                    WorkerServiceGrpc.newBlockingStub(workerChannel);

            WorkerRegistry registry = new WorkerRegistry(60_000);
            registry.register(RegisterRequest.newBuilder()
                    .setWorkerId("bench-worker")
                    .setHost("in-process")
                    .setPort(0)
                    .setCores(Runtime.getRuntime().availableProcessors())
                    .setMemoryMb(metrics.maxMemoryMb())
                    .setPoolSize(poolSize)
                    .build());

            Server schedulerServer = InProcessServerBuilder.forName(schedulerName)
                    .addService(new SchedulerServiceImpl(
                            store, new TaskValidator(4096), queue, retries))
                    .build()
                    .start();
            ManagedChannel schedulerChannel =
                    InProcessChannelBuilder.forName(schedulerName).build();

            ExecutorService clients = Executors.newFixedThreadPool(CLIENT_THREADS);
            try (Dispatcher dispatcher = new Dispatcher(
                    store, queue, registry, worker -> workerStub, retries, running,
                    0L, 2.0, CLIENT_THREADS * 2, 20)) {
                dispatcher.start();

                CountDownLatch start = new CountDownLatch(1);
                CountDownLatch submitted = new CountDownLatch(CLIENT_THREADS);
                int perClient = tasks / CLIENT_THREADS;
                for (int c = 0; c < CLIENT_THREADS; c++) {
                    final int client = c;
                    clients.execute(() -> {
                        SchedulerServiceGrpc.SchedulerServiceBlockingStub stub =
                                SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
                        try {
                            start.await();
                            for (int i = 0; i < perClient; i++) {
                                stub.submitTask(TaskRequest.newBuilder()
                                        .setTaskId("p" + poolSize + "-r" + rep
                                                + "-c" + client + "-t" + i)
                                        .setType(TaskType.CPU_TASK)
                                        .setInput(input)
                                        .setPriority(5)
                                        .build());
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            submitted.countDown();
                        }
                    });
                }

                long startedAtMs = System.currentTimeMillis();
                start.countDown();
                submitted.await(5, TimeUnit.MINUTES);
                awaitCompletion(store, perClient * CLIENT_THREADS);
                long makespanMs = System.currentTimeMillis() - startedAtMs;

                List<Long> latencies = new ArrayList<>();
                for (TaskRecord record : store.list()) {
                    latencies.add(record.completedAt() - record.submittedAt());
                }
                Collections.sort(latencies);
                double mean = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
                long p95 = latencies.isEmpty()
                        ? 0
                        : latencies.get(Math.min(latencies.size() - 1,
                                (int) Math.ceil(0.95 * latencies.size()) - 1));
                double throughput = latencies.size() / (makespanMs / 1000.0);

                return new Run(poolSize, rep, latencies.size(), makespanMs, throughput, mean, p95,
                        metrics.avgExecMs());
            } finally {
                retries.close();
                clients.shutdownNow();
                schedulerChannel.shutdownNow();
                workerChannel.shutdownNow();
                schedulerServer.shutdownNow();
                workerServer.shutdownNow();
            }
        }
    }

    private void awaitCompletion(TaskStore store, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10);
        while (System.currentTimeMillis() < deadline) {
            long terminal = store.list().stream()
                    .filter(r -> r.status() == TaskStatus.COMPLETED
                            || r.status() == TaskStatus.FAILED)
                    .count();
            if (terminal >= expected) {
                return;
            }
            Thread.sleep(25L);
        }
        throw new IllegalStateException("Benchmark timed out waiting for " + expected + " tasks");
    }

    private void writeCsv(List<Run> runs) throws IOException {
        Files.createDirectories(output.getParent());
        List<String> lines = new ArrayList<>();
        lines.add("pool_size,rep,tasks,makespan_ms,throughput_per_sec,mean_latency_ms,"
                + "p95_latency_ms,avg_exec_ms");
        for (Run run : runs) {
            lines.add(String.format(Locale.ROOT, "%d,%d,%d,%d,%.3f,%.1f,%d,%.1f",
                    run.poolSize(), run.rep(), run.tasks(), run.makespanMs(),
                    run.throughputPerSec(), run.meanLatencyMs(), run.p95LatencyMs(),
                    run.avgExecMs()));
        }
        Files.write(output, lines);
        System.out.println("Wrote " + output);
    }

    private void printSpeedupTable(List<Run> runs) {
        if (runs.isEmpty()) {
            return;
        }
        double baseline = runs.get(0).throughputPerSec();
        System.out.println();
        System.out.println("Median of " + reps + " repetitions per pool size, "
                + tasks + " x CPU_TASK " + input + ", " + CLIENT_THREADS + " concurrent clients:");
        System.out.println("| Pool size | Makespan (ms) | Throughput (tasks/s) | Mean latency (ms) "
                + "| P95 latency (ms) | Speedup vs pool 1 |");
        System.out.println("| --- | --- | --- | --- | --- | --- |");
        for (Run run : runs) {
            System.out.printf(Locale.ROOT, "| %d | %d | %.2f | %.1f | %d | %.2fx |%n",
                    run.poolSize(), run.makespanMs(), run.throughputPerSec(),
                    run.meanLatencyMs(), run.p95LatencyMs(),
                    baseline == 0 ? 0 : run.throughputPerSec() / baseline);
        }
        System.out.printf(Locale.ROOT,
                "%nMachine: %d available cores.%n", Runtime.getRuntime().availableProcessors());
    }

    public static void main(String[] args) throws Exception {
        int tasks = 40;
        String input = "n=20000000";
        int reps = 3;
        Path output = Paths.get("results", "exp2-pool-size.csv");
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--tasks" -> tasks = Integer.parseInt(args[++i]);
                case "--input" -> input = args[++i];
                case "--out" -> output = Paths.get(args[++i]);
                case "--reps" -> reps = Integer.parseInt(args[++i]);
                default -> { }
            }
        }
        new PoolSizeBenchmark(tasks, input, output, reps).run();
    }
}
