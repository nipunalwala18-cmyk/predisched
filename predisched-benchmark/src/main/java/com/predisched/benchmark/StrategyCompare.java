package com.predisched.benchmark;

import com.predisched.client.workload.Replayer;
import com.predisched.client.workload.SchedulerGateway;
import com.predisched.client.workload.TraceEntry;
import com.predisched.client.workload.TraceIo;
import com.predisched.common.InMemoryTaskStore;
import com.predisched.common.TaskStore;
import com.predisched.common.TaskValidator;
import com.predisched.proto.RegistryServiceGrpc;
import com.predisched.proto.SchedulerServiceGrpc;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskResponse;
import com.predisched.proto.TaskStatusRequest;
import com.predisched.proto.TaskStatusResponse;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerServiceGrpc;
import com.predisched.scheduler.Dispatcher;
import com.predisched.scheduler.RegistryServiceImpl;
import com.predisched.scheduler.SchedulerServiceImpl;
import com.predisched.scheduler.WorkerRegistry;
import com.predisched.scheduler.queue.AgeingPriorityQueue;
import com.predisched.scheduler.queue.DeadLetterQueue;
import com.predisched.scheduler.queue.RetryCoordinator;
import com.predisched.scheduler.queue.RetryPolicy;
import com.predisched.scheduler.queue.RunningTasks;
import com.predisched.scheduler.queue.TaskQueue;
import com.predisched.scheduler.strategy.SchedulingDecision;
import com.predisched.scheduler.strategy.StrategyRegistry;
import com.predisched.worker.ExecutionEngine;
import com.predisched.worker.ExecutorRegistry;
import com.predisched.worker.RegistrationClient;
import com.predisched.worker.WorkerMetrics;
import com.predisched.worker.WorkerServiceImpl;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exp 6 measurement: one saved trace replayed against an in-process scheduler and three real
 * workers with pool sizes 2, 4 and 8, once per strategy, same trace and same seed. Workers
 * register and heartbeat through the real {@code RegistryService}, so strategies see the same
 * metrics they would in a live cluster.
 *
 * <p>All three workers share one JVM, so their CPU and memory readings are the same process-wide
 * numbers; what differs between them is pool size and load. The resource-aware score is therefore
 * driven by its load term here.
 */
public final class StrategyCompare {

    private static final int[] POOL_SIZES = {2, 4, 8};

    private StrategyCompare() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        Path tracePath = Paths.get(opts.getOrDefault("--trace", "workloads/mixed-steady-7.jsonl"));
        double speed = Double.parseDouble(opts.getOrDefault("--speed", "1.0"));
        long seed = Long.parseLong(opts.getOrDefault("--seed", "42"));
        Path out = Paths.get(opts.getOrDefault("--out", "results/exp6-strategies.csv"));
        List<TraceEntry> trace = TraceIo.readTrace(tracePath);
        requireMockHttpIfNeeded(trace);
        System.out.printf(Locale.ROOT, "strategy-compare: %s (%d tasks), speed %.1f, workers with"
                + " pools 2/4/8, seed %d%n", tracePath, trace.size(), speed, seed);

        List<String> rows = new ArrayList<>();
        rows.add("strategy,tasks,completed,failed,tasks_w1_pool2,tasks_w2_pool4,tasks_w3_pool8,"
                + "imbalance_stddev,mean_latency_ms,p95_latency_ms,throughput_per_s,makespan_ms,"
                + "mean_decision_us");
        System.out.println();
        System.out.println("strategy        w1/p2  w2/p4  w3/p8  imbalance  mean_lat_ms"
                + "  p95_lat_ms  tasks/s  makespan_ms  decision_us  failed");
        for (String strategy : List.of("round_robin", "random", "least_loaded", "resource_aware")) {
            Result result = run(strategy, trace, speed, seed);
            rows.add(String.format(Locale.ROOT, "%s,%d,%d,%d,%d,%d,%d,%.2f,%.1f,%d,%.2f,%d,%.1f",
                    strategy, trace.size(), result.completed, result.failed,
                    result.perWorker.get("worker-1"), result.perWorker.get("worker-2"),
                    result.perWorker.get("worker-3"), result.imbalance, result.meanLatencyMs,
                    result.p95LatencyMs, result.throughput, result.makespanMs,
                    result.meanDecisionMicros));
            System.out.printf(Locale.ROOT,
                    "%-15s %5d  %5d  %5d  %9.2f  %11.1f  %10d  %7.2f  %11d  %11.1f  %6d%n",
                    strategy, result.perWorker.get("worker-1"), result.perWorker.get("worker-2"),
                    result.perWorker.get("worker-3"), result.imbalance, result.meanLatencyMs,
                    result.p95LatencyMs, result.throughput, result.makespanMs,
                    result.meanDecisionMicros, result.failed);
        }
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.write(out, rows, StandardCharsets.UTF_8);
        System.out.println("rows written to " + out);
    }

    private record Result(Map<String, Integer> perWorker, int completed, int failed,
            double imbalance, double meanLatencyMs, long p95LatencyMs, double throughput,
            long makespanMs, double meanDecisionMicros) {}

    private static Result run(String strategyName, List<TraceEntry> trace, double speed, long seed)
            throws Exception {
        List<AutoCloseable> closers = new ArrayList<>();
        try {
            TaskStore store = new InMemoryTaskStore();
            TaskQueue queue = new AgeingPriorityQueue(0.1, 10);
            RetryCoordinator retries = new RetryCoordinator(
                    store, queue, new RetryPolicy(50, 1_000, 0, 3, seed), new DeadLetterQueue());
            closers.add(retries::close);
            WorkerRegistry registry = new WorkerRegistry(5_000);
            Map<String, WorkerServiceGrpc.WorkerServiceBlockingStub> workerStubs =
                    new ConcurrentHashMap<>();
            Dispatcher dispatcher = new Dispatcher(store, queue, registry,
                    worker -> workerStubs.get(worker.id()), retries, new RunningTasks(),
                    0L, 2.0, 16, 20,
                    StrategyRegistry.standard().create(strategyName, new StrategyRegistry.Settings(
                            seed, 0.4, 0.2, 0.4)));
            closers.add(dispatcher);

            String schedulerName = "strategy-sched-" + UUID.randomUUID();
            Server scheduler = InProcessServerBuilder.forName(schedulerName)
                    .addService(new SchedulerServiceImpl(
                            store, new TaskValidator(4096), queue, retries))
                    .addService(new RegistryServiceImpl(registry))
                    .build()
                    .start();
            closers.add(scheduler::shutdownNow);
            ManagedChannel schedulerChannel = InProcessChannelBuilder.forName(schedulerName).build();
            closers.add(schedulerChannel::shutdownNow);

            for (int i = 0; i < POOL_SIZES.length; i++) {
                String id = "worker-" + (i + 1);
                WorkerMetrics metrics = new WorkerMetrics();
                ExecutionEngine engine = new ExecutionEngine(
                        new ExecutorRegistry(), metrics, id, POOL_SIZES[i], 200);
                closers.add(engine);
                String workerName = "strategy-" + id + "-" + UUID.randomUUID();
                Server worker = InProcessServerBuilder.forName(workerName)
                        .addService(new WorkerServiceImpl(engine, id))
                        .build()
                        .start();
                closers.add(worker::shutdownNow);
                ManagedChannel workerChannel = InProcessChannelBuilder.forName(workerName).build();
                closers.add(workerChannel::shutdownNow);
                workerStubs.put(id, WorkerServiceGrpc.newBlockingStub(workerChannel));
                RegistrationClient registration = new RegistrationClient(
                        RegistryServiceGrpc.newBlockingStub(schedulerChannel),
                        engine, metrics, id, "in-process", 0, 250);
                registration.start();
                closers.add(registration);
            }
            long deadline = System.currentTimeMillis() + 10_000;
            while (registry.healthy().size() < POOL_SIZES.length) {
                if (System.currentTimeMillis() > deadline) {
                    throw new IllegalStateException("workers did not register");
                }
                Thread.sleep(20);
            }
            dispatcher.start();

            SchedulerServiceGrpc.SchedulerServiceBlockingStub client =
                    SchedulerServiceGrpc.newBlockingStub(schedulerChannel);
            SchedulerGateway gateway = new SchedulerGateway() {
                @Override
                public TaskResponse submit(String taskId, TaskType type, String input,
                        int priority, String traceId, long timeoutMs, int maxRetries) {
                    return client.submitTask(TaskRequest.newBuilder()
                            .setTaskId(taskId).setType(type).setInput(input)
                            .setPriority(priority).setTraceId(traceId)
                            .setTimeoutMs(timeoutMs).setMaxRetries(maxRetries).build());
                }

                @Override
                public TaskStatusResponse status(String taskId) {
                    return client.getTaskStatus(
                            TaskStatusRequest.newBuilder().setTaskId(taskId).build());
                }
            };
            Path csv = Files.createTempFile("strategy-" + strategyName, ".csv");
            PrintStream quiet = new PrintStream(OutputStream.nullOutputStream());
            Replayer.replay(trace, speed, gateway, csv, 20, 30 * 60_000L, quiet);
            Result result = summarise(csv, dispatcher.decisions().recent());
            Files.deleteIfExists(csv);
            return result;
        } finally {
            for (int i = closers.size() - 1; i >= 0; i--) {
                try {
                    closers.get(i).close();
                } catch (Exception ignored) {
                    // Best effort: the next strategy gets a fresh cluster either way.
                }
            }
        }
    }

    /** Per-task rows from the replay CSV: worker, status and submit-to-end latency. */
    private static Result summarise(Path csv, List<SchedulingDecision> decisions)
            throws Exception {
        Map<String, Integer> perWorker = new LinkedHashMap<>();
        for (int i = 1; i <= POOL_SIZES.length; i++) {
            perWorker.put("worker-" + i, 0);
        }
        List<Long> latencies = new ArrayList<>();
        long firstSubmit = Long.MAX_VALUE;
        long lastEnd = 0;
        int completed = 0;
        int failed = 0;
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        for (String line : lines.subList(1, lines.size())) {
            // task_id,task_type,priority,offset_ms,submit_ms,start_ms,end_ms,latency_ms,status,worker,exec_ms
            String[] f = line.split(",");
            long submit = Long.parseLong(f[4]);
            long end = Long.parseLong(f[6]);
            firstSubmit = Math.min(firstSubmit, submit);
            lastEnd = Math.max(lastEnd, end);
            if (!"COMPLETED".equals(f[8])) {
                failed++;
                continue;
            }
            completed++;
            latencies.add(Long.parseLong(f[7]));
            perWorker.merge(f[9], 1, Integer::sum);
        }
        latencies.sort(Long::compare);
        double mean = latencies.stream().mapToLong(Long::longValue).average().orElse(0);
        long p95 = latencies.isEmpty() ? 0
                : latencies.get((int) Math.ceil(0.95 * latencies.size()) - 1);
        long makespan = lastEnd - firstSubmit;
        double avg = perWorker.values().stream().mapToInt(Integer::intValue).average().orElse(0);
        double variance = perWorker.values().stream()
                .mapToDouble(count -> (count - avg) * (count - avg)).average().orElse(0);
        double decisionMicros = decisions.stream()
                .mapToLong(SchedulingDecision::decisionMicros).average().orElse(0);
        return new Result(perWorker, completed, failed, Math.sqrt(variance), mean, p95,
                completed / (makespan / 1000.0), makespan, decisionMicros);
    }

    /** HTTP_TASK calls the mock service; without it every such task would fail and skew rows. */
    private static void requireMockHttpIfNeeded(List<TraceEntry> trace) {
        TraceEntry http = trace.stream()
                .filter(entry -> entry.type() == TaskType.HTTP_TASK)
                .findFirst()
                .orElse(null);
        if (http == null) {
            return;
        }
        String url = http.input().replaceAll("^.*url=([^,]+).*$", "$1");
        String base = url.replaceAll("^(https?://[^/]+).*$", "$1");
        try {
            HttpURLConnection connection = (HttpURLConnection)
                    URI.create(base + "/delay?ms=1").toURL().openConnection();
            connection.setConnectTimeout(1_000);
            connection.setReadTimeout(2_000);
            if (connection.getResponseCode() == 200) {
                return;
            }
        } catch (Exception e) {
            // fall through to the message
        }
        throw new IllegalStateException("the trace has HTTP_TASKs but " + base + " does not"
                + " answer; start it first: python scripts/mock-http.py --port "
                + base.replaceAll("^.*:(\\d+)$", "$1"));
    }
}
