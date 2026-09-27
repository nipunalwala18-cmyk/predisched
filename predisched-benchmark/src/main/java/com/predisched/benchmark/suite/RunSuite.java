package com.predisched.benchmark.suite;

import com.predisched.client.SchedulerClient;
import com.predisched.client.workload.Replayer;
import com.predisched.client.workload.TraceEntry;
import com.predisched.common.NodeConfig;
import com.predisched.common.db.Db;
import com.predisched.common.net.Transport;
import com.predisched.proto.ChaosServiceGrpc;
import com.predisched.proto.CrashRequest;
import com.predisched.proto.WorkerEntry;
import io.grpc.ManagedChannel;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * {@code run-suite} (prompt 20, spec §13): every scenario × strategy × repetition of
 * {@code configs/benchmark.yaml}, each on a fresh cluster of real processes (one scheduler and
 * three workers, started and stopped here), replaying the scenario's saved trace. Rows go to
 * {@code results/benchmark/<suite-id>/runs.csv} and {@code benchmark_runs}; the replay CSV of
 * every run is kept for the report's CDFs. Resumable: runs already in runs.csv are skipped.
 *
 * <pre>
 * run-suite --config configs/benchmark.yaml [--reps 5] [--suite-id ID] [--plan]
 *           [--only scenario[,scenario]] [--summarize] [--python .venv/Scripts/python.exe]
 * </pre>
 */
public final class RunSuite {

    private static final int SCHEDULER_PORT = 51051;
    private static final int PREDICTION_PORT = 50070;
    private static final int MOCK_HTTP_PORT = 8100;

    private RunSuite() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        Set<String> flags = new HashSet<>();
        for (int i = 0; i < args.length; i++) {
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                opts.put(args[i], args[++i]);
            } else {
                flags.add(args[i]);
            }
        }
        Path root = BenchmarkTraces.root();
        SuiteConfig config = SuiteConfig.load(root.resolve(
                opts.getOrDefault("--config", "configs/benchmark.yaml")));
        int reps = Integer.parseInt(opts.getOrDefault("--reps", String.valueOf(config.reps())));
        String suiteId = opts.getOrDefault("--suite-id", config.suite() + "-"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmm")));
        Path out = root.resolve("results").resolve("benchmark").resolve(suiteId);
        Path runsCsv = out.resolve("runs.csv");

        List<SuitePlan.Run> plan = SuitePlan.plan(config, reps, suiteId);
        if (opts.containsKey("--only")) {
            Set<String> only = Set.of(opts.get("--only").split(","));
            plan = plan.stream().filter(r -> only.contains(r.scenario())).toList();
        }
        if (flags.contains("--plan")) {
            plan.forEach(r -> System.out.printf(Locale.ROOT, "%3d %-40s %s%n", r.index(),
                    r.runId(), r.taskSuffix()));
            return;
        }
        if (flags.contains("--summarize")) {
            summarize(out);
            return;
        }

        Map<String, String> manifest = BenchmarkTraces.ensure(config, root);
        Files.createDirectories(out.resolve("runs"));
        Files.createDirectories(out.resolve("logs"));
        Set<String> done = new HashSet<>();
        if (Files.exists(runsCsv)) {
            SuiteSummary.readRuns(runsCsv).forEach(r -> done.add(r.get("run_id")));
        } else {
            Files.writeString(runsCsv, "run_id,index,scenario,strategy,rep,started_at,"
                    + String.join(",", RunMetrics.NAMES) + "\n", StandardCharsets.UTF_8);
        }
        System.out.printf(Locale.ROOT, "suite %s: %d runs (%d scenarios x %d strategies x %d"
                        + " reps), %d already done; traces %s%n", suiteId, plan.size(),
                config.scenarios().size(), config.strategies().size(), reps, done.size(),
                manifest.keySet());

        NodeConfig node = NodeConfig.load(root.resolve(config.nodeConfig()));
        String python = opts.getOrDefault("--python", ".venv/Scripts/python.exe");
        List<Process> services = new ArrayList<>();
        try (Db db = Db.open(node.getDb(), 2, "benchmark", false)) {
            services.addAll(startServices(root, python, out.resolve("logs"),
                    config.strategies().contains(SuiteSummary.PREDICTIVE)));
            int position = 0;
            for (SuitePlan.Run run : plan) {
                position++;
                if (done.contains(run.runId())) {
                    continue;
                }
                long started = System.currentTimeMillis();
                Map<String, Double> m = runOne(config, root, node, run, out, db);
                String row = run.runId() + "," + run.index() + "," + run.scenario() + ","
                        + run.strategy() + "," + run.rep() + "," + started + ","
                        + RunMetrics.NAMES.stream().map(k -> SuiteSummary.fmt(
                                m.getOrDefault(k, Double.NaN))).collect(Collectors.joining(","));
                Files.writeString(runsCsv, row + "\n", StandardCharsets.UTF_8,
                        StandardOpenOption.APPEND);
                record(db, suiteId, run, m);
                System.out.printf(Locale.ROOT, "[%3d/%d] %-40s mean %7.1f ms  p95 %7.1f ms"
                                + "  %5.1f tasks/s  SLA viol %5.1f %%  failed %.0f  (%.0f s)%n",
                        position, plan.size(), run.runId(), m.get("mean_latency_ms"),
                        m.get("p95_latency_ms"), m.get("throughput_per_s"),
                        m.get("sla_violation_pct"), m.get("failed"),
                        (System.currentTimeMillis() - started) / 1000.0);
            }
        } finally {
            services.forEach(p -> p.descendants().forEach(ProcessHandle::destroyForcibly));
            services.forEach(Process::destroyForcibly);
        }
        summarize(out);
    }

    static void summarize(Path out) throws IOException {
        List<Map<String, String>> summary =
                SuiteSummary.summarize(SuiteSummary.readRuns(out.resolve("runs.csv")));
        SuiteSummary.write(out.resolve("summary.csv"), summary);
        System.out.println();
        System.out.print(SuiteSummary.table(summary));
        System.out.println("summary written to " + out.resolve("summary.csv"));
    }

    // --- one run -----------------------------------------------------------------------------
    static Map<String, Double> runOne(SuiteConfig config, Path root, NodeConfig node,
            SuitePlan.Run run, Path out, Db db) throws Exception {
        SuiteConfig.Scenario scenario = config.scenario(run.scenario());
        List<SuiteConfig.WorkerSpec> workers = config.workerSets().get(scenario.workers());
        List<TraceEntry> trace = BenchmarkTraces.read(config, root, scenario).stream()
                .map(e -> new TraceEntry(e.offsetMs(), e.taskId() + run.taskSuffix(), e.type(),
                        e.input(), e.priority(), e.timeoutMs()))
                .toList();
        Path logs = out.resolve("logs").resolve(run.runId());
        Files.createDirectories(logs);
        Path csv = out.resolve("runs").resolve(run.runId() + ".csv");
        Cluster cluster = Cluster.start(root, config.nodeConfig(), run.strategy(), workers, logs);
        long windowStart = System.currentTimeMillis();
        Thread killer = null;
        try (SchedulerClient client = new SchedulerClient("localhost", SCHEDULER_PORT)) {
            cluster.awaitWorkers(client, workers.size(), 30_000);
            if (scenario.killWorker() != null && !scenario.killWorker().isBlank()) {
                SuiteConfig.WorkerSpec victim = workers.stream()
                        .filter(w -> w.id().equals(scenario.killWorker())).findFirst()
                        .orElseThrow();
                killer = new Thread(() -> {
                    try {
                        Thread.sleep(scenario.killAtMs());
                        crash(victim.port());
                    } catch (InterruptedException ignored) {
                        // run ended first
                    }
                }, "benchmark-killer");
                killer.setDaemon(true);
                killer.start();
            }
            PrintStream quiet = new PrintStream(OutputStream.nullOutputStream());
            Replayer.replay(trace, config.speed(), client, csv, 50, config.runTimeoutMs(),
                    config.deadlineMs(), quiet);
            Thread.sleep(2_000); // the scheduler's history writer flushes every 200 ms
        } finally {
            if (killer != null) {
                killer.interrupt();
            }
        }
        long windowEnd = System.currentTimeMillis();
        List<RunMetrics.TaskRow> rows = RunMetrics.read(csv);
        int warmup = (int) Math.ceil(config.warmupFraction() * rows.size());
        Map<String, Double> m = new LinkedHashMap<>(RunMetrics.fromReplay(rows,
                workers.stream().map(SuiteConfig.WorkerSpec::id).toList(), warmup,
                config.deadlineMs()));
        m.putAll(fromDatabase(db, run.taskSuffix(), windowStart, windowEnd));
        cluster.stop();
        return m;
    }

    private static void crash(int port) {
        ManagedChannel channel = Transport.get().channel("localhost", port);
        try {
            ChaosServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .crash(CrashRequest.newBuilder().setReason("benchmark failure scenario")
                            .build());
        } catch (RuntimeException e) {
            System.out.println("  could not crash the worker on " + port + ": " + e.getMessage());
        } finally {
            channel.shutdownNow();
        }
    }

    /** CPU per worker, scheduling overhead and prediction error, from the run's rows. */
    static Map<String, Double> fromDatabase(Db db, String suffix, long fromMs, long toMs)
            throws Exception {
        Map<String, Double> m = new LinkedHashMap<>();
        try (Connection c = db.dataSource().getConnection()) {
            List<Double> cpu = new ArrayList<>();
            try (PreparedStatement s = c.prepareStatement("SELECT worker_id, avg(cpu_pct)"
                    + " FROM worker_metrics WHERE ts BETWEEN ? AND ? GROUP BY worker_id")) {
                s.setTimestamp(1, new Timestamp(fromMs));
                s.setTimestamp(2, new Timestamp(toMs));
                try (ResultSet r = s.executeQuery()) {
                    while (r.next()) {
                        cpu.add(r.getDouble(2));
                    }
                }
            }
            double mean = cpu.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
            m.put("cpu_mean_pct", mean);
            m.put("cpu_var", cpu.stream().mapToDouble(v -> (v - mean) * (v - mean)).average()
                    .orElse(Double.NaN));
            try (PreparedStatement s = c.prepareStatement("SELECT avg(decision_us),"
                    + " percentile_cont(0.95) WITHIN GROUP (ORDER BY decision_us)"
                    + " FROM scheduling_decisions WHERE task_id LIKE ?")) {
                s.setString(1, "%" + suffix);
                try (ResultSet r = s.executeQuery()) {
                    r.next();
                    m.put("overhead_mean_us", nullable(r, 1));
                    m.put("overhead_p95_us", nullable(r, 2));
                }
            }
            try (PreparedStatement s = c.prepareStatement("SELECT avg(abs_error_ms)"
                    + " FROM prediction_outcomes WHERE task_id LIKE ?")) {
                s.setString(1, "%" + suffix);
                try (ResultSet r = s.executeQuery()) {
                    r.next();
                    m.put("mae_ms", nullable(r, 1));
                }
            }
        }
        return m;
    }

    private static double nullable(ResultSet r, int column) throws Exception {
        double v = r.getDouble(column);
        return r.wasNull() ? Double.NaN : v;
    }

    private static void record(Db db, String suiteId, SuitePlan.Run run, Map<String, Double> m)
            throws Exception {
        String json = m.entrySet().stream()
                .map(e -> "\"" + e.getKey() + "\":" + (Double.isFinite(e.getValue())
                        ? String.format(Locale.ROOT, "%.6g", e.getValue()) : "null"))
                .collect(Collectors.joining(",", "{\"suite\":\"" + suiteId + "\",\"rep\":"
                        + run.rep() + ",\"task_suffix\":\"" + run.taskSuffix() + "\",", "}"));
        try (Connection c = db.dataSource().getConnection();
                PreparedStatement s = c.prepareStatement("INSERT INTO benchmark_runs"
                        + " (run_id, strategy, scenario, metrics) VALUES (?, ?, ?, ?::jsonb)"
                        + " ON CONFLICT (run_id) DO UPDATE SET metrics = EXCLUDED.metrics")) {
            s.setString(1, suiteId + "/" + run.runId());
            s.setString(2, run.strategy());
            s.setString(3, "benchmark:" + suiteId + ":" + run.scenario());
            s.setString(4, json);
            s.executeUpdate();
        }
    }

    // --- processes ---------------------------------------------------------------------------
    private static List<Process> startServices(Path root, String python, Path logs,
            boolean prediction) throws Exception {
        List<Process> started = new ArrayList<>();
        if (!portOpen(MOCK_HTTP_PORT)) {
            started.add(new ProcessBuilder(root.resolve(python).toString(), "scripts/mock-http.py",
                    "--port", String.valueOf(MOCK_HTTP_PORT))
                    .directory(root.toFile()).redirectErrorStream(true)
                    .redirectOutput(logs.resolve("mock-http.log").toFile()).start());
            waitFor(() -> portOpen(MOCK_HTTP_PORT), 15_000, "mock HTTP server");
        }
        if (prediction && !portOpen(PREDICTION_PORT)) {
            started.add(new ProcessBuilder(root.resolve(python).toString(), "-m",
                    "predisched_ml.prediction_server", "--port", String.valueOf(PREDICTION_PORT),
                    "--no-db", "--log", logs.resolve("predictions.jsonl").toString())
                    .directory(root.resolve("ml").toFile()).redirectErrorStream(true)
                    .redirectOutput(logs.resolve("prediction-server.log").toFile()).start());
            waitFor(() -> portOpen(PREDICTION_PORT), 60_000, "prediction server");
        }
        return started;
    }

    /** One scheduler and its workers, as child processes of this JVM. */
    static final class Cluster {
        private final List<Process> processes = new ArrayList<>();

        static Cluster start(Path root, String nodeConfig, String strategy,
                List<SuiteConfig.WorkerSpec> workers, Path logs) throws Exception {
            waitFor(() -> !portOpen(SCHEDULER_PORT) && workers.stream()
                    .noneMatch(w -> portOpen(w.port())), 30_000, "ports from the last run");
            String java = ProcessHandle.current().info().command().orElse("java");
            Cluster cluster = new Cluster();
            cluster.processes.add(new ProcessBuilder(java, "-jar",
                    "predisched-scheduler/target/predisched-scheduler.jar", "--config", nodeConfig,
                    "--port", String.valueOf(SCHEDULER_PORT), "--strategy", strategy)
                    .directory(root.toFile()).redirectErrorStream(true)
                    .redirectOutput(logs.resolve("scheduler.log").toFile()).start());
            waitFor(() -> portOpen(SCHEDULER_PORT), 60_000, "scheduler");
            List<File> ready = new ArrayList<>();
            for (SuiteConfig.WorkerSpec w : workers) {
                File readyFile = logs.resolve(w.id() + ".ready").toFile();
                readyFile.delete();
                ready.add(readyFile);
                cluster.processes.add(new ProcessBuilder(java, "-jar",
                        "predisched-worker/target/predisched-worker.jar", "--config", nodeConfig,
                        "--id", w.id(), "--port", String.valueOf(w.port()),
                        "--pool-size", String.valueOf(w.poolSize()),
                        "--slowdown", String.valueOf(w.slowdown()),
                        "--ready-file", readyFile.getPath())
                        .directory(root.toFile()).redirectErrorStream(true)
                        .redirectOutput(logs.resolve(w.id() + ".log").toFile()).start());
            }
            waitFor(() -> ready.stream().allMatch(File::exists), 60_000, "workers");
            return cluster;
        }

        void awaitWorkers(SchedulerClient client, int n, long timeoutMs) throws Exception {
            waitFor(() -> {
                try {
                    List<WorkerEntry> w = client.listWorkers().getWorkersList();
                    return w.stream().filter(WorkerEntry::getHealthy).count() >= n;
                } catch (RuntimeException e) {
                    return false;
                }
            }, timeoutMs, n + " healthy workers");
        }

        void stop() throws InterruptedException {
            for (Process p : processes) {
                p.destroy();
            }
            for (Process p : processes) {
                if (!p.waitFor(10, TimeUnit.SECONDS)) {
                    p.destroyForcibly().waitFor(5, TimeUnit.SECONDS);
                }
            }
        }
    }

    interface Check {
        boolean ok() throws Exception;
    }

    static void waitFor(Check check, long timeoutMs, String what) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (check.ok()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new IllegalStateException(what + " not ready within " + timeoutMs + " ms");
    }

    static boolean portOpen(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 200);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    static Path path(String first) {
        return Paths.get(first);
    }
}
