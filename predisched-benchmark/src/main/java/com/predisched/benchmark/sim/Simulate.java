package com.predisched.benchmark.sim;

import com.predisched.benchmark.suite.RunMetrics;
import com.predisched.benchmark.suite.SuiteConfig;
import com.predisched.client.workload.TraceEntry;
import com.predisched.client.workload.TraceIo;
import com.predisched.common.NodeConfig;
import com.predisched.scheduler.prediction.PredictionClient;
import com.predisched.scheduler.strategy.PredictiveStrategy;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyRegistry;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code simulate} (prompt 20, F17): replays a trace against any strategy in the discrete-event
 * {@link Simulator}, no cluster. The predictive strategy asks a running prediction server for its
 * predictions, as in the cluster, with a generous deadline (simulated time is not wall time).
 */
public final class Simulate {

    private Simulate() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        Path tracePath = Paths.get(opts.getOrDefault("--trace",
                "workloads/benchmark/steady-mixed-steady-2001.jsonl"));
        String strategyName = opts.getOrDefault("--strategy", "least_loaded");
        double speed = Double.parseDouble(opts.getOrDefault("--speed", "1.0"));
        long seed = Long.parseLong(opts.getOrDefault("--seed", "42"));
        double overheadMs = Double.parseDouble(opts.getOrDefault("--overhead-ms", "30"));
        String workerSet = opts.getOrDefault("--workers", "heterogeneous");
        long deadlineMs = Long.parseLong(opts.getOrDefault("--deadline-ms", "2000"));
        SuiteConfig suite = SuiteConfig.load(Paths.get(opts.getOrDefault("--config",
                "configs/benchmark.yaml")));
        List<Simulator.WorkerModel> workers = suite.workerSets().get(workerSet).stream()
                .map(w -> new Simulator.WorkerModel(w.id(), w.poolSize(), w.slowdown()))
                .toList();
        ExecSamples samples = ExecSamples.load(Paths.get(opts.getOrDefault("--samples",
                "workloads/benchmark/exec-samples.csv")), 20);
        List<TraceEntry> trace = TraceIo.readTrace(tracePath);

        PredictionClient client = null;
        SchedulingStrategy strategy;
        if (strategyName.equals("predictive")) {
            NodeConfig.PredictionConfig prediction = new NodeConfig.PredictionConfig();
            prediction.setEnabled(true);
            prediction.setPort(Integer.parseInt(opts.getOrDefault("--prediction-port", "50070")));
            prediction.setTimeoutMs(1_000);
            prediction.setLatencyLogEvery(0);
            client = PredictionClient.create(prediction);
            if (client.warmUp(5_000) == null) {
                System.err.println("the predictive strategy needs a prediction server on port "
                        + prediction.getPort());
                System.exit(1);
            }
            strategy = new PredictiveStrategy(client::predict,
                    PredictiveStrategy.Settings.defaults());
        } else {
            strategy = StrategyRegistry.standard().create(strategyName,
                    new StrategyRegistry.Settings(seed, 0.4, 0.2, 0.4));
        }
        long wallStart = System.nanoTime();
        Simulator.Result result;
        try {
            result = new Simulator(strategy, workers, samples, 2.0, overheadMs, seed)
                    .run(trace, speed);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        double wallMs = (System.nanoTime() - wallStart) / 1e6;
        Map<String, Double> m = RunMetrics.fromReplay(result.rows(),
                workers.stream().map(Simulator.WorkerModel::id).toList(),
                (int) Math.ceil(0.1 * result.rows().size()), deadlineMs);
        System.out.println("SIMULATION (discrete-event model, not a measurement of the cluster)");
        System.out.printf(Locale.ROOT, "trace %s (%d tasks), strategy %s, speed %.1f, workers %s,"
                        + " seed %d, overhead %.0f ms/task%n", tracePath, trace.size(),
                strategyName, speed, workerSet, seed, overheadMs);
        System.out.printf(Locale.ROOT, "mean latency %.1f ms, p95 %.1f ms, p99 %.1f ms,"
                        + " throughput %.2f tasks/s, makespan %.0f ms, imbalance %.2f,"
                        + " max queue %.0f, SLA violations %.1f %%%n",
                m.get("mean_latency_ms"), m.get("p95_latency_ms"), m.get("p99_latency_ms"),
                m.get("throughput_per_s"), m.get("makespan_ms"), m.get("imbalance"),
                m.get("max_queue"), m.get("sla_violation_pct"));
        System.out.printf(Locale.ROOT, "%d events simulated in %.0f ms of wall time%n",
                result.events(), wallMs);
        String out = opts.get("--out");
        if (out != null) {
            StringBuilder csv = new StringBuilder("task_id,task_type,priority,offset_ms,"
                    + "submit_ms,start_ms,end_ms,latency_ms,status,worker,exec_ms\n");
            for (RunMetrics.TaskRow r : result.rows()) {
                csv.append(String.join(",", r.taskId(), r.taskType(), "0",
                        String.valueOf(r.offsetMs()), String.valueOf(r.submitMs()),
                        String.valueOf(r.startMs()), String.valueOf(r.endMs()),
                        String.valueOf(r.latencyMs()), r.status(), r.worker(),
                        String.valueOf(r.execMs()))).append('\n');
            }
            Files.writeString(Paths.get(out), csv.toString(), StandardCharsets.UTF_8);
            System.out.println("per-task rows written to " + out);
        }
    }
}
