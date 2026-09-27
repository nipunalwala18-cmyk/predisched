package com.predisched.benchmark;

import com.predisched.client.workload.TraceEntry;
import com.predisched.client.workload.TraceIo;
import com.predisched.common.NodeConfig;
import com.predisched.scheduler.prediction.PredictionClient;
import com.predisched.scheduler.strategy.PredictiveStrategy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * {@code tune-lambda} (prompt 18): replays one training-period trace with the predictive strategy
 * for each overload weight λ, on the in-process cluster of {@link StrategyCompare} with the
 * campaign's heterogeneous workers (pools 2/4/8, worker-1 at 2x slowdown). It needs a running
 * prediction server. The trace must come from the dataset campaign, never from a benchmark set,
 * so λ is not tuned on the data it will be judged on (prompt 20).
 */
public final class TuneLambda {

    private TuneLambda() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        Path tracePath = Paths.get(opts.getOrDefault("--trace",
                "workloads/campaign/mixed-steady-1506.jsonl"));
        if (!tracePath.toString().replace('\\', '/').contains("workloads/campaign/")) {
            System.err.println("tune-lambda takes a training-period trace from workloads/campaign/,"
                    + " not " + tracePath);
            System.exit(2);
        }
        double[] lambdas = Arrays.stream(opts.getOrDefault("--lambdas", "0,0.5,1,2,4").split(","))
                .mapToDouble(Double::parseDouble).toArray();
        int reps = Integer.parseInt(opts.getOrDefault("--reps", "3"));
        double speed = Double.parseDouble(opts.getOrDefault("--speed", "1.0"));
        long seed = Long.parseLong(opts.getOrDefault("--seed", "42"));
        Path out = Paths.get(opts.getOrDefault("--out", "results/lambda-tuning.csv"));
        NodeConfig.PredictionConfig prediction = new NodeConfig.PredictionConfig();
        prediction.setEnabled(true);
        prediction.setHost(opts.getOrDefault("--host", "localhost"));
        prediction.setPort(Integer.parseInt(opts.getOrDefault("--port", "50070")));
        prediction.setTimeoutMs(Long.parseLong(opts.getOrDefault("--timeout-ms", "10")));
        prediction.setLatencyLogEvery(0);
        double[] slowdowns = {2.0, 1.0, 1.0};

        List<TraceEntry> trace = TraceIo.readTrace(tracePath);
        StrategyCompare.requireMockHttpIfNeeded(trace);
        try (PredictionClient probe = PredictionClient.create(prediction)) {
            if (probe.warmUp(5_000) == null) {
                System.err.println("no prediction server on " + prediction.getHost() + ":"
                        + prediction.getPort() + ": python -m predisched_ml.prediction_server");
                System.exit(1);
            }
        }
        System.out.printf(Locale.ROOT, "tune-lambda: %s (%d tasks), speed %.1f, lambda %s,"
                        + " %d reps, pools 2/4/8 with slowdown 2/1/1%n%n", tracePath, trace.size(),
                speed, Arrays.toString(lambdas), reps);
        System.out.println("lambda  rep  mean_lat_ms  p95_lat_ms  makespan_ms  w1/w2/w3"
                + "      fallbacks  live_mae_ms  decision_p95_ms");
        List<String> rows = new ArrayList<>();
        rows.add("lambda,rep,tasks,completed,failed,mean_latency_ms,p95_latency_ms,"
                + "throughput_per_s,makespan_ms,tasks_w1,tasks_w2,tasks_w3,fallbacks,"
                + "live_mae_ms,mean_decision_us,p95_decision_us");
        Map<Double, double[]> meanLatency = new LinkedHashMap<>();
        for (int rep = 1; rep <= reps; rep++) {
            for (double lambda : lambdas) {
                PredictionClient client = PredictionClient.create(prediction);
                client.warmUp(2_000);
                PredictiveStrategy strategy = new PredictiveStrategy(client::predict,
                        new PredictiveStrategy.Settings(lambda, 0.8, 8, 0.5));
                StrategyCompare.Result r;
                try {
                    r = StrategyCompare.run("predictive", () -> strategy, trace, speed, seed,
                            slowdowns);
                } finally {
                    client.close();
                }
                meanLatency.computeIfAbsent(lambda, k -> new double[reps])[rep - 1] =
                        r.meanLatencyMs();
                rows.add(String.format(Locale.ROOT,
                        "%s,%d,%d,%d,%d,%.1f,%d,%.2f,%d,%d,%d,%d,%d,%.1f,%.1f,%d",
                        lambda, rep, trace.size(), r.completed(), r.failed(), r.meanLatencyMs(),
                        r.p95LatencyMs(), r.throughput(), r.makespanMs(),
                        r.perWorker().get("worker-1"), r.perWorker().get("worker-2"),
                        r.perWorker().get("worker-3"), strategy.fallbacks(),
                        strategy.accuracy().mae(), r.meanDecisionMicros(), r.p95DecisionMicros()));
                System.out.printf(Locale.ROOT,
                        "%6s  %3d  %11.1f  %10d  %11d  %3d/%3d/%3d  %9d  %11.1f  %15.2f%n",
                        lambda, rep, r.meanLatencyMs(), r.p95LatencyMs(), r.makespanMs(),
                        r.perWorker().get("worker-1"), r.perWorker().get("worker-2"),
                        r.perWorker().get("worker-3"), strategy.fallbacks(),
                        strategy.accuracy().mae(), r.p95DecisionMicros() / 1000.0);
            }
        }
        double best = Double.NaN;
        double bestMean = Double.MAX_VALUE;
        System.out.println();
        System.out.println("lambda  mean latency over reps (ms)  stddev");
        for (Map.Entry<Double, double[]> e : meanLatency.entrySet()) {
            double mean = Arrays.stream(e.getValue()).average().orElse(Double.NaN);
            double sd = Math.sqrt(Arrays.stream(e.getValue())
                    .map(v -> (v - mean) * (v - mean)).sum() / Math.max(1, reps - 1));
            System.out.printf(Locale.ROOT, "%6s  %27.1f  %6.1f%n", e.getKey(), mean, sd);
            if (mean < bestMean) {
                bestMean = mean;
                best = e.getKey();
            }
        }
        System.out.printf(Locale.ROOT, "lowest mean latency: lambda = %s (%.1f ms)%n", best,
                bestMean);
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.write(out, rows, StandardCharsets.UTF_8);
        System.out.println("rows written to " + out);
    }
}
