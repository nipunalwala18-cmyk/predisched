package com.predisched.benchmark;

import com.predisched.common.NodeConfig;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerState;
import com.predisched.scheduler.prediction.PredictionClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;

/**
 * {@code prediction-latency} (prompt 17): the latency the scheduler sees when it asks a running
 * prediction server, through the real {@link PredictionClient} (deadline and breaker included).
 * Tasks and worker states are random but seeded.
 */
public final class PredictionLatency {

    private static final TaskType[] TYPES = {TaskType.CPU_TASK, TaskType.SLEEP_TASK,
        TaskType.MATRIX_TASK, TaskType.SORT_TASK, TaskType.HASH_TASK, TaskType.HTTP_TASK};
    private static final String[] INPUTS = {"n=%d", "ms=%d", "size=%d", "n=%d", "rounds=%d",
        "url=http://localhost:8100/, timeout=%d"};

    private PredictionLatency() {}

    public static void main(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        NodeConfig.PredictionConfig config = new NodeConfig.PredictionConfig();
        config.setHost(opts.getOrDefault("--host", "localhost"));
        config.setPort(Integer.parseInt(opts.getOrDefault("--port", "50070")));
        config.setTimeoutMs(Long.parseLong(opts.getOrDefault("--timeout-ms", "10")));
        config.setLatencyLogEvery(Integer.parseInt(opts.getOrDefault("--log-every", "1000")));
        int requests = Integer.parseInt(opts.getOrDefault("--requests", "2000"));
        int workers = Integer.parseInt(opts.getOrDefault("--workers", "3"));
        int warmup = Integer.parseInt(opts.getOrDefault("--warmup", "200"));
        Random random = new Random(Long.parseLong(opts.getOrDefault("--seed", "17")));

        List<Double> client = new ArrayList<>();
        List<Double> server = new ArrayList<>();
        int empty = 0;
        try (PredictionClient predictions = PredictionClient.create(config)) {
            if (predictions.warmUp(5_000) == null) {
                System.err.println("no prediction server on " + config.getHost() + ":"
                        + config.getPort() + " (python -m predisched_ml.prediction_server)");
                System.exit(1);
            }
            for (int i = 0; i < warmup + requests; i++) {
                int t = random.nextInt(TYPES.length);
                TaskRequest task = TaskRequest.newBuilder().setTaskId("bench-" + i)
                        .setType(TYPES[t]).setPriority(1 + random.nextInt(10))
                        .setInput(String.format(Locale.ROOT, INPUTS[t], 1 + random.nextInt(500)))
                        .build();
                List<WorkerState> states = new ArrayList<>();
                for (int w = 0; w < workers; w++) {
                    int pool = 2 << w % 3;
                    states.add(WorkerState.newBuilder().setWorkerId("worker-" + (w + 1))
                            .setCores(8).setPoolSize(pool).setSlowdown(w == 0 ? 2.0 : 1.0)
                            .setCpuPct(random.nextDouble() * 30).setMemPct(1.0)
                            .setActiveThreads(random.nextInt(pool + 1))
                            .setQueueLen(random.nextInt(3)).setAvgExecMs(random.nextDouble() * 200)
                            .setArrivalRate(random.nextDouble() * 10).build());
                }
                PredictionClient.Result result = predictions.predict(task, states);
                if (i < warmup) {
                    continue;
                }
                if (result.isEmpty()) {
                    empty++;
                } else {
                    client.add(result.clientMs());
                    server.add(result.serverMs());
                }
            }
            System.out.printf(Locale.ROOT, "%d requests x %d workers via PredictionClient"
                            + " (deadline %d ms): %d answered, %d empty (breaker %s)%n",
                    requests, workers, config.getTimeoutMs(), client.size(), empty,
                    predictions.breakerState());
        }
        System.out.printf(Locale.ROOT, "%-8s%9s%9s%9s%9s  (ms)%n", "", "p50", "p95", "p99", "max");
        print("client", client);
        print("server", server);
    }

    private static void print(String name, List<Double> values) {
        if (values.isEmpty()) {
            System.out.println(name + ": no answers");
            return;
        }
        double[] sorted = values.stream().mapToDouble(Double::doubleValue).toArray();
        Arrays.sort(sorted);
        System.out.printf(Locale.ROOT, "%-8s%9.3f%9.3f%9.3f%9.3f%n", name, pct(sorted, 50),
                pct(sorted, 95), pct(sorted, 99), sorted[sorted.length - 1]);
    }

    private static double pct(double[] sorted, double p) {
        int rank = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, rank))];
    }
}
