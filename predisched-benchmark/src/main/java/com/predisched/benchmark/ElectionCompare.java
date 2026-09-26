package com.predisched.benchmark;

import com.predisched.common.NodeConfig;
import com.predisched.election.ElectionAlgorithm;
import com.predisched.election.InProcessElectionCluster;
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
import java.util.Optional;

/**
 * Exp 4 measurement: Bully against Ring on five election nodes over in-process gRPC, with the
 * timeouts from {@code configs/cluster.yaml}. Each run elects leader 5, kills it, and records the
 * new leader, the election messages every node sent, the failover time from the kill (or the
 * detection) until all survivors agree, and the longest detection-to-leader time a node saw.
 *
 * <p>Two scenarios: {@code all_detect} is the live cluster, where every follower pings the leader
 * and several notice the failure together; {@code lowest_detects} has only node 1 notice it,
 * which is the Bully worst case the textbook analysis uses.
 */
public final class ElectionCompare {

    private static final List<Integer> IDS = List.of(1, 2, 3, 4, 5);

    private ElectionCompare() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = options(args);
        int runs = Integer.parseInt(opts.getOrDefault("--runs", "5"));
        Path out = Paths.get(opts.getOrDefault("--out", "results/exp4-election.csv"));
        NodeConfig.ElectionConfig config = new NodeConfig.ElectionConfig();
        Path clusterConfig = Paths.get(opts.getOrDefault("--config", "configs/cluster.yaml"));
        if (Files.exists(clusterConfig)) {
            config = NodeConfig.load(clusterConfig).getElection();
        }
        System.out.printf(Locale.ROOT,
                "election-compare: %d nodes, timeout %d ms, ping every %d ms, %d misses, %d runs%n",
                IDS.size(), config.getTimeoutMs(), config.getPingIntervalMs(),
                config.getPingMisses(), runs);

        List<String> rows = new ArrayList<>();
        rows.add("algorithm,scenario,run,new_leader,messages,failover_ms,election_ms");
        Map<String, double[]> sums = new LinkedHashMap<>();
        for (String scenario : List.of("all_detect", "lowest_detects")) {
            for (String algorithm : List.of("bully", "ring")) {
                for (int run = 1; run <= runs; run++) {
                    Result result = runOnce(algorithm, scenario, config);
                    rows.add(String.format(Locale.ROOT, "%s,%s,%d,%d,%d,%d,%d", algorithm,
                            scenario, run, result.newLeader, result.messages, result.failoverMs,
                            result.electionMs));
                    double[] sum = sums.computeIfAbsent(algorithm + "," + scenario,
                            key -> new double[3]);
                    sum[0] += result.messages;
                    sum[1] += result.failoverMs;
                    sum[2] += result.electionMs;
                    System.out.printf(Locale.ROOT,
                            "  %-5s %-14s run %d: leader %d, %3d messages, failover %5d ms,"
                                    + " election %4d ms%n",
                            algorithm, scenario, run, result.newLeader, result.messages,
                            result.failoverMs, result.electionMs);
                }
            }
        }
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.write(out, rows, StandardCharsets.UTF_8);

        System.out.println();
        System.out.println("algorithm scenario        mean_messages  mean_failover_ms  mean_election_ms");
        sums.forEach((key, sum) -> {
            String[] parts = key.split(",");
            System.out.printf(Locale.ROOT, "%-9s %-15s %13.1f %17.0f %17.0f%n",
                    parts[0], parts[1], sum[0] / runs, sum[1] / runs, sum[2] / runs);
        });
        System.out.println("rows written to " + out);
    }

    private record Result(int newLeader, long messages, long failoverMs, long electionMs) {}

    private static Result runOnce(
            String algorithm, String scenario, NodeConfig.ElectionConfig config)
            throws Exception {
        boolean monitors = scenario.equals("all_detect");
        long timeoutMs = config.getTimeoutMs();
        try (InProcessElectionCluster cluster = InProcessElectionCluster.start(
                algorithm, IDS, timeoutMs, monitors ? config.getPingIntervalMs() : 0,
                config.getPingMisses())) {
            require(cluster.awaitAgreedLeader(30_000), 5, "initial election");
            // Start-up elections leave late messages in flight; let them land before measuring.
            Thread.sleep(3 * timeoutMs);
            cluster.resetStats();
            long start = System.nanoTime();
            cluster.kill(5);
            if (!monitors) {
                cluster.node(1).leaderFailed();
            }
            int leader = require(cluster.awaitAgreedLeader(30_000), 4, "failover");
            long failoverMs = (System.nanoTime() - start) / 1_000_000;
            // Coordinator hops to the dead node time out after the agreement; count them too.
            Thread.sleep(2 * timeoutMs);
            long electionMs = -1;
            for (int id : cluster.liveIds()) {
                ElectionAlgorithm node = cluster.node(id);
                electionMs = Math.max(electionMs, node.stats().lastDurationMs());
            }
            return new Result(leader, cluster.electionMessages(), failoverMs, electionMs);
        }
    }

    private static int require(Optional<Integer> leader, int expected, String what) {
        if (leader.isEmpty() || leader.get() != expected) {
            throw new IllegalStateException(what + ": expected leader " + expected + ", got "
                    + leader.map(String::valueOf).orElse("no agreement"));
        }
        return leader.get();
    }

    private static Map<String, String> options(String[] args) {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        return opts;
    }
}
