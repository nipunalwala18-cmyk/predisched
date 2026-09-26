package com.predisched.benchmark;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import com.predisched.replication.InProcessReplicaCluster;
import com.predisched.replication.ReplicationException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Exp 5 measurement: strong (N=3, W=2, R=2) against eventual consistency on three in-process
 * replicas. Replication traffic from the writer is delayed 5 ms to replica 2 and 50 ms to replica
 * 3, so replica 3 lags. For each mode, with no replica down and then with replica 2 down, node 1
 * writes {@code --writes} new tasks; right after each write the task is read through replica 3.
 * Recorded: write latency p50/p95, failed writes, stale reads (the read missed the write), and
 * the time from the last write until every live replica holds the same state.
 */
public final class ConsistencyCompare {

    private static final long DELAY_TO_2_MS = 5;
    private static final long DELAY_TO_3_MS = 50;

    private ConsistencyCompare() {}

    public static void main(String[] args) throws Exception {
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            opts.put(args[i], args[i + 1]);
        }
        int writes = Integer.parseInt(opts.getOrDefault("--writes", "500"));
        Path out = Paths.get(opts.getOrDefault("--out", "results/exp5-consistency.csv"));
        System.out.printf(Locale.ROOT, "consistency-compare: 3 replicas, W=2 R=2, %d writes per"
                + " row, delay to replica 2 %d ms, to replica 3 %d ms%n",
                writes, DELAY_TO_2_MS, DELAY_TO_3_MS);

        List<String> rows = new ArrayList<>();
        rows.add("mode,replicas_down,writes,failed_writes,write_p50_ms,write_p95_ms,"
                + "stale_reads,convergence_ms");
        System.out.println("mode      down  failed  write_p50_ms  write_p95_ms  stale_reads"
                + "  convergence_ms");
        for (String mode : List.of("strong", "eventual")) {
            for (int down = 0; down <= 1; down++) {
                Row row = run(mode, down, writes);
                rows.add(String.format(Locale.ROOT, "%s,%d,%d,%d,%.2f,%.2f,%d,%d", mode, down,
                        writes, row.failed, row.p50, row.p95, row.stale, row.convergenceMs));
                System.out.printf(Locale.ROOT, "%-9s %4d  %6d  %12.2f  %12.2f  %11d  %14d%n",
                        mode, down, row.failed, row.p50, row.p95, row.stale, row.convergenceMs);
            }
        }
        if (out.getParent() != null) {
            Files.createDirectories(out.getParent());
        }
        Files.write(out, rows, StandardCharsets.UTF_8);
        System.out.println("rows written to " + out);
    }

    private record Row(int failed, double p50, double p95, int stale, long convergenceMs) {}

    private static Row run(String mode, int down, int writes) throws Exception {
        try (InProcessReplicaCluster cluster =
                     InProcessReplicaCluster.start(mode, 3, 2, 2, 1_000)) {
            cluster.setDelay(1, 2, DELAY_TO_2_MS);
            cluster.setDelay(1, 3, DELAY_TO_3_MS);
            if (down == 1) {
                cluster.kill(2);
            }
            double[] latencies = new double[writes];
            int ok = 0;
            int failed = 0;
            int stale = 0;
            for (int i = 0; i < writes; i++) {
                String id = mode + "-" + down + "-" + i;
                TaskRecord task = TaskRecord.createQueued(id, TaskType.SLEEP_TASK, "ms=5", 5);
                long start = System.nanoTime();
                try {
                    cluster.store(1).put(task);
                } catch (ReplicationException e) {
                    failed++;
                    continue;
                }
                latencies[ok++] = (System.nanoTime() - start) / 1_000_000.0;
                if (cluster.store(3).get(id) == null) {
                    stale++;
                }
            }
            long convergenceMs = cluster.awaitConverged(30_000);
            double[] measured = Arrays.copyOf(latencies, ok);
            Arrays.sort(measured);
            return new Row(failed, percentile(measured, 50), percentile(measured, 95), stale,
                    convergenceMs);
        }
    }

    private static double percentile(double[] sorted, int p) {
        if (sorted.length == 0) {
            return -1;
        }
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(sorted.length - 1, index))];
    }
}
