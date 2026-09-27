package com.predisched.benchmark.suite;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Metric maths on a hand-made results file, the statistics, the plan and the trace manifest. */
class SuiteTest {

    @TempDir
    Path dir;

    private static RunMetrics.TaskRow row(String id, long offset, long submit, long start,
            long end, String status, String worker) {
        return new RunMetrics.TaskRow(id, "CPU_TASK", offset, submit, start, end, end - submit,
                status, worker, end - start);
    }

    @Test
    void metricsFromAHandMadeResultsFile() throws Exception {
        Path csv = dir.resolve("run.csv");
        Files.writeString(csv, String.join("\n",
                "task_id,task_type,priority,offset_ms,submit_ms,start_ms,end_ms,latency_ms,status,worker,exec_ms",
                "t0,CPU_TASK,5,0,1000,1000,1500,500,COMPLETED,worker-1,500",     // warm-up
                "t1,CPU_TASK,5,100,1100,1100,1200,100,COMPLETED,worker-1,100",
                "t2,CPU_TASK,5,200,1200,1300,1400,200,COMPLETED,worker-2,100",
                "t3,CPU_TASK,5,300,1300,1400,1600,300,COMPLETED,worker-2,200",
                "t4,CPU_TASK,5,400,1400,1500,3900,2500,COMPLETED,worker-2,2400", // misses SLA
                "t5,CPU_TASK,5,500,1500,,2000,500,FAILED,worker-1,0") + "\n",
                StandardCharsets.UTF_8);
        List<RunMetrics.TaskRow> rows = RunMetrics.read(csv);
        assertEquals(6, rows.size());
        Map<String, Double> m = RunMetrics.fromReplay(rows,
                List.of("worker-1", "worker-2", "worker-3"), 1, 2000);
        assertEquals(6.0, m.get("tasks"));
        assertEquals(5.0, m.get("completed"));
        assertEquals(1.0, m.get("failed"));
        // Warm-up t0 left out: latencies 100, 200, 300, 2500.
        assertEquals(775.0, m.get("mean_latency_ms"), 1e-9);
        assertEquals(2500.0, m.get("p95_latency_ms"));
        assertEquals(2500.0, m.get("p99_latency_ms"));
        // 5 completed over 1000..3900.
        assertEquals(2900.0, m.get("makespan_ms"));
        assertEquals(5 * 1000.0 / 2900, m.get("throughput_per_s"), 1e-9);
        // Completed per worker 2, 3, 0 (worker-3 idle counts): population std of {2, 3, 0}.
        assertEquals(Math.sqrt((1.0 / 9 + 16.0 / 9 + 25.0 / 9) / 3), m.get("imbalance"), 1e-9);
        // t4 over the deadline and t5 failed: 2 of 6.
        assertEquals(100.0 * 2 / 6, m.get("sla_violation_pct"), 1e-9);
        // Waiting at once: t2 [1200,1300), t3 [1300,1400), t4 [1400,1500), t5 [1500,2000).
        assertEquals(1.0, m.get("max_queue"));
    }

    @Test
    void percentilesAndQueueSweep() {
        double[] sorted = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        assertEquals(5.0, RunMetrics.percentile(sorted, 50));
        assertEquals(10.0, RunMetrics.percentile(sorted, 95));
        assertEquals(1.0, RunMetrics.percentile(sorted, 1));
        assertTrue(Double.isNaN(RunMetrics.percentile(new double[0], 50)));
        List<RunMetrics.TaskRow> burst = List.of(
                row("a", 0, 0, 30, 40, "COMPLETED", "w"),
                row("b", 0, 10, 50, 60, "COMPLETED", "w"),
                row("c", 0, 20, 30, 70, "COMPLETED", "w"),
                row("d", 0, 30, 35, 80, "COMPLETED", "w"));
        // At 20-30: a, b, c wait (3); at 30 a and c start as d arrives.
        assertEquals(3, RunMetrics.maxQueue(burst));
    }

    @Test
    void welchTestAndEffectSizeMatchScipy() {
        // scipy.stats.ttest_ind(a, b, equal_var=False).pvalue
        assertEquals(0.10753119493062718,
                Stats.welchP(new double[] {1, 2, 3, 4, 5}, new double[] {2, 4, 6, 8, 10}), 1e-6);
        double[] a = {101.2, 98.7, 105.1, 99.9, 102.3};
        double[] b = {95.1, 97.3, 94.8, 96.0, 98.2};
        assertEquals(0.005712675617121665, Stats.welchP(a, b), 1e-6);
        assertEquals(2.5616538096112342, Stats.cohensD(a, b), 1e-9);
        assertEquals(1.0, Stats.welchP(new double[] {3, 3}, new double[] {3, 3}));
        assertTrue(Double.isNaN(Stats.welchP(new double[] {1}, new double[] {2, 3})));
    }

    @Test
    void summaryComparesPredictiveWithTheBestBaselineInTheMetricsDirection() {
        List<Map<String, String>> runs = new java.util.ArrayList<>();
        double[][] latency = {{200, 210, 190}, {150, 160, 140}, {120, 125, 115}};
        String[] strategies = {"round_robin", "least_loaded", "predictive"};
        for (int s = 0; s < 3; s++) {
            for (double v : latency[s]) {
                Map<String, String> r = new java.util.HashMap<>();
                r.put("scenario", "steady");
                r.put("strategy", strategies[s]);
                r.put("mean_latency_ms", String.valueOf(v));
                r.put("throughput_per_s", String.valueOf(s == 0 ? 20 : 10));
                runs.add(r);
            }
        }
        List<Map<String, String>> summary = SuiteSummary.summarize(runs);
        Map<String, String> lat = summary.stream().filter(r -> r.get("strategy")
                .equals("predictive") && r.get("metric").equals("mean_latency_ms"))
                .findFirst().orElseThrow();
        assertEquals("least_loaded", lat.get("best_baseline"), "lowest latency baseline");
        assertEquals(-20.0, Double.parseDouble(lat.get("diff_pct")), 1e-6);
        assertTrue(Double.parseDouble(lat.get("p_value")) < 0.05);
        Map<String, String> tput = summary.stream().filter(r -> r.get("strategy")
                .equals("predictive") && r.get("metric").equals("throughput_per_s"))
                .findFirst().orElseThrow();
        assertEquals("round_robin", tput.get("best_baseline"), "highest throughput baseline");
    }

    private static SuiteConfig config(Path root) throws Exception {
        Files.createDirectories(root.resolve("configs"));
        Files.copy(Paths.get("..", "configs", "workloads.yaml"),
                root.resolve("configs/workloads.yaml"));
        Path yaml = root.resolve("configs/benchmark.yaml");
        Files.writeString(yaml, """
                suite: "t"
                nodeConfig: "configs/benchmark-node.yaml"
                profilesFile: "configs/workloads.yaml"
                traceDir: "workloads/benchmark"
                httpBaseUrl: "http://localhost:8100"
                strategies: [round_robin, least_loaded, predictive]
                reps: 2
                speed: 1.0
                warmupFraction: 0.1
                deadlineMs: 2000
                runTimeoutMs: 60000
                workerSets:
                  small:
                    - {id: worker-1, port: 51061, poolSize: 2, slowdown: 1.0}
                scenarios:
                  - {name: a, profile: mixed, pattern: steady, rate: 10, tasks: 20, seed: 7, workers: small}
                  - {name: b, profile: bursty, pattern: bursty, rate: 10, tasks: 20, seed: 8, workers: small}
                """, StandardCharsets.UTF_8);
        return SuiteConfig.load(yaml);
    }

    @Test
    void thePlanIsDeterministicAndRotatesTheStrategyOrder() throws Exception {
        SuiteConfig config = config(dir);
        List<SuitePlan.Run> first = SuitePlan.plan(config, 2, "t-1");
        assertEquals(first, SuitePlan.plan(config, 2, "t-1"));
        assertEquals(2 * 2 * 3, first.size());
        Set<String> ids = new HashSet<>();
        Set<String> suffixes = new HashSet<>();
        first.forEach(r -> {
            ids.add(r.runId());
            suffixes.add(r.taskSuffix());
        });
        assertEquals(first.size(), ids.size());
        assertEquals(first.size(), suffixes.size());
        assertEquals("round_robin", first.get(0).strategy());
        assertEquals("least_loaded", first.get(6).strategy(), "rep 2 starts one strategy later");
        assertNotEquals(SuitePlan.plan(config, 2, "t-2").get(0).taskSuffix(),
                first.get(0).taskSuffix(), "another suite's tasks never share ids");
    }

    @Test
    void tracesAreGeneratedOnceThenReplayedAndCheckedAgainstTheManifest() throws Exception {
        SuiteConfig config = config(dir);
        Map<String, String> manifest = BenchmarkTraces.ensure(config, dir);
        assertEquals(2, manifest.size());
        SuiteConfig.Scenario a = config.scenario("a");
        Path trace = dir.resolve(a.traceFile(config.traceDir()));
        long written = Files.getLastModifiedTime(trace).toMillis();
        String hash = BenchmarkTraces.sha256(trace);
        Thread.sleep(20);
        assertEquals(manifest, BenchmarkTraces.ensure(config, dir), "second call regenerates nothing");
        assertEquals(written, Files.getLastModifiedTime(trace).toMillis());
        assertEquals(20, BenchmarkTraces.read(config, dir, a).size());
        assertEquals(hash, manifest.get(trace.getFileName().toString()));
        Files.writeString(trace, Files.readString(trace).replaceFirst("\"priority\":\\d+",
                "\"priority\":1"), StandardCharsets.UTF_8);
        IllegalStateException changed = assertThrows(IllegalStateException.class,
                () -> BenchmarkTraces.read(config, dir, a));
        assertTrue(changed.getMessage().contains("never regenerated"), changed.getMessage());
    }
}
