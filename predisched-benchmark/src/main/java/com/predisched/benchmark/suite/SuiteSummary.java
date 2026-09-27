package com.predisched.benchmark.suite;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * {@code summary.csv} from {@code runs.csv} (prompt 20): mean ± std per scenario × strategy for
 * every metric. For the predictive strategy each metric also gets the best baseline in that
 * scenario (best mean among the four reactive strategies, in the metric's own direction), the
 * relative difference, a two-sided Welch t-test p-value and Cohen's d.
 */
public final class SuiteSummary {

    public static final String PREDICTIVE = "predictive";

    /** Lower is better unless listed here; metrics listed as neutral get no test. */
    static final Set<String> HIGHER_IS_BETTER = Set.of("throughput_per_s", "completed");
    static final Set<String> NEUTRAL = Set.of("tasks", "cpu_mean_pct");

    public static final List<String> COLUMNS = List.of("scenario", "strategy", "metric", "n",
            "mean", "std", "best_baseline", "baseline_mean", "diff_pct", "p_value", "cohens_d");

    private SuiteSummary() {}

    /** runs.csv rows as maps: identity columns plus every metric. */
    public static List<Map<String, String>> readRuns(Path runsCsv) throws IOException {
        List<String> lines = Files.readAllLines(runsCsv, StandardCharsets.UTF_8);
        String[] header = lines.get(0).split(",");
        List<Map<String, String>> rows = new ArrayList<>();
        for (String line : lines.subList(1, lines.size())) {
            if (line.isBlank()) {
                continue;
            }
            String[] f = line.split(",", -1);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < header.length; i++) {
                row.put(header[i], i < f.length ? f[i] : "");
            }
            rows.add(row);
        }
        return rows;
    }

    public static List<Map<String, String>> summarize(List<Map<String, String>> runs) {
        Set<String> scenarios = new LinkedHashSet<>();
        Set<String> strategies = new LinkedHashSet<>();
        runs.forEach(r -> {
            scenarios.add(r.get("scenario"));
            strategies.add(r.get("strategy"));
        });
        List<Map<String, String>> out = new ArrayList<>();
        for (String scenario : scenarios) {
            for (String metric : RunMetrics.NAMES) {
                Map<String, double[]> values = new LinkedHashMap<>();
                for (String strategy : strategies) {
                    values.put(strategy, runs.stream()
                            .filter(r -> r.get("scenario").equals(scenario)
                                    && r.get("strategy").equals(strategy))
                            .mapToDouble(r -> parse(r.get(metric)))
                            .filter(v -> !Double.isNaN(v))
                            .toArray());
                }
                String best = bestBaseline(metric, values);
                for (String strategy : strategies) {
                    double[] v = values.get(strategy);
                    Map<String, String> row = new LinkedHashMap<>();
                    row.put("scenario", scenario);
                    row.put("strategy", strategy);
                    row.put("metric", metric);
                    row.put("n", String.valueOf(v.length));
                    row.put("mean", fmt(Stats.mean(v)));
                    row.put("std", fmt(Stats.std(v)));
                    boolean test = strategy.equals(PREDICTIVE) && best != null
                            && !NEUTRAL.contains(metric) && v.length > 0;
                    double[] b = best == null ? new double[0] : values.get(best);
                    row.put("best_baseline", test ? best : "");
                    row.put("baseline_mean", test ? fmt(Stats.mean(b)) : "");
                    row.put("diff_pct", test ? fmt(100 * (Stats.mean(v) - Stats.mean(b))
                            / Math.abs(Stats.mean(b))) : "");
                    row.put("p_value", test ? fmt(Stats.welchP(v, b)) : "");
                    row.put("cohens_d", test ? fmt(Stats.cohensD(v, b)) : "");
                    out.add(row);
                }
            }
        }
        return out;
    }

    /** The reactive strategy with the best mean for this metric, or null. */
    static String bestBaseline(String metric, Map<String, double[]> values) {
        String best = null;
        double bestMean = Double.NaN;
        boolean higher = HIGHER_IS_BETTER.contains(metric);
        for (Map.Entry<String, double[]> e : values.entrySet()) {
            if (e.getKey().equals(PREDICTIVE) || e.getValue().length == 0) {
                continue;
            }
            double m = Stats.mean(e.getValue());
            if (Double.isNaN(m)) {
                continue;
            }
            if (best == null || (higher ? m > bestMean : m < bestMean)) {
                best = e.getKey();
                bestMean = m;
            }
        }
        return best;
    }

    public static void write(Path file, List<Map<String, String>> rows) throws IOException {
        StringBuilder out = new StringBuilder(String.join(",", COLUMNS)).append('\n');
        for (Map<String, String> row : rows) {
            List<String> cells = new ArrayList<>();
            COLUMNS.forEach(c -> cells.add(row.getOrDefault(c, "")));
            out.append(String.join(",", cells)).append('\n');
        }
        Files.writeString(file, out.toString(), StandardCharsets.UTF_8);
    }

    /** The headline metrics as a compact table, for the console. */
    public static String table(List<Map<String, String>> summary) {
        List<String> metrics = List.of("mean_latency_ms", "p95_latency_ms", "throughput_per_s",
                "sla_violation_pct", "imbalance");
        StringBuilder out = new StringBuilder(String.format(Locale.ROOT,
                "%-14s %-15s %20s %20s %16s %14s %12s%n", "scenario", "strategy",
                "mean latency ms", "p95 latency ms", "tasks/s", "SLA viol. %", "imbalance"));
        Map<String, Map<String, Map<String, String>>> byKey = new LinkedHashMap<>();
        for (Map<String, String> r : summary) {
            byKey.computeIfAbsent(r.get("scenario") + "|" + r.get("strategy"),
                    k -> new LinkedHashMap<>()).put(r.get("metric"), r);
        }
        byKey.forEach((key, m) -> {
            String[] k = key.split("\\|");
            out.append(String.format(Locale.ROOT, "%-14s %-15s", k[0], k[1]));
            for (String metric : metrics) {
                Map<String, String> r = m.get(metric);
                out.append(String.format(Locale.ROOT, metric.equals("mean_latency_ms")
                                || metric.equals("p95_latency_ms") ? " %20s" : metric.equals(
                                "throughput_per_s") ? " %16s" : metric.equals("imbalance")
                                ? " %12s" : " %14s",
                        r == null ? "" : pm(r.get("mean"), r.get("std"))));
            }
            out.append('\n');
        });
        return out.toString();
    }

    private static String pm(String mean, String std) {
        double m = parse(mean);
        double s = parse(std);
        if (Double.isNaN(m)) {
            return "n/a";
        }
        return Double.isNaN(s) ? String.format(Locale.ROOT, "%.1f", m)
                : String.format(Locale.ROOT, "%.1f +- %.1f", m, s);
    }

    static double parse(String text) {
        if (text == null || text.isBlank()) {
            return Double.NaN;
        }
        try {
            return Double.parseDouble(text);
        } catch (NumberFormatException e) {
            return Double.NaN;
        }
    }

    static String fmt(double value) {
        return Double.isNaN(value) || Double.isInfinite(value) ? ""
                : String.format(Locale.ROOT, "%.6g", value);
    }
}
