package com.predisched.benchmark.sim;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Measured execution times for the simulator (prompt 20, F17), from
 * {@code workloads/benchmark/exec-samples.csv} (scripts/export-exec-samples.py): one row per
 * completed campaign execution, normalised to a worker with no slowdown. A draw takes the
 * {@code k} samples of the task's type whose input sizes are nearest on a log scale and picks
 * one at random, so size and the run-to-run spread both carry over.
 */
public final class ExecSamples {

    record Sample(double logSize, double execMs) {}

    private final Map<String, List<Sample>> byType = new HashMap<>();
    private final int k;

    public ExecSamples(int k) {
        this.k = k;
    }

    public static ExecSamples load(Path csv, int k) throws IOException {
        ExecSamples samples = new ExecSamples(k);
        List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
        for (String line : lines.subList(1, lines.size())) {
            String[] f = line.split(",");
            if (f.length == 3) {
                samples.add(f[0], Long.parseLong(f[1]), Double.parseDouble(f[2]));
            }
        }
        samples.byType.values().forEach(l -> l.sort(Comparator.comparingDouble(Sample::logSize)));
        return samples;
    }

    public void add(String type, long inputSize, double execMs) {
        byType.computeIfAbsent(type, t -> new ArrayList<>())
                .add(new Sample(Math.log1p(Math.max(0, inputSize)), execMs));
    }

    public boolean has(String type) {
        return byType.containsKey(type);
    }

    /** One execution time (ms) for a task of this type and size; types never seen: 50 ms. */
    public double draw(String type, long inputSize, Random random) {
        List<Sample> list = byType.get(type);
        if (list == null || list.isEmpty()) {
            return 50.0;
        }
        double target = Math.log1p(Math.max(0, inputSize));
        // Binary search for the insertion point, then widen to the k nearest.
        int lo = 0;
        int hi = list.size();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (list.get(mid).logSize() < target) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        int left = lo - 1;
        int right = lo;
        List<Sample> nearest = new ArrayList<>(k);
        while (nearest.size() < Math.min(k, list.size())) {
            boolean takeLeft = right >= list.size() || (left >= 0
                    && target - list.get(left).logSize() <= list.get(right).logSize() - target);
            nearest.add(list.get(takeLeft ? left-- : right++));
        }
        return nearest.get(random.nextInt(nearest.size())).execMs();
    }
}
