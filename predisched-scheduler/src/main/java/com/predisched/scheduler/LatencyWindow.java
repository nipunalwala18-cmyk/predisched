package com.predisched.scheduler;

import java.util.Arrays;

/**
 * The last {@code capacity} latencies (ms), for percentiles in periodic log lines: prediction
 * calls (prompt 17) and scheduling decisions (prompt 18). Guarded by this.
 */
public final class LatencyWindow {

    private final double[] values;
    private int next;
    private int size;

    public LatencyWindow(int capacity) {
        values = new double[capacity];
    }

    public synchronized void add(double value) {
        values[next] = value;
        next = (next + 1) % values.length;
        size = Math.min(size + 1, values.length);
    }

    public synchronized int size() {
        return size;
    }

    /** Nearest-rank percentiles; zeros when empty. */
    public synchronized double[] percentiles(double... ps) {
        double[] out = new double[ps.length];
        if (size == 0) {
            return out;
        }
        double[] sorted = Arrays.copyOf(values, size);
        Arrays.sort(sorted);
        for (int i = 0; i < ps.length; i++) {
            int rank = (int) Math.ceil(ps[i] / 100.0 * size) - 1;
            out[i] = sorted[Math.max(0, Math.min(size - 1, rank))];
        }
        return out;
    }
}
