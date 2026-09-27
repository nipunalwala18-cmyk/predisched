package com.predisched.scheduler.strategy;

/**
 * Drift in the execution-time predictions (prompt 21, F15): the live rolling MAE divided by the
 * model's MAE on its held-out test set. A ratio above {@code threshold} for {@code window}
 * completed tasks in a row starts an episode, reported once; the episode ends when the ratio falls
 * back to the threshold or below, and only then can a new one start. Not thread-safe on its own:
 * the strategy calls it under the accuracy tracker's order of completions.
 */
public final class DriftDetector {

    /** What one observation found. */
    public record Result(boolean newEpisode, double ratio, int above, boolean inEpisode) {}

    private final double threshold;
    private final int window;
    private int above;
    private boolean inEpisode;
    private long episodes;

    public DriftDetector(double threshold, int window) {
        this.threshold = threshold;
        this.window = Math.max(1, window);
    }

    public synchronized Result observe(double rollingMae, double baselineMae) {
        if (!(baselineMae > 0) || Double.isNaN(rollingMae)) {
            return new Result(false, Double.NaN, above, inEpisode);
        }
        double ratio = rollingMae / baselineMae;
        if (ratio > threshold) {
            above++;
            if (!inEpisode && above >= window) {
                inEpisode = true;
                episodes++;
                return new Result(true, ratio, above, true);
            }
        } else {
            above = 0;
            inEpisode = false;
        }
        return new Result(false, ratio, above, inEpisode);
    }

    public synchronized long episodes() {
        return episodes;
    }

    public double threshold() {
        return threshold;
    }

    public int window() {
        return window;
    }
}
