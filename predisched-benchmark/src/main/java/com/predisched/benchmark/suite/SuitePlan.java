package com.predisched.benchmark.suite;

import java.util.ArrayList;
import java.util.List;

/**
 * The runs of a suite, in the order they run (prompt 20). Repetition-major: every scenario and
 * strategy runs once before any runs twice, so slow drift in the machine (heat, background load)
 * spreads over all strategies instead of landing on whichever ran last. Within a repetition the
 * strategy order rotates by one each time, for the same reason. Deterministic for a config.
 */
public final class SuitePlan {

    /**
     * @param strategy what the run compares: a strategy, or {@code scale:<mode>} in an
     *      auto-scaling scenario
     * @param schedulerStrategy the {@code --strategy} the scheduler runs with
     * @param autoscale the {@code --autoscale} mode ({@code none} outside such scenarios)
     */
    public record Run(int index, String runId, String scenario, String strategy, int rep,
            String taskSuffix, String schedulerStrategy, String autoscale) {}

    private SuitePlan() {}

    public static List<Run> plan(SuiteConfig config, int reps) {
        return plan(config, reps, config.suite());
    }

    /**
     * @param suiteId goes into every task id suffix, so two suites' rows never mix in the
     *      database even when they replay the same traces
     */
    public static List<Run> plan(SuiteConfig config, int reps, String suiteId) {
        List<Run> runs = new ArrayList<>();
        List<String> strategies = config.strategies();
        int index = 0;
        for (int rep = 1; rep <= reps; rep++) {
            for (SuiteConfig.Scenario scenario : config.scenarios()) {
                boolean scaling = scenario.variants() != null && !scenario.variants().isEmpty();
                List<String> arms = scaling ? scenario.variants() : strategies;
                for (int i = 0; i < arms.size(); i++) {
                    String arm = arms.get((i + rep - 1) % arms.size());
                    String label = scaling ? "scale:" + arm : arm;
                    index++;
                    String runId = config.suite() + "-" + scenario.name() + "-"
                            + label.replace(':', '-') + "-r" + rep;
                    runs.add(new Run(index, runId, scenario.name(), label, rep,
                            "~" + suiteId + "r" + index,
                            scaling ? scenario.strategy() : arm, scaling ? arm : "none"));
                }
            }
        }
        return runs;
    }
}
