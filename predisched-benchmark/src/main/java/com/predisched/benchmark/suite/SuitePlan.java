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

    public record Run(int index, String runId, String scenario, String strategy, int rep,
            String taskSuffix) {}

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
                for (int i = 0; i < strategies.size(); i++) {
                    String strategy = strategies.get((i + rep - 1) % strategies.size());
                    index++;
                    String runId = config.suite() + "-" + scenario.name() + "-" + strategy + "-r"
                            + rep;
                    runs.add(new Run(index, runId, scenario.name(), strategy, rep,
                            "~" + suiteId + "r" + index));
                }
            }
        }
        return runs;
    }
}
