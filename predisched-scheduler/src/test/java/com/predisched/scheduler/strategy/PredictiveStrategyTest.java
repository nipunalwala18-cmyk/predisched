package com.predisched.scheduler.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerPrediction;
import com.predisched.proto.WorkerState;
import com.predisched.scheduler.WorkerInfo;
import com.predisched.scheduler.prediction.PredictionClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The cost function on hand-set predictions, the overload skip, and every fallback path. */
class PredictiveStrategyTest {

    private static WorkerInfo worker(String id, int pool, int active, int queued, double avgExec) {
        return new WorkerInfo(id, "localhost", 0, 8, 1024, pool, 1.0, 5.0, 1.0, active, queued,
                0, avgExec, 0, 0);
    }

    private static PredictionClient.Result result(Object... perWorker) {
        // id, pred_exec_ms, pred_queue_len, overload_prob
        Map<String, WorkerPrediction> byWorker = new LinkedHashMap<>();
        for (int i = 0; i < perWorker.length; i += 4) {
            String id = (String) perWorker[i];
            byWorker.put(id, WorkerPrediction.newBuilder().setWorkerId(id)
                    .setPredExecMs(((Number) perWorker[i + 1]).doubleValue())
                    .setPredQueueLen(((Number) perWorker[i + 2]).doubleValue())
                    .setOverloadProb(((Number) perWorker[i + 3]).doubleValue()).build());
        }
        return new PredictionClient.Result(byWorker, 1, "m1=v1,m2=v1,m3=v1", 2.0, 1.5, null);
    }

    private static TaskRecord task(int priority) {
        return TaskRecord.createQueued("t-" + priority, TaskType.CPU_TASK, "n=1000", priority);
    }

    private static final PredictiveStrategy.Settings SETTINGS =
            new PredictiveStrategy.Settings(1.0, 0.8, 8, 0.5);

    private final List<WorkerInfo> workers = List.of(
            worker("w1", 2, 2, 2, 100.0),   // busy, slow history
            worker("w2", 4, 1, 0, 50.0),
            worker("w3", 8, 0, 0, 0.0));    // idle, no history yet

    @Test
    void costIsCompletionTimesOnePlusLambdaTimesOverload() {
        PredictiveStrategy strategy = new PredictiveStrategy((t, w, m, r) -> null, SETTINGS);
        StrategyDecision d = strategy.score(task(5), workers,
                result("w1", 40, 2.0, 0.5, "w2", 60, 1.0, 0.1, "w3", 90, 0.0, 0.0));
        Map<String, CandidateScore> by = new LinkedHashMap<>();
        d.breakdown().forEach(c -> by.put(c.workerId(), c));
        // w1: wait = 2 x 100 / 2 = 100; completion 140; penalty 140 x 1 x 0.5 = 70; cost 210.
        assertEquals(100.0, by.get("w1").predictedWaitMs(), 1e-9);
        assertEquals(70.0, by.get("w1").overloadPenaltyMs(), 1e-9);
        assertEquals(210.0, by.get("w1").cost(), 1e-9);
        // w2: wait = 1 x 50 / 4 = 12.5; completion 72.5; cost 72.5 x 1.1 = 79.75.
        assertEquals(12.5, by.get("w2").predictedWaitMs(), 1e-9);
        assertEquals(79.75, by.get("w2").cost(), 1e-9);
        // w3: no queue forecast, so no wait; cost = its predicted exec, 90.
        assertEquals(90.0, by.get("w3").cost(), 1e-9);
        assertEquals("w2", d.chosen().id());
        assertTrue(by.get("w2").chosen());
        assertFalse(d.fallback());
        assertEquals(Map.of("w1", 210.0, "w2", 79.75, "w3", 90.0), d.scores());
    }

    @Test
    void aWorkerWithoutHistoryUsesItsPredictedExecForTheWait() {
        PredictiveStrategy strategy = new PredictiveStrategy((t, w, m, r) -> null, SETTINGS);
        StrategyDecision d = strategy.score(task(5), workers,
                result("w1", 40, 0.0, 0.0, "w2", 60, 0.0, 0.0, "w3", 16, 2.0, 0.0));
        // w3 has avg_exec 0: wait = 2 x 16 / 8 = 4, cost 20.
        assertEquals(4.0, d.breakdown().get(2).predictedWaitMs(), 1e-9);
        assertEquals(20.0, d.breakdown().get(2).cost(), 1e-9);
        assertEquals("w3", d.chosen().id());
    }

    @Test
    void lambdaZeroIgnoresOverloadInTheCost() {
        PredictiveStrategy strategy = new PredictiveStrategy((t, w, m, r) -> null,
                new PredictiveStrategy.Settings(0.0, 1.0, 8, 1.0));
        StrategyDecision d = strategy.score(task(5), workers,
                result("w1", 10, 0.0, 0.9, "w2", 60, 0.0, 0.0, "w3", 90, 0.0, 0.0));
        assertEquals("w1", d.chosen().id());
        assertEquals(0.0, d.breakdown().get(0).overloadPenaltyMs(), 1e-9);
    }

    @Test
    void overloadedWorkersAreSkippedWhileAnotherIsBelowTheThreshold() {
        PredictiveStrategy strategy = new PredictiveStrategy((t, w, m, r) -> null, SETTINGS);
        // w1 is by far the cheapest but 0.85 > 0.8: skipped.
        StrategyDecision d = strategy.score(task(5), workers,
                result("w1", 1, 0.0, 0.85, "w2", 60, 0.0, 0.3, "w3", 90, 0.0, 0.0));
        CandidateScore w1 = d.breakdown().get(0);
        assertTrue(w1.skipped());
        assertEquals("overload 0.85 > 0.80", w1.skipReason());
        assertEquals("w2", d.chosen().id(), "78 (60 x 1.3) beats 90");
        // Everyone above the threshold: nobody is skipped, the cost decides.
        StrategyDecision all = strategy.score(task(5), workers,
                result("w1", 1, 0.0, 0.9, "w2", 60, 0.0, 0.95, "w3", 90, 0.0, 0.99));
        assertTrue(all.breakdown().stream().noneMatch(CandidateScore::skipped));
        assertEquals("w1", all.chosen().id());
    }

    @Test
    void highPriorityTasksUseTheStricterThreshold() {
        PredictiveStrategy strategy = new PredictiveStrategy((t, w, m, r) -> null, SETTINGS);
        PredictionClient.Result predictions =
                result("w1", 1, 0.0, 0.6, "w2", 60, 0.0, 0.1, "w3", 90, 0.0, 0.0);
        // Priority 5: 0.6 is under 0.8, and w1 (1 x 1.6 = 1.6) wins.
        assertEquals("w1", strategy.score(task(5), workers, predictions).chosen().id());
        // Priority 9 (>= 8): 0.6 > 0.5, w1 is skipped.
        StrategyDecision high = strategy.score(task(9), workers, predictions);
        assertEquals("w2", high.chosen().id());
        assertEquals("overload 0.60 > 0.50 (high priority)",
                high.breakdown().get(0).skipReason());
    }

    @Test
    void sendsEveryCandidatesStateAndTheRunningMeans() {
        List<List<WorkerState>> seen = new ArrayList<>();
        double[] typeMean = new double[1];
        PredictiveStrategy strategy = new PredictiveStrategy((t, states, mean, id) -> {
            seen.add(List.copyOf(states));
            typeMean[0] = mean;
            return result("w1", 10, 0, 0, "w2", 20, 0, 0, "w3", 30, 0, 0);
        }, SETTINGS);
        strategy.attach(new SchedulingStrategy.Context(() -> 3.5));
        TaskRecord done = task(5);
        strategy.completed(done, "w2", 42, true);
        strategy.completed(done, "w2", 58, true);
        strategy.decide(task(5), workers);
        WorkerState w2 = seen.get(0).get(1);
        assertEquals("w2", w2.getWorkerId());
        assertEquals(4, w2.getPoolSize());
        assertEquals(1, w2.getConcurrentTasks());
        assertEquals(3.5, w2.getArrivalRate());
        assertEquals(50.0, w2.getTypeWorkerMeanMs());
        assertEquals(2, w2.getTypeWorkerCount());
        assertEquals(0, seen.get(0).get(0).getTypeWorkerCount());
        assertEquals(50.0, typeMean[0]);
    }

    @Test
    void emptyResultFallsBackToLeastLoadedWithTheReason() {
        for (String reason : List.of("DEADLINE_EXCEEDED", "circuit breaker open",
                "UNAVAILABLE: io exception")) {
            PredictiveStrategy strategy = new PredictiveStrategy(
                    (t, w, m, r) -> new PredictionClient.Result(Map.of(), 0, "", 10.2, 0, reason),
                    SETTINGS);
            StrategyDecision d = strategy.decide(task(5), workers);
            assertTrue(d.fallback());
            assertEquals("least_loaded: " + reason, d.fallbackReason());
            // Least loaded: w3 (0 + 0) over w2 (1) and w1 (4).
            assertEquals(new LeastLoadedStrategy().select(task(5), workers).id(),
                    d.chosen().id());
            assertEquals("w3", d.chosen().id());
            assertEquals(3, d.breakdown().size());
            assertEquals(1, strategy.fallbacks());
        }
    }

    @Test
    void missingWorkerPredictionDisabledAndThrowingPredictorsAllFallBack() {
        PredictiveStrategy partial = new PredictiveStrategy(
                (t, w, m, r) -> result("w1", 10, 0, 0, "w2", 20, 0, 0), SETTINGS);
        assertEquals("least_loaded: no prediction for w3",
                partial.decide(task(5), workers).fallbackReason());
        PredictiveStrategy disabled = new PredictiveStrategy(null, SETTINGS,
                "prediction.enabled is false");
        assertEquals("least_loaded: prediction.enabled is false",
                disabled.decide(task(5), workers).fallbackReason());
        PredictiveStrategy throwing = new PredictiveStrategy((t, w, m, r) -> {
            throw new IllegalStateException("boom");
        }, SETTINGS);
        StrategyDecision d = throwing.decide(task(5), workers);
        assertTrue(d.fallback());
        assertTrue(d.fallbackReason().contains("boom"), d.fallbackReason());
    }

    @Test
    void completionFeedsTheRollingMaeOnlyForTheWorkerThatWasPredicted() {
        PredictiveStrategy strategy = new PredictiveStrategy(
                (t, w, m, r) -> result("w1", 100, 0, 0, "w2", 30, 0, 0, "w3", 50, 0, 0),
                SETTINGS);
        TaskRecord a = TaskRecord.createQueued("a", TaskType.CPU_TASK, "n=1", 5);
        TaskRecord b = TaskRecord.createQueued("b", TaskType.SORT_TASK, "n=1", 5);
        TaskRecord c = TaskRecord.createQueued("c", TaskType.CPU_TASK, "n=1", 5);
        assertEquals("w2", strategy.decide(a, workers).chosen().id());
        assertEquals("w2", strategy.decide(b, workers).chosen().id());
        assertEquals("w2", strategy.decide(c, workers).chosen().id());
        strategy.completed(a, "w2", 40, true);      // |30 - 40| = 10
        strategy.completed(b, "w2", 10, true);      // |30 - 10| = 20
        strategy.completed(c, "w1", 500, true);     // retried elsewhere: not the predicted worker
        assertEquals(2, strategy.accuracy().tasks());
        assertEquals(15.0, strategy.accuracy().mae(), 1e-9);
        assertEquals(10.0, strategy.accuracy().maeByType().get("CPU_TASK"), 1e-9);
        assertEquals(20.0, strategy.accuracy().maeByType().get("SORT_TASK"), 1e-9);
    }

    @Test
    void rollingMaeKeepsOnlyTheWindow() {
        PredictionAccuracy accuracy = new PredictionAccuracy(3);
        for (double error : new double[] {100, 1, 2, 3}) {
            accuracy.record("CPU_TASK", 0, error);
        }
        assertEquals(2.0, accuracy.mae(), 1e-9);
        assertEquals(4, accuracy.tasks());
    }
}
