package com.predisched.scheduler;

import com.predisched.proto.CandidateBreakdown;
import com.predisched.proto.DecisionExplanation;
import com.predisched.proto.SetStrategyResponse;
import com.predisched.scheduler.strategy.CandidateScore;
import com.predisched.scheduler.strategy.SchedulingDecision;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyRegistry;

/**
 * The scheduler's admin operations over its dispatcher (prompt 18): switch the strategy at
 * runtime ({@code SetStrategy}, used by {@code workload replay --strategy}) and explain one
 * placement from the in-memory decision log ({@code ExplainDecision}, F13).
 */
public final class DispatcherAdmin {

    private final Dispatcher dispatcher;
    private final StrategyRegistry strategies;
    private final StrategyRegistry.Settings settings;
    private final WorkerRegistry workers;

    public DispatcherAdmin(Dispatcher dispatcher, StrategyRegistry strategies,
            StrategyRegistry.Settings settings) {
        this(dispatcher, strategies, settings, null);
    }

    public DispatcherAdmin(Dispatcher dispatcher, StrategyRegistry strategies,
            StrategyRegistry.Settings settings, WorkerRegistry workers) {
        this.dispatcher = dispatcher;
        this.strategies = strategies;
        this.settings = settings;
        this.workers = workers;
    }

    /** Every registered worker and where it listens (prompt 19: chaos CLI). */
    public com.predisched.proto.WorkerList listWorkers() {
        com.predisched.proto.WorkerList.Builder out = com.predisched.proto.WorkerList.newBuilder();
        if (workers == null) {
            return out.build();
        }
        java.util.Set<String> healthy = new java.util.HashSet<>();
        workers.healthy().forEach(w -> healthy.add(w.id()));
        for (WorkerInfo w : workers.all()) {
            out.addWorkers(com.predisched.proto.WorkerEntry.newBuilder()
                    .setWorkerId(w.id()).setHost(w.host()).setPort(w.port())
                    .setPoolSize(w.poolSize()).setHealthy(healthy.contains(w.id()))
                    .setDraining(workers.isDraining(w.id()))
                    .setActiveThreads(w.activeThreads()).setQueueLen(w.queueLen()));
        }
        return out.build();
    }

    public SetStrategyResponse setStrategy(String name) {
        String previous = dispatcher.strategy().name();
        if (name.equals(previous)) {
            return SetStrategyResponse.newBuilder().setOk(true).setPrevious(previous)
                    .setCurrent(previous).setMessage("already " + previous).build();
        }
        SchedulingStrategy next;
        try {
            next = strategies.create(name, settings);
        } catch (IllegalArgumentException e) {
            return SetStrategyResponse.newBuilder().setOk(false).setPrevious(previous)
                    .setCurrent(previous).setMessage(e.getMessage()).build();
        }
        dispatcher.setStrategy(next);
        return SetStrategyResponse.newBuilder().setOk(true).setPrevious(previous)
                .setCurrent(next.name()).setMessage("strategy changed").build();
    }

    public DecisionExplanation explain(String taskId) {
        SchedulingDecision d = dispatcher.decisions().find(taskId);
        if (d == null) {
            return DecisionExplanation.newBuilder().setFound(false).setTaskId(taskId)
                    .setMessage("no decision for " + taskId + " among this scheduler's last "
                            + "10000 (the scheduling_decisions table keeps all of them)")
                    .build();
        }
        DecisionExplanation.Builder out = DecisionExplanation.newBuilder()
                .setFound(true)
                .setTaskId(d.taskId())
                .setStrategy(d.strategy())
                .setChosenWorker(d.workerId())
                .setFallback(d.fallback())
                .setFallbackReason(d.fallbackReason() == null ? "" : d.fallbackReason())
                .setDecisionUs(d.decisionMicros())
                .setAtMs(d.atMs())
                .setModelVersions(d.modelVersions() == null ? "" : d.modelVersions());
        if (d.breakdown().isEmpty()) {
            // A decision recorded without a breakdown: its scores are all there is.
            d.scores().forEach((worker, score) -> out.addCandidates(CandidateBreakdown.newBuilder()
                    .setWorkerId(worker).setScore(score).setChosen(worker.equals(d.workerId()))
                    .setPredictedWaitMs(Double.NaN).setPredictedExecMs(Double.NaN)
                    .setOverloadProb(Double.NaN).setOverloadPenaltyMs(Double.NaN)
                    .setCost(Double.NaN).setPredQueueLen(Double.NaN)));
        }
        for (CandidateScore c : d.breakdown()) {
            out.addCandidates(CandidateBreakdown.newBuilder()
                    .setWorkerId(c.workerId())
                    .setScore(c.score())
                    .setChosen(c.chosen())
                    .setPredQueueLen(c.predQueueLen())
                    .setPredictedWaitMs(c.predictedWaitMs())
                    .setPredictedExecMs(c.predictedExecMs())
                    .setOverloadProb(c.overloadProb())
                    .setOverloadPenaltyMs(c.overloadPenaltyMs())
                    .setCost(c.cost())
                    .setSkipped(c.skipped())
                    .setSkipReason(c.skipReason() == null ? "" : c.skipReason())
                    .setColdStart(c.coldStart()));
        }
        return out.build();
    }
}
