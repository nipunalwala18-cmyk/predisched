package com.predisched.scheduler.strategy;

import com.predisched.common.time.Clocks;
import com.predisched.common.TaskRecord;
import com.predisched.common.db.History;
import com.predisched.common.obs.EventLog;
import com.predisched.proto.TaskRequest;
import com.predisched.proto.WorkerPrediction;
import com.predisched.proto.WorkerState;
import com.predisched.scheduler.WorkerInfo;
import com.predisched.scheduler.prediction.PredictionClient;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Places each task on the worker with the lowest predicted cost (spec §12.4, prompt 18):
 *
 * <pre>
 * predicted_wait(w)      = pred_queue_len(w) × avg_exec_recent(w) / pool_size(w)
 * expected_completion(w) = predicted_wait(w) + pred_exec_ms(task, w)
 * cost(w)                = expected_completion(w) × (1 + λ × overload_prob(w))
 * </pre>
 *
 * <p>A worker whose overload probability is above the threshold is skipped while any other
 * candidate is below it. Tasks of priority {@code highPriority} or more use a stricter
 * threshold. A worker that has not finished a task yet has no recent average, so its
 * {@code pred_exec_ms} stands in for it.
 *
 * <p>The predictions come from the prediction server in one call per task. If that call returns
 * nothing (deadline, breaker open, server down, prediction disabled), the decision falls back to
 * {@link LeastLoadedStrategy} and is marked {@code fallback} with the reason (FR19).
 *
 * <p>Every decision keeps a per-candidate breakdown (F13). When an attempt completes, its actual
 * execution time is compared with the prediction for the worker it ran on, which feeds a rolling
 * MAE ({@link PredictionAccuracy}) in the event log, the log and the database.
 */
public final class PredictiveStrategy implements SchedulingStrategy {

    private static final Logger log = LoggerFactory.getLogger(PredictiveStrategy.class);
    private static final int ACCURACY_WINDOW = 500;
    private static final int ACCURACY_LOG_EVERY = 50;
    private static final int FALLBACK_LOG_EVERY = 100;

    /** Where predictions come from; {@link PredictionClient} in production, a fake in tests. */
    public interface Predictor {
        PredictionClient.Result predict(TaskRequest task, Collection<WorkerState> workers,
                double typeMeanMs, String requestId);
    }

    /** The tunable part of the cost function (config {@code scheduling.predictive}). */
    public record Settings(double lambda, double overloadThreshold, int highPriority,
            double highPriorityOverloadThreshold) {

        public static Settings defaults() {
            return new Settings(1.0, 0.8, 8, 0.5);
        }

        double thresholdFor(int priority) {
            return priority >= highPriority ? highPriorityOverloadThreshold : overloadThreshold;
        }
    }

    /** The prediction used for the chosen worker, kept until the attempt ends. */
    private record Pending(String workerId, double predExecMs, boolean coldStart,
            String modelVersions) {}

    private final Predictor predictor;
    private final Settings settings;
    private final String disabledReason;
    private final LeastLoadedStrategy fallback = new LeastLoadedStrategy();
    private final ExecStats stats = new ExecStats();
    private final PredictionAccuracy accuracy = new PredictionAccuracy(ACCURACY_WINDOW);
    private final Map<String, Pending> pending = new ConcurrentHashMap<>();
    private final AtomicLong fallbacks = new AtomicLong();
    private volatile DoubleSupplier arrivalRate = () -> 0.0;
    private volatile String lastFallbackReason = "";

    public PredictiveStrategy(Predictor predictor, Settings settings) {
        this(predictor, settings, null);
    }

    /** With no predictor every decision falls back, with {@code disabledReason} as the reason. */
    public PredictiveStrategy(Predictor predictor, Settings settings, String disabledReason) {
        this.predictor = predictor;
        this.settings = settings;
        this.disabledReason = disabledReason == null ? "prediction disabled" : disabledReason;
    }

    @Override
    public String name() {
        return "predictive";
    }

    public Settings settings() {
        return settings;
    }

    public PredictionAccuracy accuracy() {
        return accuracy;
    }

    public ExecStats execStats() {
        return stats;
    }

    public long fallbacks() {
        return fallbacks.get();
    }

    @Override
    public void attach(Context context) {
        this.arrivalRate = context.arrivalRatePerSecond();
    }

    @Override
    public WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates) {
        return decide(task, candidates).chosen();
    }

    @Override
    public StrategyDecision decide(TaskRecord task, List<WorkerInfo> candidates) {
        if (predictor == null) {
            return fallBack(task, candidates, disabledReason);
        }
        String type = task.type().name();
        List<WorkerState> states = new ArrayList<>(candidates.size());
        double rate = arrivalRate.getAsDouble();
        for (WorkerInfo w : candidates) {
            states.add(WorkerState.newBuilder()
                    .setWorkerId(w.id())
                    .setCores(w.cores())
                    .setCpuPct(w.cpuPct())
                    .setMemPct(w.memPct())
                    .setActiveThreads(w.activeThreads())
                    .setQueueLen(w.queueLen())
                    .setAvgExecMs(w.avgExecMs())
                    .setArrivalRate(rate)
                    .setPoolSize(w.poolSize())
                    .setSlowdown(w.slowdown())
                    .setConcurrentTasks(w.load())
                    .setTypeWorkerMeanMs(stats.mean(type, w.id()))
                    .setTypeWorkerCount(stats.count(type, w.id()))
                    .build());
        }
        TaskRequest request = TaskRequest.newBuilder()
                .setTaskId(task.id())
                .setType(task.type())
                .setInput(task.input())
                .setPriority(task.priority())
                .build();
        PredictionClient.Result result;
        try {
            result = predictor.predict(request, states, stats.mean(type), task.id());
        } catch (RuntimeException e) {
            // PredictionClient never throws; a test double or a future predictor might.
            return fallBack(task, candidates, "predictor error: " + e);
        }
        if (result == null || result.isEmpty()) {
            return fallBack(task, candidates,
                    result == null ? "no prediction" : result.failure());
        }
        for (WorkerInfo w : candidates) {
            if (!result.byWorker().containsKey(w.id())) {
                return fallBack(task, candidates, "no prediction for " + w.id());
            }
        }
        StrategyDecision decision = score(task, candidates, result);
        CandidateScore winner = decision.breakdown().stream()
                .filter(CandidateScore::chosen).findFirst().orElseThrow();
        pending.put(task.id(), new Pending(winner.workerId(), winner.predictedExecMs(),
                winner.coldStart(), result.modelVersions()));
        return decision;
    }

    /** The cost function over one set of predictions; package-private for the unit tests. */
    StrategyDecision score(TaskRecord task, List<WorkerInfo> candidates,
            PredictionClient.Result result) {
        double threshold = settings.thresholdFor(task.priority());
        List<CandidateScore> lines = new ArrayList<>(candidates.size());
        boolean anyBelow = false;
        for (WorkerInfo w : candidates) {
            if (result.byWorker().get(w.id()).getOverloadProb() <= threshold) {
                anyBelow = true;
            }
        }
        for (WorkerInfo w : candidates) {
            WorkerPrediction p = result.byWorker().get(w.id());
            double execMs = Math.max(0.0, p.getPredExecMs());
            double perTask = w.avgExecMs() > 0 ? w.avgExecMs() : execMs;
            double waitMs = Math.max(0.0, p.getPredQueueLen()) * perTask
                    / Math.max(1, w.poolSize());
            double completion = waitMs + execMs;
            double prob = Math.min(1.0, Math.max(0.0, p.getOverloadProb()));
            double penalty = completion * settings.lambda() * prob;
            double cost = completion + penalty;
            boolean skipped = anyBelow && prob > threshold;
            String why = skipped ? String.format(Locale.ROOT, "overload %.2f > %.2f%s", prob,
                    threshold, threshold == settings.highPriorityOverloadThreshold()
                            && task.priority() >= settings.highPriority()
                            ? " (high priority)" : "") : "";
            lines.add(new CandidateScore(w.id(), cost, false, p.getPredQueueLen(), waitMs,
                    execMs, prob, penalty, cost, skipped, why, p.getColdStart()));
        }
        CandidateScore best = null;
        for (CandidateScore line : lines) {
            if (line.skipped()) {
                continue;
            }
            if (best == null || line.cost() < best.cost()
                    || (line.cost() == best.cost() && line.workerId().compareTo(best.workerId()) < 0)) {
                best = line;
            }
        }
        Map<String, Double> scores = new LinkedHashMap<>();
        List<CandidateScore> breakdown = new ArrayList<>(lines.size());
        WorkerInfo chosen = null;
        for (int i = 0; i < lines.size(); i++) {
            CandidateScore line = lines.get(i);
            scores.put(line.workerId(), line.cost());
            if (line == best) {
                line = line.asChosen();
                chosen = candidates.get(i);
            }
            breakdown.add(line);
        }
        return new StrategyDecision(chosen, scores, breakdown, false, "",
                result.modelVersions());
    }

    private StrategyDecision fallBack(TaskRecord task, List<WorkerInfo> candidates, String why) {
        long n = fallbacks.incrementAndGet();
        if (!why.equals(lastFallbackReason) || n % FALLBACK_LOG_EVERY == 1) {
            log.warn("Predictive fallback to least_loaded for {} ({}): {}", task.id(),
                    n == 1 ? "first" : n + " so far", why);
            lastFallbackReason = why;
        }
        WorkerInfo chosen = fallback.select(task, candidates);
        Map<String, Double> scores = fallback.scores(task, candidates);
        return new StrategyDecision(chosen, scores,
                StrategyDecision.breakdownOf(chosen, scores, candidates), true,
                "least_loaded: " + why, "");
    }

    @Override
    public void completed(TaskRecord task, String workerId, long execMs, boolean success) {
        if (!success) {
            pending.remove(task.id());
            return;
        }
        String type = task.type().name();
        stats.record(type, workerId, execMs);
        Pending p = pending.remove(task.id());
        if (p == null || !p.workerId().equals(workerId)) {
            return; // placed by the fallback, or retried on another worker
        }
        PredictionAccuracy.Snapshot s = accuracy.record(type, p.predExecMs(), execMs);
        long now = Clocks.now();
        EventLog.get().event(EventLog.PREDICTION_OUTCOME, task.id(), Map.of(
                "worker", workerId,
                "task_type", type,
                "pred_exec_ms", String.format(Locale.ROOT, "%.1f", p.predExecMs()),
                "actual_exec_ms", String.valueOf(execMs),
                "abs_error_ms", String.format(Locale.ROOT, "%.1f",
                        Math.abs(p.predExecMs() - execMs)),
                "mae_ms", String.format(Locale.ROOT, "%.1f", s.mae()),
                "type_mae_ms", String.format(Locale.ROOT, "%.1f", s.typeMae())));
        History.get().predictionOutcome(task.id(), type, workerId, p.predExecMs(), execMs,
                p.coldStart(), p.modelVersions(), s.mae(), s.typeMae(), now);
        if (s.tasks() % ACCURACY_LOG_EVERY == 0) {
            log.info("Live exec-time MAE over the last {} predicted tasks: {} ms (by type: {})",
                    s.window(), String.format(Locale.ROOT, "%.1f", s.mae()),
                    formatByType(accuracy.maeByType()));
        }
    }

    private static String formatByType(Map<String, Double> byType) {
        StringBuilder out = new StringBuilder();
        byType.forEach((type, mae) -> out.append(out.length() == 0 ? "" : ", ")
                .append(type.replace("_TASK", "")).append('=')
                .append(String.format(Locale.ROOT, "%.1f", mae)));
        return out.toString();
    }
}
