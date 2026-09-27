package com.predisched.scheduler.autoscale;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import com.predisched.proto.WorkerPrediction;
import com.predisched.proto.WorkerState;
import com.predisched.scheduler.WorkerInfo;
import com.predisched.scheduler.prediction.PredictionClient;
import com.predisched.scheduler.strategy.PredictiveStrategy;
import java.util.ArrayList;
import java.util.List;

/**
 * The demand the auto-scaler compares with capacity (prompt 21): tasks waiting in the scheduler
 * plus what the workers are and will be doing. Two variants behind one interface, so the benchmark
 * compares them on the same traces.
 */
public interface Forecaster {

    String name();

    double demand(ClusterView.State state);

    /** Now: the scheduler's queue plus every task outstanding on a worker. */
    static Forecaster reactive() {
        return new Forecaster() {
            @Override
            public String name() {
                return "reactive";
            }

            @Override
            public double demand(ClusterView.State state) {
                return state.schedulerQueue() + state.inFlight().values().stream()
                        .mapToInt(Integer::intValue).sum();
            }
        };
    }

    /**
     * The M2 queue forecast: the scheduler's queue plus, per worker, its running tasks and its
     * predicted queue in the horizon (the dataset's N = 5 s). One prediction call per check, for
     * a probe task; falls back to the reactive demand when there is no answer.
     */
    static Forecaster predictive(PredictiveStrategy.Predictor predictor) {
        Forecaster fallback = reactive();
        TaskRecord probe = TaskRecord.createQueued("autoscale-probe", TaskType.CPU_TASK,
                "n=200000", 5);
        return new Forecaster() {
            @Override
            public String name() {
                return "predictive";
            }

            @Override
            public double demand(ClusterView.State state) {
                if (state.workers().isEmpty()) {
                    return fallback.demand(state);
                }
                List<WorkerState> workers = new ArrayList<>();
                for (WorkerInfo w : state.workers()) {
                    workers.add(WorkerState.newBuilder().setWorkerId(w.id()).setCores(w.cores())
                            .setCpuPct(w.cpuPct()).setMemPct(w.memPct())
                            .setActiveThreads(w.activeThreads()).setQueueLen(w.queueLen())
                            .setAvgExecMs(w.avgExecMs()).setPoolSize(w.poolSize())
                            .setSlowdown(w.slowdown()).setConcurrentTasks(w.load()).build());
                }
                PredictionClient.Result result = predictor.predict(
                        com.predisched.proto.TaskRequest.newBuilder().setTaskId(probe.id())
                                .setType(probe.type()).setInput(probe.input())
                                .setPriority(probe.priority()).build(),
                        workers, 0.0, "autoscale");
                if (result == null || result.isEmpty()) {
                    return fallback.demand(state);
                }
                double demand = state.schedulerQueue();
                for (WorkerInfo w : state.workers()) {
                    int inFlight = state.inFlight().getOrDefault(w.id(), 0);
                    WorkerPrediction p = result.byWorker().get(w.id());
                    demand += Math.min(inFlight, w.poolSize())
                            + (p == null ? Math.max(0, inFlight - w.poolSize())
                                    : Math.max(0.0, p.getPredQueueLen()));
                }
                return demand;
            }
        };
    }
}
