package com.predisched.benchmark.sim;

import com.predisched.benchmark.suite.RunMetrics;
import com.predisched.client.workload.TraceEntry;
import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyDecision;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Random;

/**
 * What-if simulator (prompt 20, F17): a discrete-event simulation of the scheduler and its
 * workers, driving a real {@link SchedulingStrategy}. It is a model, not the cluster:
 *
 * <ul>
 *   <li>Workers have pool slots and a FIFO queue; a task's execution time is drawn from the
 *       measured samples of its type ({@link ExecSamples}, nearest input sizes), times the
 *       worker's slowdown.
 *   <li>The scheduler holds tasks in a priority queue (priority, then arrival) and dispatches
 *       only to workers under {@code outstandingFactor × pool} in flight, as the real dispatcher.
 *   <li>Strategies see exact, current worker state (the real ones see heartbeats up to a second
 *       old plus the in-flight count). CPU and memory readings are 0.
 *   <li>{@code overheadMs} is added to every task's latency for dispatch, the reply and the
 *       client's status polling, which the model does not simulate.
 * </ul>
 *
 * Deterministic for a seed: the only randomness is the sampling and a seeded random strategy.
 */
public final class Simulator {

    public record WorkerModel(String id, int poolSize, double slowdown) {}

    public record Result(List<RunMetrics.TaskRow> rows, long events) {}

    private static final class SimTask {
        final TraceEntry entry;
        final TaskRecord record;
        final double arrivalMs;
        final int order;
        double startMs = -1;
        double endMs = -1;
        double execMs;
        String worker;

        SimTask(TraceEntry entry, double arrivalMs, int order) {
            this.entry = entry;
            this.arrivalMs = arrivalMs;
            this.order = order;
            this.record = TaskRecord.createQueued(entry.taskId(), entry.type(), entry.input(),
                    entry.priority());
        }
    }

    private static final class SimWorker {
        final WorkerModel model;
        int running;
        final Deque<SimTask> queue = new ArrayDeque<>();
        long completed;
        final Deque<Double> recent = new ArrayDeque<>();
        double recentSum;

        SimWorker(WorkerModel model) {
            this.model = model;
        }

        int inFlight() {
            return running + queue.size();
        }

        double avgExec() {
            return recent.isEmpty() ? 0.0 : recentSum / recent.size();
        }

        void record(double execMs) {
            recent.addLast(execMs);
            recentSum += execMs;
            if (recent.size() > 50) {
                recentSum -= recent.removeFirst();
            }
        }

        WorkerInfo info() {
            int active = Math.min(inFlight(), model.poolSize());
            int waiting = Math.max(0, inFlight() - model.poolSize());
            return new WorkerInfo(model.id(), "simulated", 0, 8, 1024, model.poolSize(),
                    model.slowdown(), 0.0, 0.0, active, waiting, completed, avgExec(), 0, 0);
        }
    }

    private record Event(double timeMs, long seq, int kind, SimTask task, SimWorker worker) {
        static final int ARRIVAL = 0;
        static final int COMPLETE = 1;
    }

    private final SchedulingStrategy strategy;
    private final List<WorkerModel> workers;
    private final ExecSamples samples;
    private final double outstandingFactor;
    private final double overheadMs;
    private final Random random;

    public Simulator(SchedulingStrategy strategy, List<WorkerModel> workers, ExecSamples samples,
            double outstandingFactor, double overheadMs, long seed) {
        this.strategy = strategy;
        this.workers = workers;
        this.samples = samples;
        this.outstandingFactor = outstandingFactor;
        this.overheadMs = overheadMs;
        this.random = new Random(seed);
    }

    public Result run(List<TraceEntry> trace, double speed) {
        List<SimWorker> pool = workers.stream().map(SimWorker::new).toList();
        PriorityQueue<Event> events = new PriorityQueue<>(Comparator
                .comparingDouble(Event::timeMs).thenComparingLong(Event::seq));
        PriorityQueue<SimTask> pending = new PriorityQueue<>(Comparator
                .comparingInt((SimTask t) -> -t.entry.priority()).thenComparingInt(t -> t.order));
        Deque<Double> arrivals = new ArrayDeque<>();
        double[] now = {0};
        strategy.attach(new SchedulingStrategy.Context(() -> {
            while (!arrivals.isEmpty() && arrivals.peekFirst() < now[0] - 10_000) {
                arrivals.removeFirst();
            }
            return arrivals.size() / 10.0;
        }));
        List<SimTask> tasks = new ArrayList<>();
        long seq = 0;
        for (int i = 0; i < trace.size(); i++) {
            SimTask task = new SimTask(trace.get(i), trace.get(i).offsetMs() / speed, i);
            tasks.add(task);
            events.add(new Event(task.arrivalMs, seq++, Event.ARRIVAL, task, null));
        }
        long processed = 0;
        while (!events.isEmpty()) {
            Event e = events.poll();
            now[0] = e.timeMs();
            processed++;
            if (e.kind() == Event.ARRIVAL) {
                arrivals.addLast(now[0]);
                pending.add(e.task());
            } else {
                SimWorker w = e.worker();
                SimTask t = e.task();
                w.running--;
                w.completed++;
                w.record(t.execMs);
                strategy.completed(t.record, w.model.id(), Math.round(t.execMs), true);
                if (!w.queue.isEmpty()) {
                    seq = start(w.queue.removeFirst(), w, now[0], events, seq);
                }
            }
            // Dispatch while a task waits and some worker is under its outstanding limit.
            while (!pending.isEmpty()) {
                List<WorkerInfo> candidates = new ArrayList<>();
                for (SimWorker w : pool) {
                    int limit = Math.max(1, (int) Math.round(w.model.poolSize()
                            * outstandingFactor));
                    if (w.inFlight() < limit) {
                        candidates.add(w.info());
                    }
                }
                if (candidates.isEmpty()) {
                    break;
                }
                SimTask t = pending.poll();
                StrategyDecision decision = strategy.decide(t.record, candidates);
                SimWorker chosen = pool.stream()
                        .filter(w -> w.model.id().equals(decision.chosen().id()))
                        .findFirst().orElseThrow();
                t.worker = chosen.model.id();
                if (chosen.running < chosen.model.poolSize()) {
                    seq = start(t, chosen, now[0], events, seq);
                } else {
                    chosen.queue.addLast(t);
                }
            }
        }
        List<RunMetrics.TaskRow> rows = new ArrayList<>();
        for (SimTask t : tasks) {
            long submit = Math.round(t.arrivalMs);
            long start = Math.round(t.startMs);
            long end = Math.round(t.endMs + overheadMs);
            rows.add(new RunMetrics.TaskRow(t.entry.taskId(), t.entry.type().name(),
                    t.entry.offsetMs(), submit, start, end, end - submit, "COMPLETED", t.worker,
                    Math.round(t.execMs)));
        }
        return new Result(rows, processed);
    }

    private long start(SimTask t, SimWorker w, double now, PriorityQueue<Event> events,
            long seq) {
        w.running++;
        t.startMs = now;
        long size = TaskInputSpec.inputSize(t.entry.type(), t.entry.input());
        t.execMs = samples.draw(t.entry.type().name(), size, random) * w.model.slowdown();
        t.endMs = now + t.execMs;
        events.add(new Event(t.endMs, seq, Event.COMPLETE, t, w));
        return seq + 1;
    }
}
