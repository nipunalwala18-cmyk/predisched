package com.predisched.scheduler;

import com.predisched.common.NodeConfig;
import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.common.time.Clocks;
import com.predisched.scheduler.queue.RunningTasks;
import java.util.OptionalDouble;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds stragglers (prompt 19, F11). Every {@code speculation.checkMs} it looks at each running
 * original attempt: one that has run longer than {@code max(k x predicted exec, p90 exec of its
 * type)} (the p90 alone when there is no prediction) gets a speculative copy through
 * {@link Dispatcher#speculate}. Tasks with neither a prediction nor enough history are left alone.
 */
public final class StragglerDetector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(StragglerDetector.class);

    private final Dispatcher dispatcher;
    private final RunningTasks running;
    private final TaskStore store;
    private final NodeConfig.SpeculationConfig config;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread t = new Thread(r, "straggler-detector");
                t.setDaemon(true);
                return t;
            });

    public StragglerDetector(Dispatcher dispatcher, RunningTasks running, TaskStore store,
            NodeConfig.SpeculationConfig config) {
        this.dispatcher = dispatcher;
        this.running = running;
        this.store = store;
        this.config = config;
    }

    public void start() {
        timer.scheduleWithFixedDelay(() -> {
            try {
                check();
            } catch (RuntimeException e) {
                log.warn("Straggler check failed: {}", e.toString());
            }
        }, config.getCheckMs(), config.getCheckMs(), TimeUnit.MILLISECONDS);
        log.info("Speculative execution on: check every {} ms, straggler after max({} x"
                + " predicted, p90 of its type), p90 after {} runs", config.getCheckMs(),
                config.getK(), config.getMinSamples());
    }

    /**
     * The elapsed time after which a task is a straggler: {@code max(k x predicted, p90)}, the
     * p90 alone without a prediction, the prediction alone without enough history, NaN with
     * neither.
     */
    public static double threshold(double k, OptionalDouble predictedMs, OptionalDouble p90Ms) {
        if (predictedMs.isPresent() && p90Ms.isPresent()) {
            return Math.max(k * predictedMs.getAsDouble(), p90Ms.getAsDouble());
        }
        if (p90Ms.isPresent()) {
            return p90Ms.getAsDouble();
        }
        if (predictedMs.isPresent()) {
            return k * predictedMs.getAsDouble();
        }
        return Double.NaN;
    }

    /** One pass; returns how many copies it launched. */
    public int check() {
        LamportInterceptors.applyMdc();
        long now = Clocks.now();
        int launched = 0;
        for (RunningTasks.Running r : running.all()) {
            if (r.speculative() || r.settled().get() || r.timedOut().get()) {
                continue;
            }
            TaskRecord task = store.get(r.taskId());
            if (task == null) {
                continue;
            }
            double limit = threshold(config.getK(),
                    dispatcher.strategy().predictedExecMs(r.taskId()),
                    dispatcher.execTimes().p90(task.type().name(), config.getMinSamples()));
            if (!Double.isNaN(limit) && now - r.startedAtMs() > limit && dispatcher.speculate(r)) {
                launched++;
            }
        }
        return launched;
    }

    @Override
    public void close() {
        timer.shutdownNow();
    }
}
