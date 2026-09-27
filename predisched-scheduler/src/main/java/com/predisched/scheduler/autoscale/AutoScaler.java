package com.predisched.scheduler.autoscale;

import com.predisched.common.NodeConfig;
import com.predisched.common.obs.EventLog;
import com.predisched.common.obs.LamportInterceptors;
import com.predisched.scheduler.WorkerInfo;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auto-scaling on the primary (prompt 21, F12). Every {@code intervalMs}:
 *
 * <ul>
 *   <li><b>Up</b>: when the forecast demand exceeds capacity × {@code upRatio} on two checks in a
 *       row, and the cluster is under {@code maxWorkers} (workers still starting count), start a
 *       worker.
 *   <li><b>Down</b>: when utilisation (busy slots over capacity) stays under {@code downRatio}
 *       for {@code coolDownMs}, and the cluster is over {@code minWorkers}, drain the most idle
 *       worker this scaler started: no new dispatches, then stop it once its in-flight tasks end
 *       (or after {@code drainTimeoutMs}). Workers it did not start are never stopped.
 * </ul>
 *
 * Demand comes from a {@link Forecaster}: the M2 queue forecast (predictive) or the queues now
 * (reactive), so the two can be compared on the same traces. Every decision is logged and emitted
 * as a {@code SCALE_UP} or {@code SCALE_DOWN} event.
 */
public final class AutoScaler implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AutoScaler.class);
    private static final long START_GRACE_MS = 60_000;

    private final NodeConfig.AutoscaleConfig config;
    private final ClusterView view;
    private final Forecaster forecaster;
    private final WorkerLauncher launcher;
    private final LongSupplier clock;
    private final Map<String, Long> starting = new LinkedHashMap<>();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
            r -> {
                Thread t = new Thread(r, "autoscaler");
                t.setDaemon(true);
                return t;
            });
    private int highChecks;
    private long lowSinceMs = -1;
    private String draining;
    private long drainStartedMs;
    private int scaleUps;
    private int scaleDowns;

    public AutoScaler(NodeConfig.AutoscaleConfig config, ClusterView view, Forecaster forecaster,
            WorkerLauncher launcher, LongSupplier clock) {
        this.config = config;
        this.view = view;
        this.forecaster = forecaster;
        this.launcher = launcher;
        this.clock = clock;
    }

    public void start() {
        timer.scheduleWithFixedDelay(() -> {
            try {
                tick();
            } catch (Exception e) {
                log.warn("Auto-scaler check failed: {}", e.toString());
            }
        }, config.getIntervalMs(), config.getIntervalMs(), TimeUnit.MILLISECONDS);
        log.info("Auto-scaling on ({} forecast, {} launcher): up when demand > capacity x {} twice,"
                        + " down when utilisation < {} for {} ms; {}..{} workers",
                forecaster.name(), launcher.name(), config.getUpRatio(), config.getDownRatio(),
                config.getCoolDownMs(), config.getMinWorkers(), config.getMaxWorkers());
    }

    /** One check; package-visible for the tests. */
    synchronized void tick() throws Exception {
        if (!view.isLeader()) {
            return;
        }
        LamportInterceptors.applyMdc();
        long now = clock.getAsLong();
        ClusterView.State state = view.snapshot();
        List<WorkerInfo> workers = state.workers();
        workers.forEach(w -> starting.remove(w.id()));
        starting.values().removeIf(t -> now - t > START_GRACE_MS);
        int count = workers.size() + starting.size();
        int capacity = workers.stream().filter(w -> !w.id().equals(draining))
                .mapToInt(WorkerInfo::poolSize).sum();
        int busy = workers.stream()
                .mapToInt(w -> Math.min(state.inFlight().getOrDefault(w.id(), 0), w.poolSize()))
                .sum();
        double utilisation = capacity == 0 ? 1.0 : (double) busy / capacity;
        double demand = forecaster.demand(state);

        if (draining != null) {
            int left = state.inFlight().getOrDefault(draining, 0);
            if (left == 0 || now - drainStartedMs >= config.getDrainTimeoutMs()) {
                String stopped = draining;
                launcher.stop(stopped);
                view.forget(stopped);
                draining = null;
                scaleDowns++;
                lowSinceMs = now;
                // The draining worker is already out of the snapshot: count is what remains.
                log.warn("Scale down: stopped {} after draining ({} in flight left); {} workers,"
                        + " capacity {} slots", stopped, left, count, capacity);
                event("SCALE_DOWN", Map.of("worker", stopped, "workers", String.valueOf(count),
                        "left_in_flight", String.valueOf(left)));
            }
            return;
        }

        highChecks = demand > capacity * config.getUpRatio() ? highChecks + 1 : 0;
        if (highChecks >= 2 && count < config.getMaxWorkers()) {
            String id = launcher.start();
            starting.put(id, now);
            highChecks = 0;
            lowSinceMs = -1;
            scaleUps++;
            log.warn("Scale up: {} demand {} > capacity {} x {} on two checks; starting {}"
                            + " ({} of max {} workers)", forecaster.name(),
                    String.format(Locale.ROOT, "%.1f", demand), capacity, config.getUpRatio(), id,
                    count + 1, config.getMaxWorkers());
            event("SCALE_UP", Map.of("worker", id, "forecast", forecaster.name(),
                    "demand", String.format(Locale.ROOT, "%.1f", demand),
                    "capacity", String.valueOf(capacity), "workers", String.valueOf(count + 1)));
            return;
        }

        List<String> mine = new ArrayList<>(launcher.started());
        mine.retainAll(workers.stream().map(WorkerInfo::id).toList());
        if (utilisation < config.getDownRatio() && count > config.getMinWorkers()
                && !mine.isEmpty()) {
            if (lowSinceMs < 0) {
                lowSinceMs = now;
            } else if (now - lowSinceMs >= config.getCoolDownMs()) {
                String idle = mine.stream()
                        .min((a, b) -> Integer.compare(state.inFlight().getOrDefault(a, 0),
                                state.inFlight().getOrDefault(b, 0)))
                        .orElseThrow();
                draining = idle;
                drainStartedMs = now;
                view.drain(idle);
                log.warn("Scale down: utilisation {} < {} for {} ms; draining {} ({} in flight)",
                        String.format(Locale.ROOT, "%.2f", utilisation), config.getDownRatio(),
                        now - lowSinceMs, idle, state.inFlight().getOrDefault(idle, 0));
            }
        } else {
            lowSinceMs = -1;
        }
    }

    private static void event(String type, Map<String, String> details) {
        EventLog.get().event(type, "", details);
    }

    public int scaleUps() {
        return scaleUps;
    }

    public int scaleDowns() {
        return scaleDowns;
    }

    @Override
    public void close() {
        timer.shutdownNow();
        for (String id : new ArrayList<>(launcher.started())) {
            try {
                launcher.stop(id);
            } catch (Exception e) {
                log.warn("Could not stop {}: {}", id, e.toString());
            }
        }
    }
}
