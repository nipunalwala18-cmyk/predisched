package com.predisched.scheduler.autoscale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.common.NodeConfig;
import com.predisched.scheduler.WorkerInfo;
import com.predisched.scheduler.strategy.DriftDetector;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** Scaling up after two high checks, down after the cool-down, never outside min/max; drift. */
class AutoScalerTest {

    /** Registers what it starts at once; remembers what it stopped. */
    static final class FakeLauncher implements WorkerLauncher {
        final List<String> running = new ArrayList<>();
        final List<String> stopped = new ArrayList<>();
        int next = 1;

        @Override
        public String name() {
            return "fake";
        }

        @Override
        public String start() {
            String id = "worker-a" + next++;
            running.add(id);
            return id;
        }

        @Override
        public void stop(String id) {
            running.remove(id);
            stopped.add(id);
        }

        @Override
        public List<String> started() {
            return new ArrayList<>(running);
        }
    }

    /** One fixed worker plus whatever the launcher runs; in-flight counts set per test. */
    static final class FakeView implements ClusterView {
        final FakeLauncher launcher;
        final Map<String, Integer> inFlight = new HashMap<>();
        final List<String> drained = new ArrayList<>();
        final List<String> forgotten = new ArrayList<>();

        FakeView(FakeLauncher launcher) {
            this.launcher = launcher;
        }

        static WorkerInfo worker(String id) {
            return new WorkerInfo(id, "h", 0, 4, 1024, 4, 1.0, 0, 0, 0, 0, 0, 0, 0, 0);
        }

        @Override
        public State snapshot() {
            List<WorkerInfo> ws = new ArrayList<>(List.of(worker("worker-1")));
            launcher.running.stream().filter(id -> !drained.contains(id))
                    .forEach(id -> ws.add(worker(id)));
            Map<String, Integer> flights = new HashMap<>();
            ws.forEach(w -> flights.put(w.id(), inFlight.getOrDefault(w.id(), 0)));
            drained.stream().filter(launcher.running::contains)
                    .forEach(id -> flights.put(id, inFlight.getOrDefault(id, 0)));
            return new State(ws, 0, flights);
        }

        @Override
        public boolean isLeader() {
            return true;
        }

        @Override
        public void drain(String id) {
            drained.add(id);
        }

        @Override
        public void forget(String id) {
            forgotten.add(id);
        }
    }

    /** Hands out scripted demands, then repeats the last. */
    static Forecaster scripted(Deque<Double> demands) {
        return new Forecaster() {
            double last;

            @Override
            public String name() {
                return "scripted";
            }

            @Override
            public double demand(ClusterView.State state) {
                if (!demands.isEmpty()) {
                    last = demands.poll();
                }
                return last;
            }
        };
    }

    private static NodeConfig.AutoscaleConfig config(int min, int max) {
        NodeConfig.AutoscaleConfig c = new NodeConfig.AutoscaleConfig();
        c.setMinWorkers(min);
        c.setMaxWorkers(max);
        c.setUpRatio(1.0);
        c.setDownRatio(0.3);
        c.setCoolDownMs(10_000);
        c.setDrainTimeoutMs(5_000);
        return c;
    }

    @Test
    void scalesUpOnlyAfterTwoHighChecksInARowAndNeverPastMax() throws Exception {
        FakeLauncher launcher = new FakeLauncher();
        FakeView view = new FakeView(launcher);
        AtomicLong now = new AtomicLong();
        // Capacity is 4 slots per worker. High, low, high: no scale-up (not two in a row).
        Deque<Double> demands = new ArrayDeque<>(List.of(9.0, 1.0, 9.0, 1.0));
        AutoScaler scaler = new AutoScaler(config(1, 3), view, scripted(demands), launcher,
                now::get);
        for (int i = 0; i < 4; i++) {
            scaler.tick();
        }
        assertEquals(0, scaler.scaleUps());
        // Two high checks in a row: one worker.
        demands.addAll(List.of(9.0, 9.0));
        scaler.tick();
        scaler.tick();
        assertEquals(List.of("worker-a1"), launcher.running);
        // Keep it high: two more checks per scale-up, up to max 3 workers, then no more.
        demands.add(100.0);
        for (int i = 0; i < 10; i++) {
            scaler.tick();
        }
        assertEquals(2, launcher.running.size(), "1 fixed + 2 started = max 3");
        assertEquals(2, scaler.scaleUps());
    }

    @Test
    void scalesDownAfterTheCoolDownByDrainingThenStoppingItsOwnWorker() throws Exception {
        FakeLauncher launcher = new FakeLauncher();
        FakeView view = new FakeView(launcher);
        AtomicLong now = new AtomicLong();
        Deque<Double> demands = new ArrayDeque<>(List.of(20.0, 20.0, 20.0, 20.0));
        AutoScaler scaler = new AutoScaler(config(1, 3), view, scripted(demands), launcher,
                now::get);
        for (int i = 0; i < 4; i++) {
            scaler.tick();
        }
        assertEquals(2, launcher.running.size());
        // Idle: utilisation 0 < 0.3. Nothing happens before the cool-down.
        demands.add(0.0);
        view.inFlight.put("worker-a2", 1);
        scaler.tick();                      // low since now
        now.addAndGet(9_000);
        scaler.tick();
        assertTrue(view.drained.isEmpty(), "cool-down not over");
        now.addAndGet(1_000);
        scaler.tick();
        assertEquals(List.of("worker-a1"), view.drained, "the most idle started worker");
        // Drained but empty: stopped on the next check, and forgotten by the registry.
        scaler.tick();
        assertEquals(List.of("worker-a1"), launcher.stopped);
        assertEquals(List.of("worker-a1"), view.forgotten);
        // The next one waits a full cool-down again, and drains even with a task left once
        // the drain timeout passes.
        now.addAndGet(10_000);
        scaler.tick();
        assertEquals(List.of("worker-a1", "worker-a2"), view.drained);
        scaler.tick();
        assertFalse(launcher.stopped.contains("worker-a2"), "still has a task");
        now.addAndGet(5_000);
        scaler.tick();
        assertTrue(launcher.stopped.contains("worker-a2"), "stopped at the drain timeout");
        // Down to min 1 (the fixed worker, never stopped): nothing more to remove.
        now.addAndGet(60_000);
        scaler.tick();
        scaler.tick();
        assertEquals(2, scaler.scaleDowns());
        assertEquals(List.of(), launcher.running);
    }

    @Test
    void neverGoesBelowMin() throws Exception {
        FakeLauncher launcher = new FakeLauncher();
        FakeView view = new FakeView(launcher);
        AtomicLong now = new AtomicLong();
        Deque<Double> demands = new ArrayDeque<>(List.of(20.0, 20.0));
        AutoScaler scaler = new AutoScaler(config(2, 4), view, scripted(demands), launcher,
                now::get);
        scaler.tick();
        scaler.tick();
        assertEquals(1, launcher.running.size());   // 2 workers = min
        demands.add(0.0);
        for (int i = 0; i < 10; i++) {
            now.addAndGet(20_000);
            scaler.tick();
        }
        assertTrue(view.drained.isEmpty(), "2 workers is the minimum");
    }

    @Test
    void reactiveDemandIsTheQueueNow() {
        ClusterView.State state = new ClusterView.State(
                List.of(FakeView.worker("w1"), FakeView.worker("w2")), 3,
                Map.of("w1", 5, "w2", 2));
        assertEquals(10.0, Forecaster.reactive().demand(state));
    }

    @Test
    void driftIsOneEventPerEpisode() {
        DriftDetector drift = new DriftDetector(1.5, 3);
        int events = 0;
        // Baseline 10: ratio 2 for five tasks -> one episode (at the third).
        double[] series = {20, 20, 20, 20, 20, 12, 12, 30, 30, 30, 30, 5, 30, 30};
        List<Integer> at = new ArrayList<>();
        for (int i = 0; i < series.length; i++) {
            if (drift.observe(series[i], 10).newEpisode()) {
                events++;
                at.add(i);
            }
        }
        // Episode 1 at index 2; the ratio drops at 5 (1.2), episode 2 at index 9; the drop at 11
        // ends it, and two high values after it are not enough for a third.
        assertEquals(List.of(2, 9), at);
        assertEquals(2, events);
        assertEquals(2, drift.episodes());
        assertFalse(new DriftDetector(1.5, 1).observe(100, 0).newEpisode(),
                "no baseline, no drift");
    }
}
