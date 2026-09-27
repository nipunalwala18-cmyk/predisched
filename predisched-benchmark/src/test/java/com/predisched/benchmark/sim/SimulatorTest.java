package com.predisched.benchmark.sim;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.predisched.benchmark.suite.RunMetrics;
import com.predisched.client.workload.TraceEntry;
import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.WorkerInfo;
import com.predisched.scheduler.strategy.LeastLoadedStrategy;
import com.predisched.scheduler.strategy.RandomStrategy;
import com.predisched.scheduler.strategy.SchedulingStrategy;
import com.predisched.scheduler.strategy.StrategyDecision;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** The what-if simulator is deterministic for a seed and drives the real strategy classes. */
class SimulatorTest {

    private static ExecSamples samples() {
        ExecSamples s = new ExecSamples(5);
        Random r = new Random(1);
        for (int i = 0; i < 200; i++) {
            long size = 1_000 + r.nextInt(1_000_000);
            s.add("CPU_TASK", size, size / 20_000.0 + r.nextDouble() * 5);
            s.add("SLEEP_TASK", size % 400, size % 400);
        }
        return s;
    }

    private static List<TraceEntry> trace(int n) {
        List<TraceEntry> t = new ArrayList<>();
        Random r = new Random(3);
        long offset = 0;
        for (int i = 0; i < n; i++) {
            offset += r.nextInt(40);
            boolean cpu = i % 2 == 0;
            t.add(new TraceEntry(offset, "t" + i, cpu ? TaskType.CPU_TASK : TaskType.SLEEP_TASK,
                    cpu ? "n=" + (10_000 + r.nextInt(500_000)) : "ms=" + (50 + r.nextInt(300)),
                    1 + r.nextInt(10), 0));
        }
        return t;
    }

    private static final List<Simulator.WorkerModel> WORKERS = List.of(
            new Simulator.WorkerModel("worker-1", 2, 2.0),
            new Simulator.WorkerModel("worker-2", 4, 1.0),
            new Simulator.WorkerModel("worker-3", 8, 1.0));

    @Test
    void aFixedSeedGivesTheSameRunEveryTime() {
        List<TraceEntry> trace = trace(300);
        Simulator.Result first = new Simulator(new RandomStrategy(7), WORKERS, samples(), 2.0,
                30, 42).run(trace, 1.0);
        Simulator.Result second = new Simulator(new RandomStrategy(7), WORKERS, samples(), 2.0,
                30, 42).run(trace, 1.0);
        assertEquals(first.rows(), second.rows());
        Simulator.Result otherSeed = new Simulator(new RandomStrategy(7), WORKERS, samples(),
                2.0, 30, 43).run(trace, 1.0);
        assertTrue(!first.rows().equals(otherSeed.rows()), "the seed drives the sampling");
        assertEquals(2 * 300, first.events(), "one arrival and one completion per task");
    }

    @Test
    void everyDecisionGoesThroughTheRealStrategyAndRespectsCapacity() {
        AtomicInteger decisions = new AtomicInteger();
        List<Integer> maxLoadSeen = new ArrayList<>(List.of(Integer.MIN_VALUE));
        SchedulingStrategy spy = new LeastLoadedStrategy() {
            @Override
            public StrategyDecision decide(TaskRecord task, List<WorkerInfo> candidates) {
                decisions.incrementAndGet();
                candidates.forEach(w -> maxLoadSeen.set(0, Math.max(maxLoadSeen.get(0),
                        w.load() - 2 * w.poolSize())));
                return super.decide(task, candidates);
            }
        };
        List<TraceEntry> trace = trace(400);
        Simulator.Result result = new Simulator(spy, WORKERS, samples(), 2.0, 0, 1)
                .run(trace, 4.0);
        assertEquals(400, decisions.get(), "one real decision per task");
        assertTrue(maxLoadSeen.get(0) < 0, "only workers under 2 x pool are candidates");
        for (RunMetrics.TaskRow r : result.rows()) {
            assertTrue(r.startMs() >= r.submitMs() && r.endMs() >= r.startMs(), r.toString());
            assertEquals("COMPLETED", r.status());
        }
    }

    @Test
    void samplesFollowSizeAndSlowdownStretchesThem() {
        ExecSamples s = new ExecSamples(3);
        for (int i = 1; i <= 100; i++) {
            s.add("CPU_TASK", i * 1_000L, i);
        }
        Random r = new Random(5);
        double small = s.draw("CPU_TASK", 2_000, r);
        double large = s.draw("CPU_TASK", 99_000, r);
        assertTrue(small <= 4 && large >= 97, small + " " + large);
        assertEquals(50.0, s.draw("NEW_TASK", 10, r), "an unseen type gets a default");
        List<TraceEntry> one = List.of(new TraceEntry(0, "x", TaskType.CPU_TASK, "n=50000", 5, 0));
        double fast = new Simulator(new LeastLoadedStrategy(),
                List.of(new Simulator.WorkerModel("w", 1, 1.0)), s, 2.0, 0, 9)
                .run(one, 1.0).rows().get(0).execMs();
        double slow = new Simulator(new LeastLoadedStrategy(),
                List.of(new Simulator.WorkerModel("w", 1, 3.0)), s, 2.0, 0, 9)
                .run(one, 1.0).rows().get(0).execMs();
        assertEquals(3 * fast, slow, 1.0);
    }
}
