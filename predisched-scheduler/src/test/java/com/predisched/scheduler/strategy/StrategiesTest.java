package com.predisched.scheduler.strategy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.predisched.common.TaskRecord;
import com.predisched.proto.TaskType;
import com.predisched.scheduler.WorkerInfo;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Each strategy's rule on hand-built workers, including ties and a single candidate. */
class StrategiesTest {

    private static final TaskRecord TASK =
            TaskRecord.createQueued("t", TaskType.SLEEP_TASK, "ms=5", 5);

    static WorkerInfo worker(String id, int pool, double cpu, double mem, int active, int queued) {
        return new WorkerInfo(id, "localhost", 0, 4, 1024, pool, cpu, mem, active, queued,
                0, 0, 0, 0);
    }

    @Test
    void roundRobinCyclesInIdOrderWhateverTheListOrder() {
        RoundRobinStrategy strategy = new RoundRobinStrategy();
        List<WorkerInfo> workers = List.of(worker("w3", 4, 0, 0, 0, 0),
                worker("w1", 4, 0, 0, 0, 0), worker("w2", 4, 0, 0, 0, 0));
        List<String> picks = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            picks.add(strategy.select(TASK, workers).id());
        }
        assertEquals(List.of("w1", "w2", "w3", "w1", "w2", "w3"), picks);
    }

    @Test
    void randomIsReproducibleForASeedAndCoversEveryCandidate() {
        List<WorkerInfo> workers = List.of(worker("a", 4, 0, 0, 0, 0),
                worker("b", 4, 0, 0, 0, 0), worker("c", 4, 0, 0, 0, 0));
        List<String> first = new ArrayList<>();
        List<String> second = new ArrayList<>();
        RandomStrategy one = new RandomStrategy(7);
        RandomStrategy two = new RandomStrategy(7);
        for (int i = 0; i < 60; i++) {
            first.add(one.select(TASK, workers).id());
            second.add(two.select(TASK, workers.reversed()).id());
        }
        assertEquals(first, second, "same seed must give the same picks, in any list order");
        assertEquals(3, first.stream().distinct().count());
    }

    @Test
    void leastLoadedPicksTheLowestQueuePlusActiveAndBreaksTiesById() {
        LeastLoadedStrategy strategy = new LeastLoadedStrategy();
        assertEquals("w2", strategy.select(TASK, List.of(
                worker("w1", 8, 0, 0, 3, 2), worker("w2", 2, 0, 0, 1, 1),
                worker("w3", 4, 0, 0, 2, 3))).id());
        assertEquals("w1", strategy.select(TASK, List.of(
                worker("w2", 4, 0, 0, 1, 0), worker("w1", 4, 0, 0, 0, 1))).id(), "tie");
        assertEquals(2.0, strategy.scores(TASK, List.of(worker("w2", 2, 0, 0, 1, 1)))
                .get("w2"));
    }

    @Test
    void resourceAwareWeighsCpuMemoryAndLoadAgainstPoolSize() {
        ResourceAwareStrategy strategy = new ResourceAwareStrategy(0.4, 0.2, 0.4);
        // Same load of 4: relative to its pool of 8 the big worker is half full, the small one 2x.
        WorkerInfo big = worker("big", 8, 50, 50, 4, 0);
        WorkerInfo small = worker("small", 2, 10, 10, 2, 2);
        assertEquals("big", strategy.select(TASK, List.of(small, big)).id());
        // big: 0.4*0.5 + 0.2*0.5 + 0.4*0.5 = 0.5; small: 0.04 + 0.02 + 0.8 = 0.86
        assertEquals(0.5, strategy.scores(TASK, List.of(big)).get("big"), 1e-9);
        assertEquals(0.86, strategy.scores(TASK, List.of(small)).get("small"), 1e-9);
        // Only CPU matters when it alone is weighted.
        ResourceAwareStrategy cpuOnly = new ResourceAwareStrategy(1, 0, 0);
        assertEquals("small", cpuOnly.select(TASK, List.of(big, small)).id());
        assertEquals("a", cpuOnly.select(TASK, List.of(
                worker("b", 4, 30, 0, 0, 0), worker("a", 4, 30, 0, 0, 0))).id(), "tie");
    }

    @Test
    void everyStrategyReturnsTheOnlyCandidate() {
        WorkerInfo only = worker("solo", 4, 99, 99, 9, 9);
        for (String name : StrategyRegistry.standard().names()) {
            SchedulingStrategy strategy =
                    StrategyRegistry.standard().create(name, StrategyRegistry.Settings.defaults());
            assertEquals("solo", strategy.select(TASK, List.of(only)).id(), name);
        }
    }
}
