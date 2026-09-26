package com.predisched.scheduler.strategy;

import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

/**
 * A uniformly random candidate from a seeded {@link Random} (rule 8), so a replay with the same
 * seed and the same candidate lists makes the same choices. Candidates are sorted by id first so
 * the draw does not depend on registry iteration order. {@code Random} is thread-safe.
 */
public class RandomStrategy implements SchedulingStrategy {

    private final Random random;

    public RandomStrategy(long seed) {
        this.random = new Random(seed);
    }

    @Override
    public String name() {
        return "random";
    }

    @Override
    public WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates) {
        List<WorkerInfo> ordered = candidates.stream()
                .sorted(Comparator.comparing(WorkerInfo::id))
                .toList();
        return ordered.get(random.nextInt(ordered.size()));
    }
}
