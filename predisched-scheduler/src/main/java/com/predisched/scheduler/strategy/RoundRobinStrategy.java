package com.predisched.scheduler.strategy;

import com.predisched.common.TaskRecord;
import com.predisched.scheduler.WorkerInfo;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Each dispatch goes to the next candidate in id order. Ignores load entirely, which is the point
 * of it as a baseline. The cursor is an {@link AtomicInteger}; with a changing candidate list it
 * cycles over whoever is currently eligible.
 */
public class RoundRobinStrategy implements SchedulingStrategy {

    private final AtomicInteger cursor = new AtomicInteger();

    @Override
    public String name() {
        return "round_robin";
    }

    @Override
    public WorkerInfo select(TaskRecord task, List<WorkerInfo> candidates) {
        List<WorkerInfo> ordered = candidates.stream()
                .sorted(Comparator.comparing(WorkerInfo::id))
                .toList();
        int next = Math.floorMod(cursor.getAndIncrement(), ordered.size());
        return ordered.get(next);
    }
}
