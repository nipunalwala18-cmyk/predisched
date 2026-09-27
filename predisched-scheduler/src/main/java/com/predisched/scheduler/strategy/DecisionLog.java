package com.predisched.scheduler.strategy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * The most recent {@link SchedulingDecision}s, oldest dropped first, for the benchmark and the
 * dashboard (prompt 22). Every decision also goes to the node's event log. Guarded by this.
 */
public class DecisionLog {

    private final int capacity;
    private final Deque<SchedulingDecision> recent;

    public DecisionLog(int capacity) {
        this.capacity = capacity;
        this.recent = new ArrayDeque<>(capacity);
    }

    public synchronized void add(SchedulingDecision decision) {
        if (recent.size() == capacity) {
            recent.removeFirst();
        }
        recent.addLast(decision);
    }

    /** The newest decision for this task (a retried task has several), or null. */
    public synchronized SchedulingDecision find(String taskId) {
        var it = recent.descendingIterator();
        while (it.hasNext()) {
            SchedulingDecision decision = it.next();
            if (decision.taskId().equals(taskId)) {
                return decision;
            }
        }
        return null;
    }

    /** Oldest first. */
    public synchronized List<SchedulingDecision> recent() {
        return new ArrayList<>(recent);
    }

    public synchronized int size() {
        return recent.size();
    }
}
