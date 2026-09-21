package com.predisched.scheduler.queue;

import com.predisched.common.time.Clocks;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

/**
 * Priority queue with ageing (F2, FR23): high-priority work goes first, but a task that has been
 * waiting gains priority, so low-priority tasks cannot starve behind a steady stream of urgent
 * ones.
 *
 * <pre>
 * effective(task) = min(maxPriority, priority + ageingPerSecond * secondsWaiting)
 * </pre>
 *
 * <p>The effective priority is computed when a task is taken, not when it is added, so nothing has
 * to re-sort the queue on a timer. That makes {@code take()} a linear scan of the queued tasks;
 * with the hundreds of tasks this system queues that is far cheaper than maintaining a heap whose
 * keys change with time, and the scan happens once per dispatch.
 *
 * <p>Thread safety: one {@link ReentrantLock} guards the list, with a {@link Condition} for
 * waiting takers (rule 5).
 */
public class AgeingPriorityQueue implements TaskQueue {

    /** A queued task and when it joined the queue. */
    record Entry(String taskId, int priority, long queuedAtMs) {}

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final List<Entry> entries = new ArrayList<>();

    private final double ageingPerSecond;
    private final int maxPriority;
    private final LongSupplier clock;

    public AgeingPriorityQueue(double ageingPerSecond, int maxPriority) {
        this(ageingPerSecond, maxPriority, Clocks::now);
    }

    /** Test seam: an injected clock ages tasks without sleeping. */
    public AgeingPriorityQueue(double ageingPerSecond, int maxPriority, LongSupplier clock) {
        this.ageingPerSecond = ageingPerSecond;
        this.maxPriority = maxPriority;
        this.clock = clock;
    }

    @Override
    public void add(String taskId, int priority, long queuedAtMs) {
        lock.lock();
        try {
            entries.add(new Entry(taskId, priority, queuedAtMs));
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (entries.isEmpty()) {
                notEmpty.await();
            }
            long now = clock.getAsLong();
            int bestIndex = 0;
            double bestPriority = effectivePriority(entries.get(0), now);
            for (int i = 1; i < entries.size(); i++) {
                double candidate = effectivePriority(entries.get(i), now);
                // Ties go to whoever has waited longer, so equal priorities stay FIFO.
                if (candidate > bestPriority
                        || (candidate == bestPriority
                            && entries.get(i).queuedAtMs() < entries.get(bestIndex).queuedAtMs())) {
                    bestPriority = candidate;
                    bestIndex = i;
                }
            }
            return entries.remove(bestIndex).taskId();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(String taskId) {
        lock.lock();
        try {
            Iterator<Entry> it = entries.iterator();
            while (it.hasNext()) {
                if (it.next().taskId().equals(taskId)) {
                    it.remove();
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return entries.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<String> snapshot() {
        lock.lock();
        try {
            long now = clock.getAsLong();
            List<Entry> ordered = new ArrayList<>(entries);
            ordered.sort(Comparator
                    .comparingDouble((Entry e) -> effectivePriority(e, now)).reversed()
                    .thenComparingLong(Entry::queuedAtMs));
            return ordered.stream().map(Entry::taskId).toList();
        } finally {
            lock.unlock();
        }
    }

    /** The priority a task has right now, given how long it has waited. */
    double effectivePriority(Entry entry, long nowMs) {
        double waitedSeconds = Math.max(0, nowMs - entry.queuedAtMs()) / 1000.0;
        return Math.min(maxPriority, entry.priority() + ageingPerSecond * waitedSeconds);
    }
}
