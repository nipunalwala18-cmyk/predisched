package com.predisched.scheduler.queue;

import com.predisched.common.TaskRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Where tasks go when their retries run out (F4, FR25). Parking them instead of forgetting them is
 * the point: an operator can look at what keeps failing and put it back with {@code dlq retry}.
 */
public class DeadLetterQueue {

    /** A parked task and why it stopped. */
    public record Entry(TaskRecord record, String lastError, long failedAtMs) {}

    private final Map<String, Entry> entries = new ConcurrentHashMap<>();

    public void add(TaskRecord record, String lastError, long failedAtMs) {
        entries.put(record.id(), new Entry(record, lastError, failedAtMs));
    }

    public Optional<Entry> get(String taskId) {
        return Optional.ofNullable(entries.get(taskId));
    }

    public Optional<Entry> remove(String taskId) {
        return Optional.ofNullable(entries.remove(taskId));
    }

    /** Newest failures first, which is what an operator wants to see. */
    public List<Entry> list(int limit) {
        List<Entry> all = new ArrayList<>(entries.values());
        all.sort(Comparator.comparingLong(Entry::failedAtMs).reversed());
        return limit <= 0 || limit >= all.size() ? all : all.subList(0, limit);
    }

    public int size() {
        return entries.size();
    }
}
