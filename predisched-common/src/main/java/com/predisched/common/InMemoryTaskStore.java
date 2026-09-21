package com.predisched.common;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/** Thread-safe in-memory {@link TaskStore} backed by a {@link ConcurrentHashMap}. */
public class InMemoryTaskStore implements TaskStore {

    private final ConcurrentHashMap<String, TaskRecord> map = new ConcurrentHashMap<>();

    @Override
    public void put(TaskRecord record) {
        TaskRecord existing = map.putIfAbsent(record.id(), record);
        if (existing != null) {
            throw new IllegalStateException("Task already exists: " + record.id());
        }
    }

    @Override
    public void replace(TaskRecord record) {
        map.put(record.id(), record);
    }

    @Override
    public TaskRecord get(String taskId) {
        return map.get(taskId);
    }

    @Override
    public TaskRecord update(String taskId, UnaryOperator<TaskRecord> fn) {
        return map.compute(taskId, (id, current) -> {
            if (current == null) {
                throw new NoSuchElementException("Unknown task: " + taskId);
            }
            TaskRecord next = fn.apply(current);
            if (next == null) {
                throw new IllegalStateException("Update function returned null for task: " + taskId);
            }
            return next;
        });
    }

    @Override
    public List<TaskRecord> list() {
        return new ArrayList<>(map.values());
    }

    @Override
    public boolean contains(String taskId) {
        return map.containsKey(taskId);
    }
}
