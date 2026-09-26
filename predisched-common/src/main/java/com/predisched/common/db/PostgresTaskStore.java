package com.predisched.common.db;

import com.predisched.common.TaskRecord;
import com.predisched.common.TaskStore;
import java.util.List;
import java.util.function.UnaryOperator;

/**
 * The primary's durable copy of its tasks (prompt 11): every write goes to the store it wraps
 * (in memory, or replicated between schedulers, which stays the source of truth) and then, as an
 * upsert, to the {@code tasks} table through the {@link HistorySink}. Reads never touch the
 * database, and a write never waits for it.
 *
 * <p>Only writes made through this store are copied, which on a primary-backup cluster means the
 * primary's: a backup applies replicated changes to its replica directly.
 */
public class PostgresTaskStore implements TaskStore {

    private final TaskStore delegate;
    private final HistorySink history;

    public PostgresTaskStore(TaskStore delegate, HistorySink history) {
        this.delegate = delegate;
        this.history = history;
    }

    @Override
    public void put(TaskRecord record) {
        delegate.put(record);
        history.task(record);
    }

    @Override
    public TaskRecord get(String taskId) {
        return delegate.get(taskId);
    }

    @Override
    public TaskRecord update(String taskId, UnaryOperator<TaskRecord> fn) {
        TaskRecord next = delegate.update(taskId, fn);
        history.task(next);
        return next;
    }

    @Override
    public void replace(TaskRecord record) {
        delegate.replace(record);
        history.task(record);
    }

    @Override
    public List<TaskRecord> list() {
        return delegate.list();
    }

    @Override
    public boolean contains(String taskId) {
        return delegate.contains(taskId);
    }

    /** The store this one copies to the database. */
    public TaskStore delegate() {
        return delegate;
    }
}
