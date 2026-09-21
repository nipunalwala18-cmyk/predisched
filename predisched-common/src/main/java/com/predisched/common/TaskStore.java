package com.predisched.common;

import java.util.List;
import java.util.function.UnaryOperator;

/** Task storage contract. Prompts 07 and 11 add other implementations. */
public interface TaskStore {

    void put(TaskRecord record);

    TaskRecord get(String taskId);

    TaskRecord update(String taskId, UnaryOperator<TaskRecord> fn);

    List<TaskRecord> list();

    boolean contains(String taskId);

    /**
     * Replaces an existing record wholesale, ignoring the state machine. Only for restarting a
     * task that ended in the dead-letter queue (F4), where a terminal record is deliberately
     * replaced by a fresh QUEUED one.
     */
    void replace(TaskRecord record);
}
