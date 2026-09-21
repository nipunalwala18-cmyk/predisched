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
}
