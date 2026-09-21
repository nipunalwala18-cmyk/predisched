package com.predisched.common;

import com.predisched.proto.TaskType;

/** Pluggable task-type execution contract (spec section 7.5). */
public interface TaskExecutor {

    TaskType type();

    ResourceProfile profile();

    ExecutionResult execute(String input) throws Exception;
}
