package com.predisched.common;

import com.predisched.proto.TaskType;

/** Pluggable task-type execution contract (spec section 7.5). */
public interface TaskExecutor {

    TaskType type();

    ResourceProfile profile();

    ExecutionResult execute(String input) throws Exception;

    /**
     * Same input, same output, every time: the result may be served from the result cache (F6).
     * Tasks whose output depends on time, the network, the disk or a database say false.
     */
    default boolean deterministic() {
        return true;
    }
}
