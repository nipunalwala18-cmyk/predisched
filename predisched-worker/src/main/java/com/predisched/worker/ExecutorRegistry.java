package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.TaskExecutor;
import com.predisched.proto.TaskType;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maps {@link TaskType} to its executor. Unknown types and executor exceptions
 * become failed results, never an exception across gRPC.
 */
public class ExecutorRegistry {

    private final Map<TaskType, TaskExecutor> executors = new ConcurrentHashMap<>();

    public ExecutorRegistry() {
        register(new CpuTaskExecutor());
        register(new SleepTaskExecutor());
        register(new MatrixTaskExecutor());
    }

    public void register(TaskExecutor executor) {
        executors.put(executor.type(), executor);
    }

    public ExecutionResult execute(TaskType type, String input) {
        TaskExecutor executor = executors.get(type);
        if (executor == null) {
            return ExecutionResult.failure("unsupported task type: " + type);
        }
        try {
            ExecutionResult result = executor.execute(input);
            return result == null ? ExecutionResult.failure("executor returned null") : result;
        } catch (Exception e) {
            return ExecutionResult.failure("executor failed: " + e.getMessage());
        }
    }
}
