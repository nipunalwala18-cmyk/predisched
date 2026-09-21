package com.predisched.worker;

import com.predisched.common.ExecutionResult;
import com.predisched.common.InputParser;
import com.predisched.common.ResourceProfile;
import com.predisched.common.TaskInputSpec;
import com.predisched.common.TaskExecutor;
import com.predisched.proto.TaskType;
import java.util.Map;

/** Sleeps for a fixed time. Control case with known duration and near-zero resource use. */
public class SleepTaskExecutor implements TaskExecutor {

    @Override
    public TaskType type() {
        return TaskType.SLEEP_TASK;
    }

    @Override
    public ResourceProfile profile() {
        return ResourceProfile.IO_BOUND;
    }

    @Override
    public ExecutionResult execute(String input) {
        String error = TaskInputSpec.firstError(type(), input);
        if (error != null) {
            return ExecutionResult.failure("bad input '" + input + "': " + error);
        }
        long ms = Long.parseLong(InputParser.parse(input).get("ms"));
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ExecutionResult.failure("interrupted after sleep request ms=" + ms);
        }
        return ExecutionResult.success("slept_ms=" + ms);
    }
}
